use std::{
    collections::{BTreeMap, HashMap},
    error::Error,
    io,
    net::UdpSocket,
    sync::atomic::Ordering,
    time::{Duration, Instant},
};

#[cfg(target_os = "linux")]
use std::{mem::size_of_val, os::fd::AsRawFd};

use anyhow::{Result, anyhow};
use solana_entry_v3::entry::Entry;
use solana_ledger_lite::shred::{self, DATA_SHREDS_PER_FEC_BLOCK, ReedSolomonCache, Shred, Shredder};
use solana_transaction_v3::versioned::VersionedTransaction;
use tokio::{sync::broadcast::error::TryRecvError, task};
use tracing::{Level, error, info, warn};

use crate::{
    config::{Config, Endpoint},
    utils::{TransactionData, get_current_timestamp, open_log_file, write_log_entry},
};

use super::{
    GeyserProvider, ProviderContext,
    common::{
        TransactionAccumulator, build_signature_envelope, enqueue_signature, fatal_connection_error,
    },
    xw_tx::{parse_account_filter, parse_udp_bind_addr},
};

const RAW_SHRED_RECVBUF_BYTES: usize = 64 * 1024 * 1024;
const RAW_SHRED_PACKET_CAP: usize = 2_048;
const RAW_SHRED_POLL_INTERVAL: Duration = Duration::from_millis(200);
const FEC_STATE_TTL: Duration = Duration::from_secs(30);
const SLOT_STATE_TTL: Duration = Duration::from_secs(60);
const CLEANUP_INTERVAL: Duration = Duration::from_secs(5);
const CODING_HEADER_OFFSET: usize = 83;

pub struct RawShredProvider;

impl GeyserProvider for RawShredProvider {
    fn process(
        &self,
        endpoint: Endpoint,
        config: Config,
        context: ProviderContext,
    ) -> task::JoinHandle<Result<(), Box<dyn Error + Send + Sync>>> {
        task::spawn(async move { process_raw_shred_endpoint(endpoint, config, context).await })
    }
}

async fn process_raw_shred_endpoint(
    endpoint: Endpoint,
    config: Config,
    context: ProviderContext,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    task::spawn_blocking(move || run_raw_shred_loop(endpoint, config, context))
        .await
        .map_err(|err| io::Error::other(format!("raw_shred worker join failed: {err}")))?
}

fn run_raw_shred_loop(
    endpoint: Endpoint,
    config: Config,
    context: ProviderContext,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let ProviderContext {
        shutdown_tx,
        mut shutdown_rx,
        start_wallclock_secs,
        start_instant,
        comparator,
        signature_tx,
        shared_counter,
        shared_shutdown,
        target_transactions,
        total_producers,
        progress,
    } = context;

    let signature_sender = signature_tx;
    let account_filter = parse_account_filter(&config.account)?;
    let endpoint_name = endpoint.name.clone();
    let bind_addr = parse_udp_bind_addr(&endpoint.url)
        .unwrap_or_else(|err| fatal_connection_error(&endpoint_name, err));

    let mut log_file = if tracing::enabled!(Level::TRACE) {
        Some(open_log_file(&endpoint_name)?)
    } else {
        None
    };

    info!(endpoint = %endpoint_name, bind = %bind_addr, "Binding raw shred UDP listener");
    let socket = UdpSocket::bind(bind_addr).unwrap_or_else(|err| fatal_connection_error(&endpoint_name, err));
    socket
        .set_read_timeout(Some(RAW_SHRED_POLL_INTERVAL))
        .unwrap_or_else(|err| fatal_connection_error(&endpoint_name, err));
    set_socket_recvbuf(&socket, RAW_SHRED_RECVBUF_BYTES)
        .unwrap_or_else(|err| fatal_connection_error(&endpoint_name, err));
    info!(endpoint = %endpoint_name, bind = %bind_addr, "raw shred UDP listener ready");

    let mut processor = ShredProcessor::new();
    let mut accumulator = TransactionAccumulator::new();
    let mut transaction_count = 0usize;
    let mut packet = [0u8; RAW_SHRED_PACKET_CAP];

    loop {
        match shutdown_rx.try_recv() {
            Ok(()) | Err(TryRecvError::Closed) => {
                info!(endpoint = %endpoint_name, "Received stop signal");
                break;
            }
            Err(TryRecvError::Lagged(skipped)) => {
                info!(endpoint = %endpoint_name, skipped, "Shutdown signal lagged; stopping raw shred listener");
                break;
            }
            Err(TryRecvError::Empty) => {}
        }

        let len = match recv_packet(&socket, &mut packet) {
            Ok(0) => continue,
            Ok(len) => len,
            Err(err)
                if matches!(
                    err.kind(),
                    io::ErrorKind::Interrupted
                        | io::ErrorKind::WouldBlock
                        | io::ErrorKind::TimedOut
                ) =>
            {
                continue;
            }
            Err(err) => {
                error!(endpoint = %endpoint_name, error = %err, "raw shred UDP receive failed");
                continue;
            }
        };

        let wallclock = get_current_timestamp();
        let elapsed = start_instant.elapsed();
        let decoded_transactions = match processor.process_packet(&packet[..len]) {
            Ok(decoded) => decoded,
            Err(err) => {
                warn!(endpoint = %endpoint_name, error = %err, packet_len = len, "Failed to process raw shred packet");
                continue;
            }
        };

        for decoded in decoded_transactions {
            if !matches_v3_account_filter(
                account_filter.as_deref(),
                decoded.transaction.message.static_account_keys(),
            ) {
                continue;
            }

            let Some(signature) = decoded
                .transaction
                .signatures
                .first()
                .map(|sig| bs58::encode(sig.as_ref()).into_string())
            else {
                continue;
            };

            if let Some(file) = log_file.as_mut() {
                write_log_entry(file, wallclock, &endpoint_name, &signature)?;
            }

            let tx_data = TransactionData {
                slot: Some(decoded.slot),
                wallclock_secs: wallclock,
                elapsed_since_start: elapsed,
                start_wallclock_secs,
            };

            let updated = accumulator.record(signature.clone(), tx_data.clone());
            if updated
                && let Some(envelope) = build_signature_envelope(
                    &comparator,
                    &endpoint_name,
                    &signature,
                    tx_data,
                    total_producers,
                )
            {
                if let Some(target) = target_transactions {
                    let shared = shared_counter.fetch_add(1, Ordering::AcqRel) + 1;
                    if let Some(tracker) = progress.as_ref() {
                        tracker.record(shared);
                    }
                    if shared >= target && !shared_shutdown.swap(true, Ordering::AcqRel) {
                        info!(endpoint = %endpoint_name, target, "Reached shared signature target; broadcasting shutdown");
                        let _ = shutdown_tx.send(());
                    }
                }

                if let Some(sender) = signature_sender.as_ref() {
                    enqueue_signature(sender, &endpoint_name, &signature, envelope);
                }
            }

            transaction_count += 1;
        }
    }

    let unique_signatures = accumulator.len();
    let collected = accumulator.into_inner();
    comparator.add_batch(&endpoint_name, collected);
    info!(
        endpoint = %endpoint_name,
        total_transactions = transaction_count,
        unique_signatures,
        "raw shred UDP listener closed"
    );
    Ok(())
}

fn matches_v3_account_filter<K>(filter: Option<&[solana_pubkey::Pubkey]>, keys: &[K]) -> bool
where
    K: AsRef<[u8]>,
{
    match filter {
        Some(wanted) => keys
            .iter()
            .any(|key| wanted.iter().any(|candidate| candidate.as_ref() == key.as_ref())),
        None => true,
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct FecKey {
    slot: u64,
    fec_set_index: u32,
}

#[derive(Clone)]
struct DecodedTransaction {
    slot: u64,
    transaction: VersionedTransaction,
}

struct FecSetState {
    fec_set_index: u32,
    data_shreds: BTreeMap<u32, Shred>,
    code_shreds: BTreeMap<u32, Shred>,
    expected_data_shreds: Option<usize>,
    last_update: Instant,
}

struct SlotState {
    data_shreds: BTreeMap<u32, Shred>,
    next_decode_index: Option<u32>,
    last_update: Instant,
}

struct ShredProcessor {
    reed_solomon_cache: ReedSolomonCache,
    fec_sets: HashMap<FecKey, FecSetState>,
    completed_fec_sets: HashMap<FecKey, Instant>,
    slots: HashMap<u64, SlotState>,
    last_cleanup: Instant,
}

impl ShredProcessor {
    fn new() -> Self {
        Self {
            reed_solomon_cache: ReedSolomonCache::default(),
            fec_sets: HashMap::new(),
            completed_fec_sets: HashMap::new(),
            slots: HashMap::new(),
            last_cleanup: Instant::now(),
        }
    }

    fn process_packet(&mut self, packet: &[u8]) -> Result<Vec<DecodedTransaction>> {
        self.cleanup_if_needed();

        let shred = Shred::new_from_serialized_shred(packet.to_vec())
            .map_err(|err| anyhow!("failed to deserialize incoming raw shred: {err}"))?;
        let key = FecKey {
            slot: shred.slot(),
            fec_set_index: shred.fec_set_index(),
        };

        if self.completed_fec_sets.contains_key(&key) {
            return Ok(Vec::new());
        }

        self.insert_fec_shred(key, shred);

        let Some(completed_data_shreds) = self.try_complete_fec_set(key)? else {
            return Ok(Vec::new());
        };

        self.ingest_completed_fec_set(key.slot, completed_data_shreds)
    }

    fn insert_fec_shred(&mut self, key: FecKey, shred: Shred) {
        let now = Instant::now();
        let index = shred.index();
        let is_data = shred.is_data();
        let expected = if shred.is_code() {
            parse_num_data_shreds(&shred)
        } else {
            None
        };

        let state = self.fec_sets.entry(key).or_insert_with(|| FecSetState {
            fec_set_index: key.fec_set_index,
            data_shreds: BTreeMap::new(),
            code_shreds: BTreeMap::new(),
            expected_data_shreds: expected,
            last_update: now,
        });
        state.last_update = now;
        if state.expected_data_shreds.is_none() {
            state.expected_data_shreds = expected;
        }

        let target = if is_data {
            &mut state.data_shreds
        } else {
            &mut state.code_shreds
        };
        target.entry(index).or_insert(shred);
    }

    fn try_complete_fec_set(&mut self, key: FecKey) -> Result<Option<Vec<Shred>>> {
        let mut should_recover = false;
        let mut ready = false;

        {
            let Some(state) = self.fec_sets.get_mut(&key) else {
                return Ok(None);
            };

            if state.expected_data_shreds.is_none() {
                state.expected_data_shreds = infer_expected_data_shreds(state);
            }

            let Some(expected_data_shreds) = state.expected_data_shreds else {
                return Ok(None);
            };

            if state.data_shreds.len() >= expected_data_shreds {
                ready = true;
            } else if !state.code_shreds.is_empty()
                && state.data_shreds.len() + state.code_shreds.len() >= expected_data_shreds
            {
                should_recover = true;
            }
        }

        if should_recover {
            self.recover_fec_set(key)?;
            let Some(state) = self.fec_sets.get(&key) else {
                return Ok(None);
            };
            let Some(expected_data_shreds) = state.expected_data_shreds else {
                return Ok(None);
            };
            ready = state.data_shreds.len() >= expected_data_shreds;
        }

        if !ready {
            return Ok(None);
        }

        let Some(state) = self.fec_sets.remove(&key) else {
            return Ok(None);
        };
        self.completed_fec_sets.insert(key, Instant::now());

        let mut data_shreds = state.data_shreds.into_values().collect::<Vec<_>>();
        data_shreds.sort_by_key(Shred::index);
        Ok(Some(data_shreds))
    }

    fn recover_fec_set(&mut self, key: FecKey) -> Result<()> {
        let Some(state) = self.fec_sets.get(&key) else {
            return Ok(());
        };

        let shreds = state
            .data_shreds
            .values()
            .chain(state.code_shreds.values())
            .cloned()
            .collect::<Vec<_>>();
        let recovered = shred::recover(shreds, &self.reed_solomon_cache)
            .map_err(|err| anyhow!("failed to recover FEC set slot={} fec={}: {}", key.slot, key.fec_set_index, err))?;

        let Some(state) = self.fec_sets.get_mut(&key) else {
            return Ok(());
        };
        for recovered_shred in recovered {
            let recovered_shred = recovered_shred
                .map_err(|err| anyhow!("failed to materialize recovered shred slot={} fec={}: {}", key.slot, key.fec_set_index, err))?;
            if recovered_shred.is_data() {
                state
                    .data_shreds
                    .entry(recovered_shred.index())
                    .or_insert(recovered_shred);
            }
        }
        Ok(())
    }

    fn ingest_completed_fec_set(
        &mut self,
        slot: u64,
        data_shreds: Vec<Shred>,
    ) -> Result<Vec<DecodedTransaction>> {
        let now = Instant::now();
        let slot_state = self.slots.entry(slot).or_insert_with(|| SlotState {
            data_shreds: BTreeMap::new(),
            next_decode_index: None,
            last_update: now,
        });
        slot_state.last_update = now;

        for shred in data_shreds {
            slot_state.data_shreds.entry(shred.index()).or_insert(shred);
        }

        sync_slot_state(slot_state);
        drain_ready_batches(slot_state, slot)
    }

    fn cleanup_if_needed(&mut self) {
        if self.last_cleanup.elapsed() < CLEANUP_INTERVAL {
            return;
        }

        let now = Instant::now();
        self.fec_sets
            .retain(|_, state| now.duration_since(state.last_update) <= FEC_STATE_TTL);
        self.slots
            .retain(|_, state| now.duration_since(state.last_update) <= SLOT_STATE_TTL);
        self.completed_fec_sets
            .retain(|_, completed_at| now.duration_since(*completed_at) <= SLOT_STATE_TTL);
        self.last_cleanup = now;
    }
}

fn sync_slot_state(slot_state: &mut SlotState) {
    if slot_state.next_decode_index.is_some() {
        return;
    }

    if slot_state.data_shreds.contains_key(&0) {
        slot_state.next_decode_index = Some(0);
        return;
    }

    let Some(boundary) = slot_state
        .data_shreds
        .iter()
        .find_map(|(index, shred)| shred.data_complete().then_some(*index))
    else {
        return;
    };

    slot_state.data_shreds.retain(|index, _| *index > boundary);
    slot_state.next_decode_index = boundary.checked_add(1);
}

fn drain_ready_batches(slot_state: &mut SlotState, slot: u64) -> Result<Vec<DecodedTransaction>> {
    let mut decoded_transactions = Vec::new();

    loop {
        let Some(start_index) = slot_state.next_decode_index else {
            break;
        };

        let mut shreds = Vec::new();
        let mut cursor = start_index;
        let mut batch_end_index = None;

        loop {
            let Some(shred) = slot_state.data_shreds.get(&cursor).cloned() else {
                break;
            };

            let is_data_complete = shred.data_complete();
            shreds.push(shred);

            if is_data_complete {
                batch_end_index = Some(cursor);
                break;
            }

            let Some(next_cursor) = cursor.checked_add(1) else {
                batch_end_index = Some(cursor);
                break;
            };
            cursor = next_cursor;
        }

        let Some(batch_end_index) = batch_end_index else {
            break;
        };

        match decode_batch(slot, start_index, batch_end_index, &shreds) {
            Ok(mut batch_transactions) => decoded_transactions.append(&mut batch_transactions),
            Err(err) => {
                warn!(slot, start_index, batch_end_index, error = %err, "Failed to decode completed raw shred batch");
            }
        }

        for index in start_index..=batch_end_index {
            slot_state.data_shreds.remove(&index);
        }
        slot_state.next_decode_index = batch_end_index.checked_add(1);
    }

    Ok(decoded_transactions)
}

fn decode_batch(
    slot: u64,
    batch_start_index: u32,
    batch_end_index: u32,
    shreds: &[Shred],
) -> Result<Vec<DecodedTransaction>> {
    let payload = Shredder::deshred(shreds.iter().map(Shred::payload)).map_err(|err| {
        anyhow!(
            "deshred failed for slot={} batch={}..{}: {}",
            slot,
            batch_start_index,
            batch_end_index,
            err
        )
    })?;
    let entries: Vec<Entry> = wincode::deserialize(&payload).map_err(|err| {
        anyhow!(
            "entry decode failed for slot={} batch={}..{}: {}",
            slot,
            batch_start_index,
            batch_end_index,
            err
        )
    })?;

    let mut decoded_transactions = Vec::new();
    for entry in entries {
        for transaction in entry.transactions {
            decoded_transactions.push(DecodedTransaction { slot, transaction });
        }
    }
    Ok(decoded_transactions)
}

fn infer_expected_data_shreds(state: &FecSetState) -> Option<usize> {
    if let Some(expected) = state.expected_data_shreds {
        return Some(expected);
    }

    if state.data_shreds.len() >= DATA_SHREDS_PER_FEC_BLOCK {
        return Some(DATA_SHREDS_PER_FEC_BLOCK);
    }

    let last_in_slot_index = state
        .data_shreds
        .values()
        .find_map(|shred| shred.last_in_slot().then_some(shred.index()))?;
    let count = last_in_slot_index.checked_sub(state.fec_set_index)? + 1;
    usize::try_from(count).ok()
}

fn parse_num_data_shreds(shred: &Shred) -> Option<usize> {
    if !shred.is_code() {
        return None;
    }

    let payload = shred.payload();
    let bytes =
        <[u8; 2]>::try_from(payload.get(CODING_HEADER_OFFSET..CODING_HEADER_OFFSET + 2)?).ok()?;
    Some(usize::from(u16::from_le_bytes(bytes)))
}

#[inline(always)]
fn recv_packet(socket: &UdpSocket, packet: &mut [u8]) -> io::Result<usize> {
    #[cfg(target_os = "linux")]
    {
        let recv_len = unsafe {
            libc::recv(
                socket.as_raw_fd(),
                packet.as_mut_ptr().cast(),
                packet.len(),
                0,
            )
        };
        if recv_len >= 0 {
            Ok(recv_len as usize)
        } else {
            Err(io::Error::last_os_error())
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        socket.recv_from(packet).map(|(len, _)| len)
    }
}

#[inline(always)]
fn set_socket_recvbuf(socket: &UdpSocket, recvbuf: usize) -> io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        let recvbuf = recvbuf.min(i32::MAX as usize) as libc::c_int;
        let rc = unsafe {
            libc::setsockopt(
                socket.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_RCVBUF,
                (&recvbuf as *const libc::c_int).cast(),
                size_of_val(&recvbuf) as libc::socklen_t,
            )
        };
        if rc == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (socket, recvbuf);
        Ok(())
    }
}
