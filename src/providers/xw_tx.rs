use std::{error::Error, net::SocketAddr, sync::atomic::Ordering};

use tokio::{net::UdpSocket, task};
use tracing::{Level, error, info};

use solana_pubkey::Pubkey;
use solana_transaction::versioned::VersionedTransaction;

use crate::{
    config::{Config, Endpoint},
    utils::{TransactionData, get_current_timestamp, open_log_file, write_log_entry},
};

use super::{
    GeyserProvider, ProviderContext,
    common::{
        TransactionAccumulator, build_signature_envelope, enqueue_signature, fatal_connection_error,
    },
};

pub struct XwTxProvider;

impl GeyserProvider for XwTxProvider {
    fn process(
        &self,
        endpoint: Endpoint,
        config: Config,
        context: ProviderContext,
    ) -> task::JoinHandle<Result<(), Box<dyn Error + Send + Sync>>> {
        task::spawn(async move { process_xw_tx_endpoint(endpoint, config, context).await })
    }
}

async fn process_xw_tx_endpoint(
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

    info!(endpoint = %endpoint_name, bind = %bind_addr, "Binding UDP listener");
    let socket = UdpSocket::bind(bind_addr)
        .await
        .unwrap_or_else(|err| fatal_connection_error(&endpoint_name, err));
    info!(endpoint = %endpoint_name, "UDP listener ready");

    let mut accumulator = TransactionAccumulator::new();
    let mut transaction_count = 0usize;
    let mut buffer = [0u8; 2048];

    loop {
        tokio::select! { biased;
        _ = shutdown_rx.recv() => {
            info!(endpoint = %endpoint_name, "Received stop signal");
            break;
        }

        recv = socket.recv_from(&mut buffer) => {
            let (size, _peer) = match recv {
                Ok(value) => value,
                Err(err) => {
                    error!(endpoint = %endpoint_name, error = %err, "UDP receive failed");
                    continue;
                }
            };
            let (slot, tx) = match parse_udp_tx_payload(&buffer[..size]) {
                Ok(value) => value,
                Err(err) => {
                    error!(endpoint = %endpoint_name, error = %err, "Failed to deserialize xw_tx payload");
                    continue;
                }
            };

            if !matches_account_filter(account_filter.as_deref(), tx.message.static_account_keys())
                || tx.signatures.is_empty()
            {
                continue;
            }

            let wallclock = get_current_timestamp();
            let elapsed = start_instant.elapsed();
            let signature = tx.signatures[0].to_string();

            if let Some(file) = log_file.as_mut() {
                write_log_entry(file, wallclock, &endpoint_name, &signature)?;
            }

            let tx_data = TransactionData {
                slot,
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
                ) {
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
    }

    let unique_signatures = accumulator.len();
    let collected = accumulator.into_inner();
    comparator.add_batch(&endpoint_name, collected);
    info!(
        endpoint = %endpoint_name,
        total_transactions = transaction_count,
        unique_signatures,
        "UDP xw_tx listener closed"
    );
    Ok(())
}

pub(crate) fn parse_udp_bind_addr(url: &str) -> Result<SocketAddr, Box<dyn Error + Send + Sync>> {
    let raw = url.strip_prefix("udp://").unwrap_or(url);
    Ok(raw.parse()?)
}

pub(crate) fn parse_account_filter(
    value: &str,
) -> Result<Option<Vec<Pubkey>>, Box<dyn Error + Send + Sync>> {
    let trimmed = value.trim();
    if trimmed.is_empty() || trimmed == "*" {
        Ok(None)
    } else {
        let accounts = trimmed
            .split([',', '\n', '\r', ' ', '\t'])
            .map(str::trim)
            .filter(|item| !item.is_empty())
            .map(str::parse::<Pubkey>)
            .collect::<Result<Vec<_>, _>>()?;
        if accounts.is_empty() {
            Ok(None)
        } else {
            Ok(Some(accounts))
        }
    }
}

pub(crate) fn matches_account_filter<K>(filter: Option<&[Pubkey]>, keys: &[K]) -> bool
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

pub(crate) fn parse_udp_tx_payload(
    payload: &[u8],
) -> Result<(Option<u64>, VersionedTransaction), Box<dyn Error + Send + Sync>> {
    if payload.len() >= 8 {
        let slot = u64::from_le_bytes(payload[..8].try_into()?);
        if let Ok(tx) = bincode::deserialize::<VersionedTransaction>(&payload[8..]) {
            return Ok((Some(slot), tx));
        }
    }
    Ok((None, bincode::deserialize::<VersionedTransaction>(payload)?))
}

#[cfg(test)]
mod tests {
    use super::{matches_account_filter, parse_account_filter, parse_udp_tx_payload};
    use bincode::serialize;
    use solana_pubkey::Pubkey;
    use solana_transaction::versioned::VersionedTransaction;

    fn sample_tx() -> VersionedTransaction {
        VersionedTransaction::default()
    }

    #[test]
    fn parses_slot_prefixed_payload() {
        let slot = 77u64;
        let tx = sample_tx();
        let mut datagram = slot.to_le_bytes().to_vec();
        datagram.extend_from_slice(&serialize(&tx).unwrap());
        let (decoded_slot, decoded_tx) = parse_udp_tx_payload(&datagram).unwrap();
        assert_eq!(decoded_slot, Some(slot));
        assert_eq!(decoded_tx, tx);
    }

    #[test]
    fn falls_back_to_legacy_unprefixed_payload() {
        let tx = sample_tx();
        let payload = serialize(&tx).unwrap();
        let (decoded_slot, decoded_tx) = parse_udp_tx_payload(&payload).unwrap();
        assert_eq!(decoded_slot, None);
        assert_eq!(decoded_tx, tx);
    }

    #[test]
    fn parses_multi_account_filter() {
        let filter = parse_account_filter(
            "11111111111111111111111111111111, Sysvar1111111111111111111111111111111111111",
        )
        .unwrap()
        .unwrap();

        assert_eq!(filter.len(), 2);
        let keys = vec![
            "Sysvar1111111111111111111111111111111111111"
                .parse::<Pubkey>()
                .unwrap(),
        ];
        assert!(matches_account_filter(Some(filter.as_slice()), &keys));
    }
}
