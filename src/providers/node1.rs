use std::{
    error::Error,
    io::{self, Write},
    net::{Shutdown, SocketAddrV4, TcpStream, UdpSocket},
    sync::atomic::Ordering,
    time::Duration,
};

#[cfg(target_os = "linux")]
use std::{mem::size_of_val, os::fd::AsRawFd};

use serde::Serialize;
use solana_pubkey::Pubkey;
use tokio::{sync::broadcast::error::TryRecvError, task};
use tracing::{Level, error, info, warn};

use crate::{
    config::{Config, Endpoint, EndpointKind},
    utils::{TransactionData, get_current_timestamp, open_log_file, write_log_entry},
};

use super::{
    GeyserProvider, ProviderContext,
    common::{
        TransactionAccumulator, build_signature_envelope, enqueue_signature, fatal_connection_error,
    },
    xw_tx::{
        matches_account_filter, parse_account_filter, parse_udp_bind_addr, parse_udp_tx_payload,
    },
};

const NODE1_STREAM_RECVBUF_BYTES: usize = 16 * 1024 * 1024;
const NODE1_STREAM_PACKET_CAP: usize = 65_535;
const NODE1_STREAM_POLL_INTERVAL: Duration = Duration::from_millis(200);
const DEFAULT_TX_STREAM_CONTROL_ADDR: &str = "127.0.0.1:3031";

#[derive(Debug, Serialize)]
struct StreamFilter {
    account_include: Vec<Pubkey>,
}

pub struct Node1Provider;

impl GeyserProvider for Node1Provider {
    fn process(
        &self,
        endpoint: Endpoint,
        config: Config,
        context: ProviderContext,
    ) -> task::JoinHandle<Result<(), Box<dyn Error + Send + Sync>>> {
        task::spawn(async move { process_node1_endpoint(endpoint, config, context).await })
    }
}

async fn process_node1_endpoint(
    endpoint: Endpoint,
    config: Config,
    context: ProviderContext,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    task::spawn_blocking(move || run_node1_stream_loop(endpoint, config, context))
        .await
        .map_err(|err| io::Error::other(format!("node1 worker join failed: {err}")))?
}

fn run_node1_stream_loop(
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
    let bind_addr = match bind_addr {
        std::net::SocketAddr::V4(addr) => addr,
        std::net::SocketAddr::V6(addr) => fatal_connection_error(
            &endpoint_name,
            format!("node1 only supports IPv4 bind addresses, got {addr}"),
        ),
    };

    let mut log_file = if tracing::enabled!(Level::TRACE) {
        Some(open_log_file(&endpoint_name)?)
    } else {
        None
    };

    info!(endpoint = %endpoint_name, bind = %bind_addr, "Binding node1 UDP listener");
    let socket = UdpSocket::bind(SocketAddrV4::new(*bind_addr.ip(), bind_addr.port()))
        .unwrap_or_else(|err| fatal_connection_error(&endpoint_name, err));
    socket
        .set_read_timeout(Some(NODE1_STREAM_POLL_INTERVAL))
        .unwrap_or_else(|err| fatal_connection_error(&endpoint_name, err));
    set_socket_recvbuf(&socket, NODE1_STREAM_RECVBUF_BYTES)
        .unwrap_or_else(|err| fatal_connection_error(&endpoint_name, err));
    info!(endpoint = %endpoint_name, bind = %bind_addr, "node1 UDP listener ready");

    let tx_stream_control_addr = endpoint.control_addr.clone().or_else(|| {
        (endpoint.kind == EndpointKind::TxStream)
            .then(|| DEFAULT_TX_STREAM_CONTROL_ADDR.to_string())
    });

    if let Some(control_addr) = tx_stream_control_addr.as_deref() {
        let subscribed_accounts = account_filter.as_deref().unwrap_or_else(|| {
            fatal_connection_error(
                &endpoint_name,
                "tx_stream subscriptions require config.account to contain at least one pubkey",
            )
        });
        send_tx_stream_subscribe_request(
            control_addr,
            bind_addr.port(),
            subscribed_accounts,
            endpoint.is_follow,
        )
        .unwrap_or_else(|err| fatal_connection_error(&endpoint_name, err));
        info!(
            endpoint = %endpoint_name,
            control_addr,
            local_port = bind_addr.port(),
            is_follow = endpoint.is_follow,
            accounts = subscribed_accounts.len(),
            "tx_stream subscribe request sent"
        );
    }

    let mut accumulator = TransactionAccumulator::new();
    let mut transaction_count = 0usize;
    let mut packet = vec![0u8; NODE1_STREAM_PACKET_CAP];

    loop {
        match shutdown_rx.try_recv() {
            Ok(()) | Err(TryRecvError::Closed) => {
                info!(endpoint = %endpoint_name, "Received stop signal");
                break;
            }
            Err(TryRecvError::Lagged(skipped)) => {
                info!(endpoint = %endpoint_name, skipped, "Shutdown signal lagged; stopping node1 listener");
                break;
            }
            Err(TryRecvError::Empty) => {}
        }

        let len = match recv_packet(&socket, packet.as_mut_slice()) {
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
                error!(endpoint = %endpoint_name, error = %err, "node1 UDP receive failed");
                continue;
            }
        };

        let (slot, tx) = match parse_udp_tx_payload(&packet[..len]) {
            Ok(value) => value,
            Err(err) => {
                error!(endpoint = %endpoint_name, error = %err, "Failed to deserialize node1 payload");
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

    if let Some(control_addr) = tx_stream_control_addr.as_deref() {
        match send_tx_stream_unsubscribe_request(control_addr, bind_addr.port()) {
            Ok(()) => info!(
                endpoint = %endpoint_name,
                control_addr,
                local_port = bind_addr.port(),
                "tx_stream unsubscribe request sent"
            ),
            Err(err) => warn!(
                endpoint = %endpoint_name,
                control_addr,
                local_port = bind_addr.port(),
                error = %err,
                "Failed to send tx_stream unsubscribe request"
            ),
        }
    }

    let unique_signatures = accumulator.len();
    let collected = accumulator.into_inner();
    comparator.add_batch(&endpoint_name, collected);
    info!(
        endpoint = %endpoint_name,
        total_transactions = transaction_count,
        unique_signatures,
        "node1 UDP listener closed"
    );
    Ok(())
}

fn send_tx_stream_subscribe_request(
    control_addr: &str,
    local_port: u16,
    subscribed_accounts: &[Pubkey],
    is_follow: bool,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let filter = StreamFilter {
        account_include: subscribed_accounts.to_vec(),
    };

    let mut request = Vec::new();
    request.extend_from_slice(&local_port.to_le_bytes());

    let mut payload = bincode::serialize(&filter)?;
    payload.push(if is_follow { 1 } else { 0 });
    request.extend(payload);

    write_tx_stream_control_request(control_addr, &request)
}

fn send_tx_stream_unsubscribe_request(
    control_addr: &str,
    local_port: u16,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let filter = StreamFilter {
        account_include: Vec::new(),
    };

    let mut request = Vec::new();
    request.extend_from_slice(&local_port.to_le_bytes());
    request.extend(bincode::serialize(&filter)?);

    write_tx_stream_control_request(control_addr, &request)
}

fn write_tx_stream_control_request(
    control_addr: &str,
    request: &[u8],
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut stream = TcpStream::connect(control_addr)?;
    stream.write_all(request)?;
    stream.shutdown(Shutdown::Both).ok();
    Ok(())
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
