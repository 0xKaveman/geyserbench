use futures::{SinkExt, channel::mpsc::unbounded};
use futures_util::stream::StreamExt;
use solana_transaction::versioned::VersionedTransaction;
use std::{collections::HashMap, error::Error, sync::atomic::Ordering};
use tokio::task;
use tracing::{Level, error, info, trace};

use crate::{
    config::{Config, Endpoint},
    utils::{TransactionData, get_current_timestamp, open_log_file, write_log_entry},
};

use super::{
    GeyserProvider, ProviderContext,
    common::{
        TransactionAccumulator, build_signature_envelope, enqueue_signature, fatal_connection_error,
    },
    xw_tx::{matches_account_filter, parse_account_filter},
};

#[allow(clippy::all, dead_code)]
pub mod shreder_binary {
    include!(concat!(env!("OUT_DIR"), "/shreder_binary.rs"));
}

use shreder_binary::{
    SubscribeBinaryTransactionsRequest, SubscribeRequestFilterBinaryTransactions,
    shreder_binary_service_client::ShrederBinaryServiceClient,
};

pub struct ShrederBinaryProvider;

impl GeyserProvider for ShrederBinaryProvider {
    fn process(
        &self,
        endpoint: Endpoint,
        config: Config,
        context: ProviderContext,
    ) -> task::JoinHandle<Result<(), Box<dyn Error + Send + Sync>>> {
        task::spawn(async move { process_shreder_binary_endpoint(endpoint, config, context).await })
    }
}

async fn process_shreder_binary_endpoint(
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
    let required_accounts = account_filter
        .as_ref()
        .map(|accounts| accounts.iter().map(ToString::to_string).collect())
        .unwrap_or_default();
    let endpoint_name = endpoint.name.clone();

    let mut log_file = if tracing::enabled!(Level::TRACE) {
        Some(open_log_file(&endpoint_name)?)
    } else {
        None
    };

    let endpoint_url = endpoint.url.clone();

    info!(endpoint = %endpoint_name, url = %endpoint_url, "Connecting");

    let mut client = ShrederBinaryServiceClient::connect(endpoint_url.clone())
        .await
        .unwrap_or_else(|err| fatal_connection_error(&endpoint_name, err));
    info!(endpoint = %endpoint_name, "Connected");

    let mut transactions: HashMap<String, SubscribeRequestFilterBinaryTransactions> =
        HashMap::with_capacity(1);
    transactions.insert(
        String::from("account"),
        SubscribeRequestFilterBinaryTransactions {
            account_exclude: vec![],
            account_include: vec![],
            account_required: required_accounts,
        },
    );

    let request = SubscribeBinaryTransactionsRequest { transactions };
    let (mut subscribe_tx, subscribe_rx) =
        unbounded::<shreder_binary::SubscribeBinaryTransactionsRequest>();
    subscribe_tx.send(request).await?;
    let mut stream = client
        .subscribe_binary_transactions(subscribe_rx)
        .await?
        .into_inner();

    let mut accumulator = TransactionAccumulator::new();
    let mut transaction_count = 0usize;

    loop {
        tokio::select! { biased;
            _ = shutdown_rx.recv() => {
                info!(endpoint = %endpoint_name, "Received stop signal");
                break;
            }

            message = stream.next() => {
                if let Some(m) = message.as_ref() {
                    trace!(endpoint = %endpoint_name, ?m, "Received stream message");
                }

                let Some(Ok(msg)) = message else { continue };
                let Some(tx_update) = msg.transaction.as_ref() else { continue };
                let Some(binary_tx) = tx_update.transaction.as_ref() else { continue };

                let versioned_tx = match bincode::deserialize::<VersionedTransaction>(
                    &binary_tx.binary_transaction,
                ) {
                    Ok(tx) => tx,
                    Err(err) => {
                        error!(endpoint = %endpoint_name, error = %err, "Failed to deserialize binary transaction");
                        continue;
                    }
                };

                if !matches_account_filter(
                    account_filter.as_deref(),
                    versioned_tx.message.static_account_keys(),
                ) || versioned_tx.signatures.is_empty()
                {
                    continue;
                }

                let wallclock = get_current_timestamp();
                let elapsed = start_instant.elapsed();
                let signature = versioned_tx.signatures[0].to_string();

                if let Some(file) = log_file.as_mut() {
                    write_log_entry(file, wallclock, &endpoint_name, &signature)?;
                }

                let tx_data = TransactionData {
                    slot: Some(tx_update.slot),
                    wallclock_secs: wallclock,
                    elapsed_since_start: elapsed,
                    start_wallclock_secs,
                };

                let updated = accumulator.record(signature.clone(), tx_data.clone());

                if updated && let Some(envelope) = build_signature_envelope(
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
        "Stream closed after dispatching transactions"
    );
    Ok(())
}
