use std::{ops::Range, sync::{Arc, Mutex}};

use backon::{ExponentialBuilder, Retryable};
use evm_state::{BlockNum, Block};
use futures::future::join_all;
use solana_storage_bigtable::LedgerStorage;

use crate::{cli::HealthCheckBigtEvmBlocks, error::AppError};

use super::scan_evm_state_roots::{bigtable_fetcha::ChunkedRange, range_map};

pub async fn fetch_one(
    bigtable: &LedgerStorage,
    range: &Range<BlockNum>,
) -> Result<Vec<Block>, solana_storage_bigtable::Error> {
    let block_res = bigtable
        .get_evm_confirmed_full_blocks(range.start, range.end - 1)
        .await;
    block_res
}

const MIN_DELAY_NANOS: u32 = 1_000_000; // 1 millisecond
const MAX_TIMES: usize = 22; // 1 millisecond
pub async fn fetch_one_retry_backoff(
    bigtable: &LedgerStorage,
    range: &Range<BlockNum>,
) -> Result<Vec<Block>, solana_storage_bigtable::Error> {
    let count = Arc::new(Mutex::new(0));
    let fetch_cl = || {
        {
            let mut lock = count.lock().expect("locked poisoned");
            *lock += 1;
        }
        async {
            log::trace!(
                "attempting try fo fetch block_num ({:?}) {:?}",
                count.clone(),
                range
            );
            fetch_one(bigtable, range).await
        }
    };

    fetch_cl
        .retry(
            &ExponentialBuilder::default()
                .with_min_delay(std::time::Duration::new(0, MIN_DELAY_NANOS))
                .with_max_times(MAX_TIMES),
        )
        .await
}
pub async fn command(args: &HealthCheckBigtEvmBlocks) -> Result<(), AppError> {
    let HealthCheckBigtEvmBlocks {
        start,
        end_exclusive,
        rangemap_json,
    } = args;
    let rangemap = range_map::MasterRange::new(rangemap_json)?;

    let bigtable = solana_storage_bigtable::LedgerStorage::new(
        false,
        Some(std::time::Duration::new(5, 0)),
        None,
    )
    .await
    .map_err(|source| AppError::OpenLedger {
        source,
        creds_path: None,
        instance: "velas-ledger".to_string(),
    })?;

    let chunked = ChunkedRange {
        range: *start..*end_exclusive,
        chunk_size: 3_000_000,
    };
    let mut vec_joins = vec![];
    for subrange in chunked {
        let rangemap_clone = rangemap.clone();
        let bt_clone = bigtable.clone();

        let jh = tokio::task::spawn(async move {
            let chunked = ChunkedRange { range: subrange, chunk_size: 5_000};
            for chunk in chunked {

                let result = fetch_one_retry_backoff(&bt_clone, &chunk).await;
                if result.is_err() {
                    eprintln!("{:?}", result);
                }
                let result = result.expect("error pissing through retries");

                for block in result {
                    let bn = block.header.block_number;
                    rangemap_clone.update(bn, "Present".to_string()).expect("stdio erroro on persist range");
                    
                }

                rangemap_clone.persist_external().expect("std io error on persist range");
                println!("{} -> {} worked through", chunk.start, chunk.end );
                
            }
            
        });
        vec_joins.push(jh);

    }
    join_all(vec_joins).await;

    
    Ok(())
}
