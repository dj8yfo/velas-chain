use std::{
    ops::Range,
    sync::Arc,
    time::{Duration, Instant},
};

use evm_state::BlockNum;
use tokio::sync::{
    mpsc::{error::SendError, Sender},
    Semaphore,
};

use crate::triedb::{
    client::{
        proto::app_grpc::backend_client::BackendClient,
        sync::range_processor::kickstart_point::{Entry, KickStartPoint},
    },
    debug_elapsed,
    error::{DiffRequest, StageOneError},
    EvmHeightIndex,
};

use super::diff_stages;

pub mod steel_container;

#[derive(Debug)]
pub struct StageOnePayload {
    pub ledger_storage_dur: Duration,
    pub diff_request_dur: Duration,
    pub request: DiffRequest,
    pub changeset: Vec<triedb::DiffChange>,
}

pub async fn request_short_height<S>(
    block_storage: S,
    job_for_a_cowboy: BackendClient<tonic::transport::Channel>,
    state_rpc_address: String,
    stage_one_output: Sender<Result<StageOnePayload, StageOneError>>,
    target: BlockNum,
) -> Result<(), SendError<Result<StageOnePayload, StageOneError>>>
where
    S: EvmHeightIndex + Clone + Sync + Send + 'static,
{
    let from_height = target - 1;

    let mut instant = Instant::now();
    let heights = vec![from_height, target];

    let mut hashes = vec![];
    for height in heights {
        let hash = block_storage
            .get_evm_confirmed_state_root_retried(height)
            .await;
        let hash = match hash {
            Ok(hash) => hash,
            Err(err) => {
                let send_res = stage_one_output.send(Err(err.into())).await;
                return send_res;
            }
        };
        hashes.push(hash);
    }
    let ledger_storage_dur = debug_elapsed(&mut instant);
    let (from_hash, target_hash) = (hashes[0], hashes[1]);

    let request = DiffRequest {
        heights: (from_height, target),
        expected_hashes: (from_hash, target_hash),
    };
    let result = diff_stages::one::<S>(&job_for_a_cowboy, request, &state_rpc_address).await;
    let result = result.map(|(diff_request_dur, request, changeset)| StageOnePayload {
        ledger_storage_dur,
        diff_request_dur,
        request,
        changeset,
    });

    stage_one_output.send(result.map_err(Into::into)).await
}

pub async fn request_long_height<S>(
    block_storage: S,
    job_for_a_cowboy: BackendClient<tonic::transport::Channel>,
    state_rpc_address: String,
    from: Entry,
    stage_one_output: Sender<Result<StageOnePayload, StageOneError>>,
    target: BlockNum,
) -> Result<(), SendError<Result<StageOnePayload, StageOneError>>>
where
    S: EvmHeightIndex + Clone + Sync + Send + 'static,
{

    let mut instant = Instant::now();
    let target_hash = block_storage
        .get_evm_confirmed_state_root_retried(target)
        .await;
    let ledger_storage_dur = debug_elapsed(&mut instant);
    let target_hash = match target_hash {
        Ok(hash) => hash,
        Err(err) => {
            let send_res = stage_one_output.send(Err(err.into())).await;
            return send_res;
        }
    };

    let request = DiffRequest {
        heights: (from.height, target),
        expected_hashes: (from.hash, target_hash),
    };
    let result = diff_stages::one::<S>(&job_for_a_cowboy, request, &state_rpc_address).await;
    let result = result.map(|(diff_request_dur, request, changeset)| StageOnePayload {
        ledger_storage_dur,
        diff_request_dur,
        request,
        changeset,
    });

    stage_one_output.send(result.map_err(Into::into)).await
}

pub async fn process<S>(
    job_for_a_cowboy: &BackendClient<tonic::transport::Channel>,
    block_storage: &S,
    range: Range<BlockNum>,
    kickstart_point: KickStartPoint,
    state_rpc_address: String,
    stage_one_output: Sender<Result<StageOnePayload, StageOneError>>,
    request_workers: u32,
) where
    S: EvmHeightIndex + Clone + Sync + Send + 'static,
{
    let s = Arc::new(Semaphore::new(request_workers as usize));

    for target in range {
        let permit = s
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore closed?!?");

        let from = kickstart_point.get();
        let height_diff = target - from.height;
        let _jh = tokio::task::spawn({
            let block_storage = block_storage.clone();
            let state_rpc_address = state_rpc_address.clone();
            let job_for_a_cowboy = job_for_a_cowboy.clone();
            let stage_one_output = stage_one_output.clone();
            async move {
                let send_res = request_short_height(
                    block_storage.clone(),
                    job_for_a_cowboy.clone(),
                    state_rpc_address.clone(),
                    stage_one_output.clone(),
                    target,
                )
                .await;
                if send_res.is_err() {
                    log::error!("stage two input closed");
                }

                if height_diff > 1 {
                    let send_res = request_long_height(
                        block_storage,
                        job_for_a_cowboy,
                        state_rpc_address,
                        from,
                        stage_one_output,
                        target,
                    )
                    .await;
                    if send_res.is_err() {
                        log::error!("stage two input closed");
                    }
                }
                drop(permit);
            }
        });
    }
}
