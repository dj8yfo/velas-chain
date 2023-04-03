use std::{sync::Arc, time::Duration};

use evm_state::Storage;
use tokio::sync::{
    mpsc::{error::SendError, Receiver, Sender},
    Semaphore,
};
use triedb::gc::DbCounter;

use crate::triedb::{
    client::sync::range_processor::{
        kickstart_point::{Entry, KickStartPoint, SuccessHeights},
        kilosievert::diff_stages,
    },
    collection,
    error::{DiffRequest, StageOneError, StageTwoError},
};

use super::StageOnePayload;
mod shortcut_cache;

#[derive(Debug)]
pub struct StageTwoPayload {
    pub apply_duration: Duration,
    pub request: DiffRequest,
    pub changeset_len: usize,
    pub result_root_gc_count: usize,
}

async fn apply_height(
    stage_one: StageOnePayload,
    storage: Storage,
    kickstart_point: KickStartPoint,
    success_heights: SuccessHeights,
    stage_two_output: Sender<Result<StageTwoPayload, StageTwoError>>,
) -> Result<(), SendError<Result<StageTwoPayload, StageTwoError>>> {
    let changeset_len = stage_one.changeset.len();
    //
    // rocksdb's `ColumnFamily` being not `Send` prevents it being used across `await` point
    //
    let result = tokio::task::spawn_blocking(move || {
        let result: Result<StageTwoPayload, StageTwoError> = {
            let collection = collection(&storage);
            let apply_result =
                diff_stages::two((stage_one.request, stage_one.changeset), &collection);
            match apply_result {
                Ok((duration, root_guard)) => {
                    let target = root_guard.leak_root();
                    assert_eq!(stage_one.request.expected_hashes.1, target);

                    collection.database.gc_pin_root(target);
                    let result_count = collection.database.gc_count(target);
                    kickstart_point.update(stage_one.request.heights.1, target);
                    success_heights.push_height(stage_one.request.heights.1);
                    Ok(StageTwoPayload {
                        apply_duration: duration,
                        changeset_len,
                        request: stage_one.request,
                        result_root_gc_count: result_count,
                    })
                }
                Err(err) => Err(err.into()),
            }
        };
        result
    })
    .await;
    let result = match result {
        Err(err) => Err(err.into()),
        Ok(result) => result,
    };

    stage_two_output.send(result).await
}

pub async fn process(
    kickstart_point: KickStartPoint,
    success_heights: SuccessHeights,
    start: Entry,
    storage: Storage,
    mut stage_two_input: Receiver<Result<StageOnePayload, StageOneError>>,
    stage_two_output: Sender<Result<StageTwoPayload, StageTwoError>>,
    db_workers: u32,
) {
    let s = Arc::new(Semaphore::new(db_workers as usize));
    let mut shortcuts = shortcut_cache::ShortcutRequestCache::new();
    let _empty = shortcuts.height_ready(start.height);

    while let Some(stage_one_result) = stage_two_input.recv().await {

        let applied_heights= success_heights.take();
        for height in applied_heights {
            shortcuts.phase_out(height -1);
            let shortcut = shortcuts.height_ready(height);
            if let Some(stage_one) = shortcut
            {
                let permit = s
                    .clone()
                    .acquire_owned()
                    .await
                    .expect("semaphore closed?!?");
                let _jh = tokio::task::spawn({
                    let storage = storage.clone();
                    let kickstart_point = kickstart_point.clone();
                    let success_heights = success_heights.clone();
                    let stage_two_output = stage_two_output.clone();

                    async move {
                        let send_res =
                            apply_height(stage_one, storage, kickstart_point, success_heights, stage_two_output)
                                .await;
                        if send_res.is_err() {
                            log::error!("stage three input closed");
                        }
                        drop(permit);
                    }
                });
            }
            
        }
        let stage_one = match stage_one_result {
            Err(err) => {
                let send_res = stage_two_output.send(Err(err.into())).await;
                if send_res.is_err() {
                    log::error!("stage three input closed");
                }
                continue;
            }
            Ok(stage_one) => stage_one,
        };
        log::debug!(
            "< {:?} {:?} {} > - {:#?} ",
            stage_one.ledger_storage_dur,
            stage_one.diff_request_dur,
            stage_one.changeset.len(),
            stage_one.request
        );

        if stage_one.request.heights.1 - stage_one.request.heights.0 == 1 {
            if let Some(stage_one) = shortcuts.save_shortcut(stage_one.request.heights.0, stage_one)
            {
                let permit = s
                    .clone()
                    .acquire_owned()
                    .await
                    .expect("semaphore closed?!?");
                let _jh = tokio::task::spawn({
                    let storage = storage.clone();
                    let kickstart_point = kickstart_point.clone();
                    let success_heights = success_heights.clone();
                    let stage_two_output = stage_two_output.clone();

                    async move {
                        let send_res =
                            apply_height(stage_one, storage, kickstart_point, success_heights, stage_two_output)
                                .await;
                        if send_res.is_err() {
                            log::error!("stage three input closed");
                        }
                        drop(permit);
                    }
                });
            }
        } else {
            let target =  stage_one.request.heights.1;
            if !shortcuts.ready(target) && !shortcuts.sent_for_apply(target -1) {
                let permit = s
                    .clone()
                    .acquire_owned()
                    .await
                    .expect("semaphore closed?!?");
                let _jh = tokio::task::spawn({
                    let storage = storage.clone();
                    let kickstart_point = kickstart_point.clone();
                    let success_heights = success_heights.clone();
                    let stage_two_output = stage_two_output.clone();

                    async move {
                        let send_res =
                            apply_height(stage_one, storage, kickstart_point, success_heights, stage_two_output).await;
                        if send_res.is_err() {
                            log::error!("stage three input closed");
                        }
                        drop(permit);
                    }
                });
            }
        }
    }
}
