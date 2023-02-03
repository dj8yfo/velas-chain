use std::time::Instant;

use log::info;

use evm_rpc::FormatHex;
use evm_state::{empty_trie_hash, storage::account_extractor, Storage, StorageSecondary, H256};
use rlp::Rlp;
use triedb::{gc::ReachableHashes, merkle::MerkleNode, DiffChange};

use super::UsedStorage;
use app_grpc::backend_server::{Backend, BackendServer};
use tonic::{Request, Response, Status};

use app_grpc::PingReply;
pub mod app_grpc {
    tonic::include_proto!("triedb_repl");
}

use crate::triedb::{
    check_root, debug_elapsed, lock_root, range::MasterRange, LittleBig, NodePath,
};

pub struct Server<S> {
    storage: UsedStorage,
    range: MasterRange,
    block_threshold: evm_state::BlockNum,
    block_storage: S,
}

impl<S> Server<S>
where
    S: LittleBig + Send + Sync + 'static,
{
    pub fn new(
        storage: UsedStorage,
        range: MasterRange,
        block_threshold: evm_state::BlockNum,
        block_storage: S,
    ) -> BackendServer<Self> {
        BackendServer::new(Server {
            storage,
            range,
            block_threshold,
            block_storage,
        })
    }

    async fn fetch_state_roots(
        &self,
        from: evm_state::BlockNum,
        to: evm_state::BlockNum,
    ) -> anyhow::Result<(H256, H256)> {
        let from = self
            .block_storage
            .get_evm_confirmed_state_root(from)
            .await?;
        let to = self.block_storage.get_evm_confirmed_state_root(to).await?;
        Ok((from, to))
    }

    fn get_state_diff_gc_storage(
        from: H256,
        to: H256,
        storage: &Storage,
    ) -> Result<Vec<DiffChange>, Status> {
        let start = Instant::now();

        let db_handle = storage.rocksdb_trie_handle();
        let _from_guard = lock_root(&db_handle, from, evm_state::storage::account_extractor)
            .map_err(|err| Status::not_found(format!("failure to lock root {}", err)))?;
        let _to_guard = lock_root(&db_handle, to, evm_state::storage::account_extractor)
            .map_err(|err| Status::not_found(format!("failure to lock root {}", err)))?;
        debug_elapsed("locked roots", &start);

        let ach = triedb::rocksdb::SyncRocksHandle::new(triedb::rocksdb::RocksDatabaseHandle::new(
            storage.db(),
        ));

        let changeset = triedb::diff(&ach, evm_state::storage::account_extractor, from, to)
            .map_err(|err| {
                log::error!("triedb::diff {:?}", err);
                Status::internal("Cannot calculate diff between states")
            })?;
        debug_elapsed("retrieved changeset", &start);
        Ok(changeset)
    }

    fn get_state_diff_secondary_storage(
        from: H256,
        to: H256,
        storage: &StorageSecondary,
    ) -> Result<Vec<DiffChange>, Status> {
        let start = Instant::now();

        let db = storage.db();
        check_root(db, from).map_err(|err| Status::not_found(format!("check root {}", err)))?;
        check_root(db, to).map_err(|err| Status::not_found(format!("check root {}", err)))?;
        debug_elapsed("locked roots", &start);

        let ach = storage.rocksdb_trie_handle();

        let changeset = triedb::diff(&ach, evm_state::storage::account_extractor, from, to)
            .map_err(|err| {
                log::error!("triedb::diff {:?}", err);
                Status::internal("Cannot calculate diff between states")
            })?;
        debug_elapsed("retrieved changeset", &start);
        Ok(changeset)
    }

    fn map_changeset(changeset: Vec<DiffChange>) -> Vec<app_grpc::Insert> {
        let mut reply_changeset = vec![];

        for change in changeset {
            match change {
                triedb::DiffChange::Insert(hash, data) => {
                    let raw_insert = app_grpc::Insert {
                        hash: Some(app_grpc::Hash {
                            value: hash.format_hex(),
                        }),
                        data: data.into(),
                    };
                    reply_changeset.push(raw_insert);
                }
                triedb::DiffChange::Removal(..) => {
                    // skip
                    // no need to transfer it over the wire
                }
            }
        }
        reply_changeset
    }

    fn get_bytes_body(&self, key: H256) -> Result<Vec<u8>, Status> {
        let maybe_bytes = match self.storage {
            UsedStorage::WritableWithGC(ref storage) => storage.db().get(key),

            UsedStorage::ReadOnlyNoGC(ref storage) => storage.db().get(key),
        };

        let value = if let Ok(option) = maybe_bytes {
            Ok(option)
        } else {
            Err(Status::internal("DB access error"))
        };
        let bytes = value?.ok_or_else(|| Status::not_found(format!("not found {}", key)))?;
        Ok(bytes)
    }
    fn state_diff_body(
        &self,
        from: H256,
        to: H256,
    ) -> Result<Response<app_grpc::GetStateDiffReply>, Status> {
        let changeset = match self.storage {
            UsedStorage::WritableWithGC(ref storage) => {
                Self::get_state_diff_gc_storage(from, to, storage)?
            }
            UsedStorage::ReadOnlyNoGC(ref storage) => {
                Self::get_state_diff_secondary_storage(from, to, storage)?
            }
        };

        let reply_changeset = Self::map_changeset(changeset);

        let reply = app_grpc::GetStateDiffReply {
            changeset: reply_changeset,
            first_root: Some(app_grpc::Hash {
                value: from.format_hex(),
            }),
            second_root: Some(app_grpc::Hash {
                value: to.format_hex(),
            }),
        };

        Ok(Response::new(reply))
    }
}

trait TryConvert<S>: Sized {
    type Error;

    fn try_from(value: S) -> Result<Self, Self::Error>;
}

impl TryConvert<Option<&app_grpc::Hash>> for H256 {
    type Error = tonic::Status;

    fn try_from(value: Option<&app_grpc::Hash>) -> Result<Self, Self::Error> {
        let hash = value.ok_or_else(|| Status::invalid_argument("empty arg"))?;

        let res = H256::from_hex(&hash.value).map_err(|_| {
            Status::invalid_argument(format!("Couldn't parse requested hash key {}", hash.value))
        })?;
        Ok(res)
    }
}

impl TryConvert<&app_grpc::PathElement> for (H256, bool) {
    type Error = tonic::Status;

    fn try_from(value: &app_grpc::PathElement) -> Result<Self, Self::Error> {
        let key = <H256 as TryConvert<_>>::try_from(value.hash.as_ref())?;
        Ok((key, value.direct))
    }
}
const MIN_PATH_LEN: usize = 3; // contracting 256 times ; 16^(N-1)

#[tonic::async_trait]
impl<S: LittleBig + Sync + Send + 'static> Backend for Server<S> {
    async fn ping(&self, request: Request<()>) -> Result<Response<PingReply>, Status> {
        info!("Got a request: {:?}", request);

        let reply = app_grpc::PingReply {
            message: "ABDULA STATUS 7".to_string(),
        };

        Ok(Response::new(reply))
    }

    async fn get_full_subtree(
        &self,
        request: tonic::Request<app_grpc::FullSubtreePath>,
    ) -> Result<tonic::Response<app_grpc::GetStateDiffReply>, tonic::Status> {
        info!("Got a request: {:?}", request);
        let request = request.into_inner();
        let len = request.paths.len();
        if len < MIN_PATH_LEN {
            return Err(Status::failed_precondition(format!(
                "must include more path segments, actual {}",
                len
            )));
        }
        for index in 0..request.paths.len() - 1 {
            let current = <(H256, bool) as TryConvert<_>>::try_from(&request.paths[index])?;
            let next = <(H256, bool) as TryConvert<_>>::try_from(&request.paths[index + 1])?;
            let node = self.get_bytes_body(current.0).map_err(|err| {
                Status::failed_precondition(format!(
                    "path supplied is bigus, try again {:#?} {:#?}",
                    err, request.paths
                ))
            })?;
            let found = find_node(current, node, next)?;
            if !found {
                return Err(Status::failed_precondition(format!(
                    "path supplied is bigus, try again {:#?}",
                    request.paths,
                )));
            }
        }

        let to = <H256 as TryConvert<_>>::try_from(request.paths.last().unwrap().hash.as_ref())?;

        self.state_diff_body(empty_trie_hash(), to)
    }
    async fn get_raw_bytes(
        &self,
        request: Request<app_grpc::GetRawBytesRequest>,
    ) -> Result<Response<app_grpc::GetRawBytesReply>, Status> {
        info!("Got a request: {:?}", request);

        let key = <H256 as TryConvert<_>>::try_from(request.into_inner().hash.as_ref())?;
        let bytes = self.get_bytes_body(key)?;

        let reply = app_grpc::GetRawBytesReply { node: bytes };

        Ok(Response::new(reply))
    }

    async fn get_state_diff(
        &self,
        request: Request<app_grpc::GetStateDiffRequest>,
    ) -> Result<Response<app_grpc::GetStateDiffReply>, Status> {
        info!("Got a request: {:?}", request);

        let inner = request.into_inner();
        let height_diff = if inner.to >= inner.from {
            inner.to - inner.from
        } else {
            inner.from - inner.to
        };
        if height_diff > self.block_threshold {
            return Err(Status::invalid_argument(format!(
                "blocks too far {}",
                inner.to - inner.from
            )));
        }
        let (from, to) = self
            .fetch_state_roots(inner.from, inner.to)
            .await
            .map_err(|err| {
                log::error!("fetch_state_roots encountered err {:?}", err);
                Status::internal("failure to fetch state roots")
            })?;
        self.state_diff_body(from, to)
    }

    async fn get_block_range(
        &self,
        _request: tonic::Request<()>,
    ) -> Result<tonic::Response<app_grpc::GetBlockRangeReply>, tonic::Status> {
        let r: std::ops::Range<evm_state::BlockNum> = self.range.get();
        let reply = app_grpc::GetBlockRangeReply {
            start: r.start,
            end: r.end,
        };

        Ok(Response::new(reply))
    }
}

impl<S: LittleBig + Sync + Send + 'static> BackendServer<Server<S>> {
    pub fn join(&self) -> Result<(), Box<(dyn std::error::Error + 'static)>> {
        Ok(())
    }
}

fn find_node(parent: (H256, bool), node: Vec<u8>, target: (H256, bool)) -> Result<bool, Status> {
    let node = MerkleNode::decode(&Rlp::new(&node)).map_err(|err| {
        log::error!("MerkleNode::decode {:?}", err);
        Status::internal("corruption in ministry of defence")
    })?;

    let (_hash, direct) = parent;
    let (direct_childs, indirect_childs) = if direct {
        ReachableHashes::collect(&node, account_extractor).childs()
    } else {
        // prevent more than one layer of indirection
        let childs = ReachableHashes::collect(&node, no_childs).childs();
        assert!(
            childs.1.is_empty(),
            "There should be no subtrie with 'no_childs' extractor"
        );
        // All direct childs for indirect childs should be handled as indirect.
        (vec![], childs.0)
    };

    let paths: Vec<_> = direct_childs
        .into_iter()
        .map(|k| (k, true))
        .chain(indirect_childs.into_iter().map(|k| (k, false)))
        .collect();
    Ok(paths.into_iter().find(|el| *el == target).is_some())
}

pub fn no_childs(_: &[u8]) -> Vec<H256> {
    vec![]
}
