use std::time::Duration;

use methods::app_grpc::backend_client::BackendClient;
use rlp::Rlp;
use sha3::Keccak256;
use sha3::Digest;

use super::Layers;
use super::NodePath;
use super::{
    range::{Advance, MasterRange},
    LittleBig,
};
use evm_state::{BlockNum, Storage, H256, storage::account_extractor};
use log;
use triedb::{gc::{DbCounter, RootGuard, ReachableHashes}, merkle::MerkleNode};
mod methods;

type RocksHandleA<'a> = triedb::rocksdb::RocksHandle<'a, &'a triedb::rocksdb::DB>;

pub struct Client<S> {
    state_rpc_address: String,
    storage: Storage,
    client: BackendClient<tonic::transport::Channel>,
    range: MasterRange,
    block_storage: S,
}

impl From<methods::app_grpc::GetBlockRangeReply> for std::ops::Range<BlockNum> {
    fn from(value: methods::app_grpc::GetBlockRangeReply) -> Self {
        value.start..value.end
    }
}

const MAX_CHUNK: u64 = 10;
impl<S> Client<S> {
    pub async fn connect(
        state_rpc_address: String,
        range: MasterRange,
        storage: Storage,
        block_storage: S,
    ) -> Result<Self, tonic::transport::Error> {
        log::info!("starting the client routine {}", state_rpc_address);
        let client = BackendClient::connect(state_rpc_address.clone()).await?;
        Ok(Self {
            client,
            range,
            state_rpc_address,
            storage,
            block_storage,
        })
    }

    fn compute_advance(&self, server_offer: std::ops::Range<BlockNum>) -> Advance {
        self.range.compute_advance(MAX_CHUNK, server_offer)
    }

    pub fn db_handles(
        storage: &Storage,
    ) -> (
        RocksHandleA<'_>,
        triedb::gc::TrieCollection<RocksHandleA<'_>>,
    ) {
        (
            storage.rocksdb_trie_handle(),
            triedb::gc::TrieCollection::new(storage.rocksdb_trie_handle()),
        )
    }
}

impl<S> Client<S>
where
    S: LittleBig,
{
    async fn fetch_state_roots(
        &self,
        heights: (BlockNum, BlockNum),
    ) -> anyhow::Result<(H256, H256)> {
        let from = self
            .block_storage
            .get_evm_confirmed_state_root(heights.0)
            .await?;
        let to = self
            .block_storage
            .get_evm_confirmed_state_root(heights.1)
            .await?;
        Ok((from, to))
    }

    async fn iterate_range(&mut self, mut advance: Advance) -> anyhow::Result<()> {
        let (db_handle, collection) = Self::db_handles(&self.storage);
        let mut start = advance.start;
        log::warn!("attempting to advance {:?}", advance);

        while let Some(next) = advance.next_biderectional() {
            log::warn!("next height {}", next);
            let heights = (start, next);
            let hashes = self.fetch_state_roots(heights).await?;
            let diff_response = Self::download_and_apply_diff(
                &mut self.client,
                &db_handle,
                &collection,
                heights,
                hashes,
            )
            .await;
            match diff_response {
                Err(e) => {
                    return Err(anyhow::anyhow!(
                        "heights advance error: {:?}, {:?}, {:?}",
                        heights,
                        hashes,
                        e
                    ));
                }
                Ok(guard) => {
                    let to = hashes.1;
                    log::debug!("persisted root {}", guard.leak_root());
                    db_handle.gc_pin_root(to);
                    log::debug!("persisted root count after leak {}", db_handle.gc_count(to));
                    self.range.update(next).expect("persist range update");
                }
            }
            start = next;
        }
        Ok(())
    }

    async fn main_loop_iteration(&mut self) -> Result<Advance, (anyhow::Error, Duration)> {
        let block_range = self.get_block_range().await;
        if let Err(e) = block_range {
            return Err((
                anyhow::anyhow!(
                    "get block range from server {:?}: {:?}",
                    self.state_rpc_address,
                    e
                ),
                Duration::new(1, 0),
            ));
        }
        let block_range: std::ops::Range<BlockNum> = block_range.unwrap().into();

        let advance = self.compute_advance(block_range.clone());
        if advance.is_empty() {
            return Err((
                anyhow::anyhow!(
                    "no useful advance can be made on {:?};  our : {:?} ; offer: {:?}",
                    self.state_rpc_address,
                    self.range.get(),
                    block_range,
                ),
                Duration::new(3, 0),
            ));
        }
        let result = self.iterate_range(advance.clone()).await;
        if let Err(e) = result {
            Err((
                anyhow::anyhow!("during iteration over advance.added_range {:?}", e),
                Duration::new(5, 0),
            ))
        } else {
            Ok(advance)
        }
    }

    pub async fn extend_range_routine(&mut self) {
        assert!(!self.range.get().is_empty());
        loop {
            match self.main_loop_iteration().await {
                Err((err, dur)) => {
                    log::error!("main loop {:?}", err);
                    tokio::time::sleep(dur).await;
                }
                Ok(advance) => {
                    log::warn!("success on advance {:?}", advance);
                }
            }
        }
    }

    pub async fn prep_bootstrap_state_hashes(&mut self, height: u64) -> anyhow::Result<Layers> {

        let root_hash = self
            .block_storage
            .get_evm_confirmed_state_root(height)
            .await?;
        let root = self.get_raw_bytes(root_hash).await?.node;
        verify_hash(&root, root_hash)?;
        let root_path = NodePath {
            path: vec![(root_hash, true)],
        };
        let layer1: Vec<NodePath> = map_node_to_next_layer(root_path, root)?;
        let mut layer1_nodes = vec![]; // 16 x
        for path in layer1.clone().into_iter() {

            let (hash, _direct) =  *path.path.last().unwrap();
            let node = self.get_raw_bytes(hash).await?.node;

            verify_hash(&node, hash)?;
            layer1_nodes.push(node);
            
        }

        let layer2: anyhow::Result<Vec<Vec<NodePath>>> = layer1.clone().into_iter().enumerate().map(|(index, path)| { 
            map_node_to_next_layer(path, layer1_nodes[index].clone())

        }).collect();
        let layer2: Vec<NodePath> = layer2?.into_iter().flat_map(|vec| vec).collect();
        Ok(Layers {
            layer0: root_hash,
            layer1,
            layer2,
        })


    }
    pub async fn bootstrap_state(&mut self, layers: Layers) -> anyhow::Result<()> {
        let (db_handle, collection) = Self::db_handles(&self.storage);
        for path in layers.layer2 {
            self.get_full_subtree(path).await?;
            
        }

        // let root_guard = RootGuard::new(&collection.database, patch.target_root, account_extractor as ChildExtractorFn);
        // ERROR
        Ok(())
    }
}

type ChildExtractorFn = fn(&[u8]) -> Vec<H256>;

fn verify_hash(value: &[u8], hash: H256) -> anyhow::Result<()> {
    let actual_hash = H256::from_slice(Keccak256::digest(value).as_slice());
    if hash != actual_hash {
        return Err(anyhow::anyhow!("hash mismatch {:?} {:?}", hash, actual_hash))?;
    }
    Ok(())
}

pub fn no_childs(_: &[u8]) -> Vec<H256> {
    vec![]
}


fn map_node_to_next_layer(parent_path: NodePath, node: Vec<u8>) -> anyhow::Result<Vec<NodePath>>{
    let node = MerkleNode::decode(&Rlp::new(&node))?;

    let (_hash, direct) = *parent_path.path.last().unwrap();
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
        .map(|k| 

            {
                let mut p = parent_path.path.clone();
                p.push((k, true));
                NodePath {
                    path: p,
                }
            }
        )
        .chain(indirect_childs.into_iter().map(|k| {
                let mut p = parent_path.path.clone();
                p.push((k, false));
                NodePath {
                    path: p,
                }
            }))
        .collect();
    Ok(paths)

}
