use evm_rpc::FormatHex;
use evm_state::storage::account_extractor;
use evm_state::H256;

use std::time::Instant;

use crate::triedb::{debug_elapsed, lock_root, NodePath};

use self::app_grpc::backend_client::BackendClient;

pub mod app_grpc {
    tonic::include_proto!("triedb_repl");
}

type RocksHandleA<'a> = super::RocksHandleA<'a>;

type ChildExtractorFn = fn(&[u8]) -> Vec<H256>;

fn parse_diff_response(
    in_: app_grpc::GetStateDiffReply,
) -> Result<Vec<triedb::DiffChange>, tonic::Status> {
    in_.changeset
        .into_iter()
        .map(|insert| {
            let hash = insert
                .hash
                .ok_or_else(|| tonic::Status::invalid_argument("insert with empty hash"))?;
            match FormatHex::from_hex(&hash.value) {
                Ok(hash) => Ok(triedb::DiffChange::Insert(hash, insert.data.into())),
                Err(e) => Err(tonic::Status::invalid_argument(format!(
                    "could not parse hash {:?}",
                    e
                ))),
            }
        })
        .collect()
}
impl tonic::IntoRequest<app_grpc::FullSubtreePath> for NodePath {
    fn into_request(self) -> tonic::Request<app_grpc::FullSubtreePath> {
        let vecy: Vec<_> = self
            .path
            .into_iter()
            .map(|element| app_grpc::PathElement {
                hash: Some(app_grpc::Hash {
                    value: element.0.format_hex(),
                }),
                direct: element.1,
            })
            .collect();
        tonic::Request::new(app_grpc::FullSubtreePath { paths: vecy })
    }
}
impl<S> super::Client<S> {
    pub async fn ping(&mut self) -> Result<(), tonic::Status> {
        let request = tonic::Request::new(());
        let response = self.client.ping(request).await?;
        log::trace!("PING | RESPONSE={:?}", response);
        Ok(())
    }

    pub async fn get_block_range(&mut self) -> Result<app_grpc::GetBlockRangeReply, tonic::Status> {
        let request = tonic::Request::new(());
        let response = self.client.get_block_range(request).await?;

        let response = response.into_inner();
        log::info!(
            "block_range received {} -> {}",
            response.start,
            response.end,
        );
        Ok(response)
    }

    pub async fn get_full_subtree(
        &mut self,
        path_elements: NodePath,
    ) -> Result<app_grpc::GetStateDiffReply, tonic::Status> {
        let response = self.client.get_full_subtree(path_elements).await?;

        let response = response.into_inner();

        log::debug!(
            "changeset received {:?} -> {:?}, {}",
            response.first_root,
            response.second_root,
            response.changeset.len()
        );
        Ok(response)
    }
    pub async fn get_raw_bytes(
        &mut self,
        hash: H256,
    ) -> Result<app_grpc::GetRawBytesReply, tonic::Status> {
        let request = tonic::Request::new(app_grpc::GetRawBytesRequest {
            hash: Some(app_grpc::Hash {
                value: hash.format_hex(),
            }),
        });
        let response = self.client.get_raw_bytes(request).await?;
        log::trace!("PING | RESPONSE={:?}", response);
        Ok(response.into_inner())
    }

    fn state_diff_request(
        heights: (evm_state::BlockNum, evm_state::BlockNum),
    ) -> tonic::Request<app_grpc::GetStateDiffRequest> {
        tonic::Request::new(app_grpc::GetStateDiffRequest {
            from: heights.0,
            to: heights.1,
        })
    }

    fn check_hash(
        height: evm_state::BlockNum,
        actual: Option<app_grpc::Hash>,
        expected: H256,
    ) -> Result<(), anyhow::Error> {
        if actual.is_none() {
            return Err(anyhow::anyhow!(
                "expected `from` for height {}: {:?}, received over network {:?}",
                height,
                expected,
                "None",
            ));
        }
        let actual = actual.unwrap();

        let actual: H256 = FormatHex::from_hex(&actual.value)?;

        if actual != expected {
            return Err(anyhow::anyhow!(
                "expected `from` for height {}: {:?}, received over network {:?}",
                height,
                expected,
                actual,
            ));
        }
        Ok(())
    }

    pub async fn download_and_apply_diff<'a, 'b>(
        client: &mut BackendClient<tonic::transport::Channel>,
        db_handle: &RocksHandleA<'a>,
        collection: &'b triedb::gc::TrieCollection<RocksHandleA<'b>>,
        heights: (evm_state::BlockNum, evm_state::BlockNum),
        expected_hashes: (H256, H256),
    ) -> Result<triedb::gc::RootGuard<'b, RocksHandleA<'b>, ChildExtractorFn>, anyhow::Error> {
        log::debug!("download_and_apply_diff start");
        let start = Instant::now();
        let _from_guard = lock_root(db_handle, expected_hashes.0, account_extractor)?;
        debug_elapsed("locked root", &start);

        let response = client
            .get_state_diff(Self::state_diff_request(heights))
            .await?;
        debug_elapsed("queried response over network", &start);

        let response = response.into_inner();
        log::debug!(
            "changeset received {:?} -> {:?}, {}",
            response.first_root,
            response.second_root,
            response.changeset.len()
        );
        Self::check_hash(heights.0, response.first_root.clone(), expected_hashes.0)?;
        Self::check_hash(heights.1, response.second_root.clone(), expected_hashes.1)?;

        let diff_changes = parse_diff_response(response)?;
        debug_elapsed("parsed response", &start);

        let diff_patch = triedb::verify_diff(
            db_handle,
            expected_hashes.1,
            diff_changes,
            account_extractor,
            false,
        )?;
        debug_elapsed("verified response", &start);

        let to_guard =
            collection.apply_diff_patch(diff_patch, account_extractor as ChildExtractorFn)?;
        debug_elapsed("applied response", &start);
        Ok(to_guard)
    }
}
