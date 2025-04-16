use {
    super::{
        immutable_deserialized_packet::ImmutableDeserializedPacket,
        latest_unprocessed_votes::{
            LatestUnprocessedVotes, LatestValidatorVotePacket, VoteBatchInsertionMetrics,
            VoteSource,
        },
    },
    solana_runtime::bank::Bank,
    std::sync::Arc,
};

/// Maximum number of votes a single receive call will accept
const MAX_NUM_VOTES_RECEIVE: usize = 10_000;

#[derive(Debug)]
pub struct VoteStorage {
    latest_unprocessed_votes: Arc<LatestUnprocessedVotes>,
    vote_source: VoteSource,
}

impl VoteStorage {
    pub fn new(
        latest_unprocessed_votes: Arc<LatestUnprocessedVotes>,
        vote_source: VoteSource,
    ) -> Self {
        Self {
            latest_unprocessed_votes,
            vote_source,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.latest_unprocessed_votes.is_empty()
    }

    pub fn len(&self) -> usize {
        self.latest_unprocessed_votes.len()
    }

    pub fn max_receive_size(&self) -> usize {
        MAX_NUM_VOTES_RECEIVE
    }

    pub(crate) fn insert_batch(
        &mut self,
        deserialized_packets: Vec<ImmutableDeserializedPacket>,
    ) -> VoteBatchInsertionMetrics {
        self.latest_unprocessed_votes.insert_batch(
            deserialized_packets
                .into_iter()
                .filter_map(|deserialized_packet| {
                    LatestValidatorVotePacket::new_from_immutable(
                        Arc::new(deserialized_packet),
                        self.vote_source,
                        self.latest_unprocessed_votes
                            .should_deprecate_legacy_vote_ixs(),
                    )
                    .ok()
                }),
            false, // should_replenish_taken_votes
        )
    }

    // Re-insert re-tryable packets.
    pub(crate) fn reinsert_packets(
        &mut self,
        packets: impl Iterator<Item = Arc<ImmutableDeserializedPacket>>,
    ) {
        self.latest_unprocessed_votes.insert_batch(
            packets.filter_map(|packet| {
                LatestValidatorVotePacket::new_from_immutable(
                    packet,
                    self.vote_source,
                    self.latest_unprocessed_votes
                        .should_deprecate_legacy_vote_ixs(),
                )
                .ok()
            }),
            true, // should_replenish_taken_votes
        );
    }

    pub fn drain_unprocessed(&self, bank: &Bank) -> Vec<Arc<ImmutableDeserializedPacket>> {
        self.latest_unprocessed_votes.drain_unprocessed(bank)
    }

    pub fn clear(&mut self) {
        self.latest_unprocessed_votes.clear();
    }

    pub fn cache_epoch_boundary_info(&mut self, bank: &Bank) {
        if matches!(self.vote_source, VoteSource::Gossip) {
            panic!("Gossip vote thread should not be checking epoch boundary");
        }
        self.latest_unprocessed_votes
            .cache_epoch_boundary_info(bank);
    }

    pub fn should_not_process(&self) -> bool {
        // The gossip vote thread does not need to process or forward any votes, that is
        // handled by the tpu vote thread
        matches!(self.vote_source, VoteSource::Gossip)
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        solana_perf::packet::{BytesPacket, PacketFlags},
        solana_runtime::genesis_utils,
        solana_sdk::{
            hash::Hash,
            signature::{Keypair, Signer},
        },
        solana_vote::vote_transaction::new_tower_sync_transaction,
        solana_vote_program::vote_state::TowerSync,
        std::error::Error,
    };

    #[test]
    fn test_reinsert_packets() -> Result<(), Box<dyn Error>> {
        let node_keypair = Keypair::new();
        let genesis_config =
            genesis_utils::create_genesis_config_with_leader(100, &node_keypair.pubkey(), 200)
                .genesis_config;
        let (bank, _bank_forks) = Bank::new_with_bank_forks_for_tests(&genesis_config);
        let vote_keypair = Keypair::new();
        let mut vote = BytesPacket::from_data(
            None,
            new_tower_sync_transaction(
                TowerSync::default(),
                Hash::new_unique(),
                &node_keypair,
                &vote_keypair,
                &vote_keypair,
                None,
            ),
        )?;
        vote.meta_mut().flags.set(PacketFlags::SIMPLE_VOTE_TX, true);

        let latest_unprocessed_votes =
            LatestUnprocessedVotes::new_for_tests(&[vote_keypair.pubkey()]);
        let mut transaction_storage =
            VoteStorage::new(Arc::new(latest_unprocessed_votes), VoteSource::Tpu);

        transaction_storage.insert_batch(vec![ImmutableDeserializedPacket::new(vote.as_ref())?]);
        assert_eq!(1, transaction_storage.len());

        // Drain all packets, then re-insert.
        let packets = transaction_storage.drain_unprocessed(&bank);
        transaction_storage.reinsert_packets(packets.into_iter());

        // All packets should remain in the transaction storage
        assert_eq!(1, transaction_storage.len());
        Ok(())
    }
}
