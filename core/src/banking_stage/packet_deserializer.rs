//! Deserializes packets from sigverify stage. Owned by banking stage.

use {
    super::{
        immutable_deserialized_packet::{DeserializedPacketError, ImmutableDeserializedPacket},
        packet_filter::PacketFilterFailure,
    },
    agave_banking_stage_ingress_types::{BankingPacketBatch, BankingPacketReceiver},
    crossbeam_channel::RecvTimeoutError,
    solana_perf::packet::PacketBatch,
    solana_sdk::saturating_add_assign,
    std::time::{Duration, Instant},
};

/// Results from deserializing packet batches.
pub struct ReceivePacketResults {
    /// Deserialized packets from all received packet batches
    pub deserialized_packets: Vec<ImmutableDeserializedPacket>,
    /// Counts of packets received and errors recorded during deserialization
    /// and filtering
    pub packet_stats: PacketReceiverStats,
}

pub struct PacketDeserializer {
    /// Receiver for packet batches from sigverify stage
    packet_batch_receiver: BankingPacketReceiver,
}

#[derive(Default, Debug, PartialEq)]
pub struct PacketReceiverStats {
    /// Number of packets passing sigverify
    pub passed_sigverify_count: u64,
    /// Number of packets failing sigverify
    pub failed_sigverify_count: u64,
    /// Number of packets dropped due to sanitization error
    pub failed_sanitization_count: u64,
    /// Number of packets dropped due to prioritization error
    pub failed_prioritization_count: u64,
    /// Number of vote packets dropped
    pub invalid_vote_count: u64,
    /// Number of packets dropped due to excessive precompiles
    pub excessive_precompile_count: u64,
    /// Number of packets dropped due to insufficient compute limit
    pub insufficient_compute_limit_count: u64,
}

impl PacketReceiverStats {
    pub fn increment_error_count(&mut self, err: &DeserializedPacketError) {
        match err {
            DeserializedPacketError::ShortVecError(..)
            | DeserializedPacketError::DeserializationError(..)
            | DeserializedPacketError::SignatureOverflowed(..)
            | DeserializedPacketError::SanitizeError(..) => {
                saturating_add_assign!(self.failed_sanitization_count, 1);
            }
            DeserializedPacketError::PrioritizationFailure => {
                saturating_add_assign!(self.failed_prioritization_count, 1);
            }
            DeserializedPacketError::VoteTransactionError => {
                saturating_add_assign!(self.invalid_vote_count, 1);
            }
            DeserializedPacketError::FailedFilter(PacketFilterFailure::ExcessivePrecompiles) => {
                saturating_add_assign!(self.excessive_precompile_count, 1);
            }
            DeserializedPacketError::FailedFilter(
                PacketFilterFailure::InsufficientComputeLimit,
            ) => {
                saturating_add_assign!(self.insufficient_compute_limit_count, 1);
            }
        }
    }
}

impl PacketDeserializer {
    pub fn new(packet_batch_receiver: BankingPacketReceiver) -> Self {
        Self {
            packet_batch_receiver,
        }
    }

    /// Handles receiving packet batches from sigverify and returns a vector of deserialized packets
    pub fn receive_packets(
        &self,
        recv_timeout: Duration,
        capacity: usize,
        packet_filter: impl Fn(
            ImmutableDeserializedPacket,
        ) -> Result<ImmutableDeserializedPacket, PacketFilterFailure>,
    ) -> Result<ReceivePacketResults, RecvTimeoutError> {
        let (packet_count, packet_batches) = self.receive_until(recv_timeout, capacity)?;

        Ok(Self::deserialize_and_collect_packets(
            packet_count,
            &packet_batches,
            packet_filter,
        ))
    }

    /// Deserialize packet batches, aggregates tracer packet stats, and collect
    /// them into ReceivePacketResults
    fn deserialize_and_collect_packets(
        packet_count: usize,
        banking_batches: &[BankingPacketBatch],
        packet_filter: impl Fn(
            ImmutableDeserializedPacket,
        ) -> Result<ImmutableDeserializedPacket, PacketFilterFailure>,
    ) -> ReceivePacketResults {
        let mut packet_stats = PacketReceiverStats::default();
        let mut errors = 0_usize;
        let deserialized_packets: Vec<_> = banking_batches
            .iter()
            .flat_map(|banking_batch| banking_batch.iter())
            .flat_map(|batch| batch.iter())
            .filter(|pkt| !pkt.meta().discard())
            .filter_map(|pkt| {
                match ImmutableDeserializedPacket::new(pkt)
                    .and_then(|pkt| packet_filter(pkt).map_err(Into::into))
                {
                    Ok(pkt) => Some(pkt),
                    Err(err) => {
                        saturating_add_assign!(errors, 1);
                        packet_stats.increment_error_count(&err);
                        None
                    }
                }
            })
            .collect();
        saturating_add_assign!(
            packet_stats.passed_sigverify_count,
            deserialized_packets.len().saturating_add(errors) as u64
        );
        saturating_add_assign!(
            packet_stats.failed_sigverify_count,
            packet_count
                .saturating_sub(deserialized_packets.len())
                .saturating_sub(errors) as u64
        );

        ReceivePacketResults {
            deserialized_packets,
            packet_stats,
        }
    }

    /// Receives packet batches from sigverify stage with a timeout
    fn receive_until(
        &self,
        recv_timeout: Duration,
        packet_count_upperbound: usize,
    ) -> Result<(usize, Vec<BankingPacketBatch>), RecvTimeoutError> {
        let start = Instant::now();

        let packet_batches = self.packet_batch_receiver.recv_timeout(recv_timeout)?;
        let mut num_packets_received = packet_batches
            .iter()
            .map(|batch| batch.len())
            .sum::<usize>();
        let mut messages = vec![packet_batches];

        while let Ok(packet_batches) = self.packet_batch_receiver.try_recv() {
            trace!("got more packet batches in packet deserializer");
            num_packets_received += packet_batches
                .iter()
                .map(|batch| batch.len())
                .sum::<usize>();
            messages.push(packet_batches);

            if start.elapsed() >= recv_timeout || num_packets_received >= packet_count_upperbound {
                break;
            }
        }

        Ok((num_packets_received, messages))
    }

    pub(crate) fn deserialize_packets_with_indexes(
        packet_batch: &PacketBatch,
    ) -> impl Iterator<Item = (ImmutableDeserializedPacket, usize)> + '_ {
        packet_batch.iter().enumerate().filter_map(|(index, pkt)| {
            if !pkt.meta().discard() {
                let pkt = ImmutableDeserializedPacket::new(pkt).ok()?;
                Some((pkt, index))
            } else {
                None
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        solana_perf::packet::to_packet_batches,
        solana_sdk::{
            hash::Hash, pubkey::Pubkey, signature::Keypair, system_transaction,
            transaction::Transaction,
        },
    };

    fn random_transfer() -> Transaction {
        system_transaction::transfer(&Keypair::new(), &Pubkey::new_unique(), 1, Hash::default())
    }

    #[test]
    fn test_deserialize_and_collect_packets_empty() {
        let results = PacketDeserializer::deserialize_and_collect_packets(0, &[], Ok);
        assert_eq!(results.deserialized_packets.len(), 0);
        assert_eq!(results.packet_stats.passed_sigverify_count, 0);
        assert_eq!(results.packet_stats.failed_sigverify_count, 0);
    }

    #[test]
    fn test_deserialize_and_collect_packets_simple_batches() {
        let transactions = vec![random_transfer(), random_transfer()];
        let packet_batches = to_packet_batches(&transactions, 1);
        assert_eq!(packet_batches.len(), 2);

        let packet_count: usize = packet_batches.iter().map(|x| x.len()).sum();
        let results = PacketDeserializer::deserialize_and_collect_packets(
            packet_count,
            &[BankingPacketBatch::new(packet_batches)],
            Ok,
        );
        assert_eq!(results.deserialized_packets.len(), 2);
        assert_eq!(results.packet_stats.passed_sigverify_count, 2);
        assert_eq!(results.packet_stats.failed_sigverify_count, 0);
    }

    #[test]
    fn test_deserialize_and_collect_packets_simple_batches_with_failure() {
        let transactions = vec![random_transfer(), random_transfer()];
        let mut packet_batches = to_packet_batches(&transactions, 1);
        assert_eq!(packet_batches.len(), 2);
        packet_batches[0]
            .first_mut()
            .unwrap()
            .meta_mut()
            .set_discard(true);

        let packet_count: usize = packet_batches.iter().map(|x| x.len()).sum();
        let results = PacketDeserializer::deserialize_and_collect_packets(
            packet_count,
            &[BankingPacketBatch::new(packet_batches)],
            Ok,
        );
        assert_eq!(results.deserialized_packets.len(), 1);
        assert_eq!(results.packet_stats.passed_sigverify_count, 1);
        assert_eq!(results.packet_stats.failed_sigverify_count, 1);
    }
}
