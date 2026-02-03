// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use arrow::array::ArrayRef;
use arrow::record_batch::RecordBatch;
use arrow::row::{OwnedRow, RowConverter};
use datafusion::common::Result;
use datafusion::physical_expr::{LexOrdering, PhysicalExpr};
use datafusion_comet_spark_expr::murmur3::create_murmur3_hashes;
use std::sync::Arc;

#[derive(Default)]
pub(super) struct ScratchSpace {
    /// Hashes for each row in the current batch.
    hashes_buf: Vec<u32>,
    /// Partition ids for each row in the current batch.
    partition_ids: Vec<u32>,
    /// The row indices of the rows in each partition. This array is conceptually divided into
    /// partitions, where each partition contains the row indices of the rows in that partition.
    /// The length of this array is the same as the number of rows in the batch.
    partition_row_indices: Vec<u32>,
    /// The start indices of partitions in partition_row_indices. partition_starts[K] and
    /// partition_starts[K + 1] are the start and end indices of partition K in partition_row_indices.
    /// The length of this array is 1 + the number of partitions.
    partition_starts: Vec<u32>,
}

pub struct PartitionInfo<'a> {
    pub partition_starts: &'a Vec<u32>,
    pub partition_row_indices: &'a Vec<u32>,
}

#[derive(Debug, Clone)]
pub enum CometPartitioning {
    SinglePartition,
    /// Allocate rows based on a hash of one of more expressions and the specified number of
    /// partitions. Args are 1) the expression to hash on, and 2) the number of partitions.
    Hash(Vec<Arc<dyn PhysicalExpr>>, usize),
    /// Allocate rows based on the lexical order of one of more expressions and the specified number of
    /// partitions. Args are 1) the LexOrdering to use to compare values and split into partitions,
    /// 2) the number of partitions, 3) the RowConverter used to view incoming RecordBatches as Arrow
    /// Rows for comparing to 4) OwnedRows that represent the boundaries of each partition, used with
    /// LexOrdering to bin each value in the RecordBatch to a partition.
    RangePartitioning(LexOrdering, usize, Arc<RowConverter>, Vec<OwnedRow>),
    /// Round robin partitioning. Distributes rows across partitions by sorting them by hash
    /// (computed from columns) and then assigning partitions sequentially. Args are:
    /// 1) number of partitions, 2) max columns to hash (0 means no limit).
    RoundRobin(usize, usize),
}

impl CometPartitioning {
    pub(super) fn partition_count(&self) -> usize {
        use CometPartitioning::*;
        match self {
            SinglePartition => 1,
            Hash(_, n) | RangePartitioning(_, n, _, _) | RoundRobin(n, _) => *n,
        }
    }

    /// Returns whether this partitioning type requires a hashes buffer in scratch space
    pub(super) fn requires_hashes_buffer(&self) -> bool {
        matches!(
            self,
            CometPartitioning::Hash(_, _) | CometPartitioning::RoundRobin(_, _)
        )
    }

    /// Initialize scratch space with appropriate sizes for this partitioning scheme
    pub(super) fn init_scratch_space(&self, batch_size: usize) -> ScratchSpace {
        ScratchSpace {
            // Allocate hashes_buf for hash and round robin partitioning.
            // Round robin hashes all columns to achieve even, deterministic distribution.
            hashes_buf: if self.requires_hashes_buffer() {
                vec![0; batch_size]
            } else {
                vec![]
            },
            partition_ids: vec![0; batch_size],
            partition_row_indices: vec![0; batch_size],
            partition_starts: vec![0; self.partition_count() + 1],
        }
    }

    /// Compute partition IDs for each row in the batch
    /// Returns the partition_ids array and optionally the hashes buffer if needed
    pub(super) fn compute_partition_ids(
        &self,
        batch: &RecordBatch,
        scratch: &mut ScratchSpace,
    ) -> Result<()> {
        let num_rows = batch.num_rows();
        let partition_ids = &mut scratch.partition_ids[..num_rows];

        match self {
            CometPartitioning::Hash(exprs, num_output_partitions) => {
                // Evaluate partition expressions to get rows to apply partitioning scheme.
                let arrays = exprs
                    .iter()
                    .map(|expr| expr.evaluate(batch)?.into_array(batch.num_rows()))
                    .collect::<Result<Vec<_>>>()?;

                // Use identical seed as Spark hash partitioning.
                let hashes_buf = &mut scratch.hashes_buf[..num_rows];
                hashes_buf.fill(42_u32);

                // Hash arrays and compute partition ids based on number of partitions.
                create_murmur3_hashes(&arrays, hashes_buf)?
                    .iter()
                    .enumerate()
                    .for_each(|(idx, hash)| {
                        partition_ids[idx] = pmod(*hash, *num_output_partitions) as u32;
                    });
            }
            CometPartitioning::RangePartitioning(lex_ordering, _, row_converter, bounds) => {
                // Evaluate partition expressions for values to apply partitioning scheme on.
                let arrays = lex_ordering
                    .iter()
                    .map(|expr| expr.expr.evaluate(batch)?.into_array(batch.num_rows()))
                    .collect::<Result<Vec<_>>>()?;

                // Generate partition ids for every row, first by converting the partition
                // arrays to Rows, and then doing binary search for each Row against the
                // bounds Rows.
                let row_batch = row_converter.convert_columns(arrays.as_slice())?;
                row_batch.iter().enumerate().for_each(|(row_idx, row)| {
                    partition_ids[row_idx] = bounds
                        .as_slice()
                        .partition_point(|bound| bound.row() <= row)
                        as u32
                });
            }
            // Comet implements "round robin" as hash partitioning on columns.
            // This achieves the same goal as Spark's round robin (even distribution
            // without semantic grouping) while being deterministic for fault tolerance.
            //
            // Note: This produces different partition assignments than Spark's round robin,
            // which sorts by UnsafeRow binary representation before assigning partitions.
            // However, both approaches provide even distribution and determinism.
            CometPartitioning::RoundRobin(num_output_partitions, max_hash_columns) => {
                // Collect columns for hashing, respecting max_hash_columns limit
                // max_hash_columns of 0 means no limit (hash all columns)
                // Negative values are normalized to 0 in the planner
                let num_columns_to_hash = if *max_hash_columns == 0 {
                    batch.num_columns()
                } else {
                    (*max_hash_columns).min(batch.num_columns())
                };
                let columns_to_hash: Vec<ArrayRef> = (0..num_columns_to_hash)
                    .map(|i| Arc::clone(batch.column(i)))
                    .collect();

                // Use identical seed as Spark hash partitioning.
                let hashes_buf = &mut scratch.hashes_buf[..num_rows];
                hashes_buf.fill(42_u32);
                create_murmur3_hashes(&columns_to_hash, hashes_buf)?;

                // Compute hash for selected columns
                hashes_buf.iter().enumerate().for_each(|(idx, hash)| {
                    // Assign partition IDs based on hash (same as hash partitioning)
                    partition_ids[idx] = pmod(*hash, *num_output_partitions) as u32;
                });
            }
            CometPartitioning::SinglePartition => {
                // All rows go to partition 0
                partition_ids.fill(0);
            }
        }

        Ok(())
    }

    fn map_partition_ids_to_starts_and_indices(
        scratch: &mut ScratchSpace,
        num_output_partitions: usize,
        num_rows: usize,
    ) {
        let partition_ids = &mut scratch.partition_ids[..num_rows];

        // count each partition size, while leaving the last extra element as 0
        let partition_counters = &mut scratch.partition_starts;
        partition_counters.resize(num_output_partitions + 1, 0);
        partition_counters.fill(0);
        partition_ids
            .iter()
            .for_each(|partition_id| partition_counters[*partition_id as usize] += 1);

        // accumulate partition counters into partition ends
        // e.g. partition counter: [1, 3, 2, 1, 0] => [1, 4, 6, 7, 7]
        let partition_ends = partition_counters;
        let mut accum = 0;
        partition_ends.iter_mut().for_each(|v| {
            *v += accum;
            accum = *v;
        });

        // calculate partition row indices and partition starts
        // e.g. partition ids: [3, 1, 1, 1, 2, 2, 0] will produce the following partition_row_indices
        // and partition_starts arrays:
        //
        //  partition_row_indices: [6, 1, 2, 3, 4, 5, 0]
        //  partition_starts: [0, 1, 4, 6, 7]
        //
        // partition_starts conceptually splits partition_row_indices into smaller slices.
        // Each slice partition_row_indices[partition_starts[K]..partition_starts[K + 1]] contains the
        // row indices of the input batch that are partitioned into partition K. For example,
        // first partition 0 has one row index [6], partition 1 has row indices [1, 2, 3], etc.
        let partition_row_indices = &mut scratch.partition_row_indices;
        partition_row_indices.resize(num_rows, 0);
        for (index, partition_id) in partition_ids.iter().enumerate().rev() {
            partition_ends[*partition_id as usize] -= 1;
            let end = partition_ends[*partition_id as usize];
            partition_row_indices[end as usize] = index as u32;
        }

        // after calculating, partition ends become partition starts
    }

    /// Partition a batch and return partition starts and row indices
    /// This is the complete pipeline: compute partition IDs -> map to starts/indices
    pub(super) fn partition_batch<'a>(
        &self,
        batch: &RecordBatch,
        scratch: &'a mut ScratchSpace,
    ) -> Result<PartitionInfo<'a>> {
        let num_rows = batch.num_rows();

        // Generate partition ids for every row.
        self.compute_partition_ids(batch, scratch)?;

        // We now have partition ids for every input row, map that to partition starts
        // and partition indices to eventually right these rows to partition buffers.
        Self::map_partition_ids_to_starts_and_indices(scratch, self.partition_count(), num_rows);

        Ok(PartitionInfo {
            partition_starts: &scratch.partition_starts,
            partition_row_indices: &scratch.partition_row_indices,
        })
    }
}

fn pmod(hash: u32, n: usize) -> usize {
    let hash = hash as i32;
    let n = n as i32;
    let r = hash % n;
    let result = if r < 0 { (r + n) % n } else { r };
    result as usize
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pmod() {
        let i: Vec<u32> = vec![0x99f0149d, 0x9c67b85d, 0xc8008529, 0xa05b5d7b, 0xcd1e64fb];
        let result = i.into_iter().map(|i| pmod(i, 200)).collect::<Vec<usize>>();

        // expected partition from Spark with n=200
        let expected = vec![69, 5, 193, 171, 115];
        assert_eq!(result, expected);
    }
}
