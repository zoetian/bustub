//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hyperloglog_presto.cpp
//
// Identification: src/primer/hyperloglog_presto.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//
// The key difference in Presto's design is that now we split the bits after leading bits
// into 2 buckets to save memory. [overflow_bucket 3 bits][dense_bucket 4 bits].
// When leftover bits < 15 (2^4-1), we store it into dense_bucket;
// O.W. re-cal the bucket_idx for overflow_bucket and store the (val - 15) into it.
// The calculation for prev. register_idx remains the same, using n_leading_bits
//
// Hash x: 11100000000000000 (many zeros), n_leading_bits = 2
//         ↑↑|.............|
//         11|.............| → bucket_idx = 3 (11 in binary)
//                          → trailing_zeros = 16
//
// Since 16 > 15:
// - dense_bucket_[3] = 1111 (15 in binary)
// - overflow_bucket_[3] = 001 (16 - 15 = 1 in binary)
//===----------------------------------------------------------------------===//

#include "primer/hyperloglog_presto.h"
#include <_types/_uint64_t.h>
#include "primer/hyperloglog.h"

namespace bustub {

/** @brief Parameterized constructor. */
template <typename KeyType>
HyperLogLogPresto<KeyType>::HyperLogLogPresto(int16_t n_leading_bits) : cardinality_(0) {
  n_leading_bits_ = n_leading_bits > 0 ? n_leading_bits : 0;
  dense_bucket_.resize(1UL << n_leading_bits_);
}

/** @brief Element is added for HLL calculation. */
template <typename KeyType>
auto HyperLogLogPresto<KeyType>::AddElem(KeyType val) -> void {
  auto hash = CalculateHash(val);
  auto binary = std::bitset<BITSET_CAPACITY>(hash);

  uint64_t bucket_idx = hash >> (BITSET_CAPACITY - n_leading_bits_);

  uint64_t trailing_zeros = 1;
  uint64_t mask = 1ULL << (BITSET_CAPACITY - 1 - n_leading_bits_);
  while ((mask > 0) && ((hash & mask) == 0)) {
    trailing_zeros++;
    mask >>= 1;
  }

  if (trailing_zeros < 15) {
    auto curr_val = dense_bucket_[bucket_idx].to_ulong();
    if (trailing_zeros > curr_val) {
      dense_bucket_[bucket_idx] = std::bitset<DENSE_BUCKET_SIZE>(trailing_zeros);
    }
  } else {
    auto overflow_val = trailing_zeros - 15;

    if (overflow_bucket_.find(bucket_idx) == overflow_bucket_.end()) {
      overflow_bucket_[bucket_idx] = std::bitset<OVERFLOW_BUCKET_SIZE>(overflow_val);
    } else {
      auto curr_overflow_val = overflow_bucket_[bucket_idx].to_ulong();
      if (curr_overflow_val < overflow_val) {
        overflow_bucket_[bucket_idx] = std::bitset<OVERFLOW_BUCKET_SIZE>(overflow_val);
      }
    }
  }
}
 
/** @brief Function to compute cardinality. */
template <typename T>
auto HyperLogLogPresto<T>::ComputeCardinality() -> void {
  double sum = 0.0;
  uint64_t m = (1UL << n_leading_bits_);  // size of registers

  for (uint64_t i = 0; i < m; i++) {
    uint64_t value = dense_bucket_[i].to_ulong();
    value += overflow_bucket_[i].to_ulong();
    sum += std::pow(2, -value);
  }
  if (sum == 0.0) {
    cardinality_ = 0;
    return;
  }
  cardinality_ = std::floor(CONSTANT * m * m / sum);
}

template class HyperLogLogPresto<int64_t>;
template class HyperLogLogPresto<std::string>;
}  // namespace bustub