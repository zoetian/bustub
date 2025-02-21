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
// Presto's HyperLogLog implementation uses a split bucket design for storing trailing zeros:
// - dense_bucket_: 4 bits, stores values 0-15
// - overflow_bucket_: 3 bits, combines with dense_bucket_ for larger values
//
// The total value is: (overflow_bucket_ << 4) | dense_bucket_
// For example, if we need to store 25:
// - dense_bucket_ would store 9 (1001 in binary)
// - overflow_bucket_ would store 1 (001 in binary)
// - Combined: 001|1001 = 25 (1 shifted left 4 bits + 9)
//
// Example with hash value and n_leading_bits = 2:
// Hash: 11010000...
//       ↑↑|.......|
//       11|.......| → bucket_idx = 3 (11 in binary)
//             trailing_zeros = 5 (stored in dense_bucket_ only since < 16)
//
// For larger values (trailing_zeros ≥ 16):
// dense_bucket_ and overflow_bucket_ work together:
// - value = (overflow_bits << 4) | dense_bits
// - When updating, we compare the total combined value
//
// Example with hash value and n_leading_bits = 2:
// Hash: 11000000000000000000... (20 trailing zeros)
//       ↑↑|...................|
//       11|...................| → bucket_idx = 3 (11 in binary)
//                                trailing_zeros = 20
//
// Since trailing_zeros = 20 > 15:
// dense_bucket_[3] = 0100 (4 in binary)     // lower 4 bits of 20
// overflow_bucket_[3] = 001 (1 in binary)   // upper bits (20 >> 4 = 1)
// Combined value: (001 << 4) | 0100 = 20    // reconstructs original value
//                 ↑↑↑|↑↑↑↑
//           overflow |dense
//             bits   |bits
//
// debug line cmd:
// std::ofstream outputFile("/tmp/output.txt", std::ios::app);
// make -j$(nproc) hyperloglog_test && rm /tmp/output.txt && ./test/hyperloglog_test; cat /tmp/output.txt

//===----------------------------------------------------------------------===//

#include "primer/hyperloglog_presto.h"
#include <_types/_uint64_t.h>
#include <bitset>
#include <cmath>
#include <iostream>
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

  // guard on zero leading bits
  uint64_t bucket_idx = n_leading_bits_ == 0 ? 0 : hash >> (BITSET_CAPACITY - n_leading_bits_);

  uint64_t trailing_zeros = 0;
  uint64_t mask = 1ULL;

  auto shifted = (n_leading_bits_ == 0) ? 0 : (1ULL << (BITSET_CAPACITY - n_leading_bits_));
  auto shifted_hash = hash | shifted;
  while (mask > 0 && (shifted_hash & mask) == 0) {
    trailing_zeros++;
    mask <<= 1;
  }

  if (trailing_zeros < (1 << DENSE_BUCKET_SIZE)) {  // trailing < 15
    auto curr_val = dense_bucket_[bucket_idx].to_ulong();
    if (trailing_zeros > curr_val) {
      dense_bucket_[bucket_idx] = std::bitset<DENSE_BUCKET_SIZE>(trailing_zeros);
    }
  } else {  // trailing > 15
    auto total_binary =
        (overflow_bucket_[bucket_idx].to_ulong() << DENSE_BUCKET_SIZE) | dense_bucket_[bucket_idx].to_ulong();

    if (trailing_zeros > total_binary) {
      dense_bucket_[bucket_idx] = std::bitset<DENSE_BUCKET_SIZE>(trailing_zeros);
      overflow_bucket_[bucket_idx] = std::bitset<OVERFLOW_BUCKET_SIZE>(trailing_zeros >> DENSE_BUCKET_SIZE);
    }
  }
}

/** @brief Function to compute cardinality. */
template <typename T>
auto HyperLogLogPresto<T>::ComputeCardinality() -> void {
  double sum = 0.0;
  uint64_t m = (1UL << n_leading_bits_);  // size of registers

  for (uint64_t i = 0; i < m; i++) {
    int64_t value = dense_bucket_[i].to_ulong();
    value += overflow_bucket_[i].to_ulong() << DENSE_BUCKET_SIZE;  ///
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
