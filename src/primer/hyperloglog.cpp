//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hyperloglog.cpp
//
// Identification: src/primer/hyperloglog.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "include/primer/hyperloglog.h"
#include <bitset>
#include "type/limits.h"

namespace bustub {

/** @brief Parameterized constructor. */
template <typename KeyType>
HyperLogLog<KeyType>::HyperLogLog(int16_t n_bits) : cardinality_(0) {
  n_bits_ = n_bits > 0 ? n_bits : 0;
  registers_.resize(1UL << n_bits_);
}

/**
 * @brief Function that computes binary.
 *
 * @param[in] hash
 * @returns binary of a given hash
 */
template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeBinary(const hash_t &hash) const -> std::bitset<BITSET_CAPACITY> {
  return {hash};
}

/**
 * @brief Function that computes leading zeros.
 *
 * @param[in] bset - binary values of a given bitset
 * @returns leading zeros of given binary set
 */
// count non-zero index from b_bits_
// i.e. 110|00001010....
template <typename KeyType>
auto HyperLogLog<KeyType>::PositionOfLeftmostOne(const std::bitset<BITSET_CAPACITY> &bset) const -> uint64_t {
  for (int i = n_bits_; i <= BITSET_CAPACITY; i++) {
    if (bset[BITSET_CAPACITY - 1 - i]) {
      return i - n_bits_ + 1;
    }
  }
  return BITSET_CAPACITY - n_bits_;
}

/**
 * @brief Adds a value into the HyperLogLog.
 *
 * @param[in] val - value that's added into hyperloglog
 */
template <typename KeyType>
auto HyperLogLog<KeyType>::AddElem(KeyType val) -> void {
  auto hash = CalculateHash(val);
  auto binary = ComputeBinary(hash);
  std::bitset<BITSET_CAPACITY> shifted_bin =
      binary >> (BITSET_CAPACITY - n_bits_);  // shifted to get first n_bits binary
  uint64_t idx = shifted_bin.to_ullong();     // convert the first n_bits from binary to decimal

  uint64_t p = PositionOfLeftmostOne(binary);
  registers_[idx] = std::max(registers_[idx], p);
}

/**
 * @brief Function that computes cardinality.
 */
template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeCardinality() -> void {
  double sum = 0.0;
  uint64_t m = (1 << n_bits_);  // number of registers
  for (uint64_t i = 0; i < m; i++) {
    sum += std::pow(2, -static_cast<int64_t>(registers_[i]));
  }

  cardinality_ = std::floor(CONSTANT * m * m / sum);
}

// KeyType is a placeholder, it can be these below:
template class HyperLogLog<int64_t>;
template class HyperLogLog<std::string>;

}  // namespace bustub
