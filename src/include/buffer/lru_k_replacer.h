//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.h
//
// Identification: src/include/buffer/lru_k_replacer.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <limits>
#include <list>
#include <mutex>  // NOLINT
#include <optional>
#include <unordered_map>
#include <vector>

#include "common/config.h"
#include "common/macros.h"

namespace bustub {

enum class AccessType { Unknown = 0, Lookup, Scan, Index };

class LRUKNode {
 public:
  explicit LRUKNode(size_t k, frame_id_t fid) : k_(k), fid_(fid) {}

  void RecordAccess(size_t timestamp) { history_.push_back(timestamp); }

  auto GetBackwardKDistance(size_t current_timestamp) const -> size_t {
    if (history_.size() < k_) {  // less than k access, return inf
      return std::numeric_limits<size_t>::max();
    }

    auto it = history_.rbegin();
    for (size_t i = 1; i < k_; i++) {
      ++it;
    }
    return current_timestamp - *it;
  }

  auto GetEarliestTimestamp() const -> size_t {
    return history_.empty() ? std::numeric_limits<size_t>::max() : history_.front();
  }

  void SetEvictable(bool set_evictable) { is_evictable_ = set_evictable; }
  auto IsEvictable() const -> bool { return is_evictable_; }
  auto GetFrameId() const -> frame_id_t { return fid_; }
  auto GetHistory() const -> const std::list<size_t> & { return history_; }
  void ClearHistory() { history_.clear(); }

 private:
  /** History of last seen K timestamps of this page. Least recent timestamp stored in front. */
  std::list<size_t> history_;
  size_t k_;
  frame_id_t fid_;
  bool is_evictable_{false};
};

/**
 * LRUKReplacer implements the LRU-k replacement policy.
 *
 * The LRU-k algorithm evicts a frame whose backward k-distance is maximum
 * of all frames. Backward k-distance is computed as the difference in time between
 * current timestamp and the timestamp of kth previous access.
 *
 * A frame with less than k historical references is given
 * +inf as its backward k-distance. When multiple frames have +inf backward k-distance,
 * classical LRU algorithm is used to choose victim.
 */
class LRUKReplacer {
 public:
  explicit LRUKReplacer(size_t num_frames, size_t k);

  DISALLOW_COPY_AND_MOVE(LRUKReplacer);

  /**
   * @brief Destroys the LRUReplacer.
   */
  ~LRUKReplacer() = default;

  auto Evict() -> std::optional<frame_id_t>;

  void RecordAccess(frame_id_t frame_id, AccessType access_type = AccessType::Unknown);

  void SetEvictable(frame_id_t frame_id, bool set_evictable);

  void Remove(frame_id_t frame_id);

  auto Size() -> size_t;

 private:
  std::unordered_map<frame_id_t, LRUKNode> node_store_;
  size_t current_timestamp_{0};
  size_t curr_size_{0};
  size_t replacer_size_;
  size_t k_;
  std::mutex latch_;
};

}  // namespace bustub
