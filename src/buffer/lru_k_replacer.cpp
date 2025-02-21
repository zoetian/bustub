//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <limits>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <vector>
#include "common/config.h"
#include "common/exception.h"

namespace bustub {

/**
 * @brief a new LRUKReplacer.
 * @param num_frames the maximum number of frames the LRUReplacer will be required to store
 */
LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

/**
 * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
 * that are marked as 'evictable' are candidates for eviction.
 *
 * A frame with less than k historical references is given +inf as its backward k-distance.
 * If multiple frames have inf backward k-distance, then evict frame whose oldest timestamp
 * is furthest in the past.
 *
 * Successful eviction of a frame should decrement the size of replacer and remove the frame's
 * access history.
 *
 * @return true if a frame is evicted successfully, false if no frames can be evicted.
 */
auto LRUKReplacer::Evict() -> std::optional<frame_id_t> {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t victim_id = -1;
  size_t max_distance = 0;
  size_t earliest_timestamp = std::numeric_limits<size_t>::max();

  for (const auto &[fid, node] : node_store_) {
    if (!node.IsEvictable()) {
      continue;
    }

    size_t distance = node.GetBackwardKDistance(current_timestamp_);
    size_t timestamp = node.GetEarliestTimestamp();

    // update victim when: 1) curr node has > k-dist 2) both have inf k-dist but curr node has earlier timestamp
    if (distance > max_distance || (distance == max_distance && distance == std::numeric_limits<size_t>::max() &&
                                    timestamp < earliest_timestamp)) {
      victim_id = fid;
      max_distance = distance;
      earliest_timestamp = timestamp;
    }
  }

  if (victim_id != -1) {
    // Remove(victim_id);
    auto it = node_store_.find(victim_id);
    if (!it->second.IsEvictable()) {
      throw bustub::Exception("Attempting to remove a non-evictable frame");
    }
    curr_size_--;
    node_store_.erase(it);
    return victim_id;
  }

  return std::nullopt;
}

/**
 * @brief Record the event that the given frame id is accessed at current timestamp.
 * Create a new entry for access history if frame id has not been seen before.
 *
 * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
 * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
 *
 * @param frame_id id of frame that received a new access.
 * @param access_type type of access that was received. This parameter is only needed for
 * leaderboard tests.
 */
void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::scoped_lock<std::mutex> lock(latch_);

  BUSTUB_ASSERT(frame_id < static_cast<frame_id_t>(replacer_size_), "Invalid frame_id");

  auto [it, inserted] = node_store_.try_emplace(frame_id, k_, frame_id);
  it->second.RecordAccess(current_timestamp_);
  current_timestamp_++;
}

/**
 * @brief Toggle whether a frame is evictable or non-evictable. This function also
 * controls replacer's size. Note that size is equal to number of evictable entries.
 *
 * If a frame was previously evictable and is to be set to non-evictable, then size should
 * decrement. If a frame was previously non-evictable and is to be set to evictable,
 * then size should increment.
 *
 * If frame id is invalid, throw an exception or abort the process.
 *
 * For other scenarios, this function should terminate without modifying anything.
 *
 * @param frame_id id of frame whose 'evictable' status will be modified
 * @param set_evictable whether the given frame is evictable or not
 */
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);

  BUSTUB_ASSERT(frame_id < static_cast<frame_id_t>(replacer_size_), "Invalid frame_id");

  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    return;
  }

  if (it->second.IsEvictable() != set_evictable) {
    it->second.SetEvictable(set_evictable);
    curr_size_ += (set_evictable ? 1 : -1);
  }
}

/**
 * @brief Remove an evictable frame from replacer, along with its access history.
 * This function should also decrement replacer's size if removal is successful.
 *
 * Note that this is different from evicting a frame, which always remove the frame
 * with largest backward k-distance. This function removes specified frame id,
 * no matter what its backward k-distance is.
 *
 * If Remove is called on a non-evictable frame, throw an exception or abort the
 * process.
 *
 * If specified frame is not found, directly return from this function.
 *
 * @param frame_id id of frame to be removed
 */
void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);

  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    return;
  }

  if (!it->second.IsEvictable()) {
    throw bustub::Exception("Attempting to remove a non-evictable frame");
  }

  curr_size_--;
  node_store_.erase(it);
}

/**
 * @brief Return replacer's size, which tracks the number of evictable frames.
 *
 * @return size_t
 */
auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  return curr_size_;
}
}  // namespace bustub