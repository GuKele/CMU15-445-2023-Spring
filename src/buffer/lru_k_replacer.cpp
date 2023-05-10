//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <cstdint>
#include <iostream>
#include <ostream>
#include <utility>

#include "common/config.h"
#include "common/exception.h"
#include "storage/page/page_guard.h"

namespace bustub {

LRUKNode::LRUKNode(frame_id_t frame_id) : fid_(frame_id) {}

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : max_frame_capacity_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::unique_lock<std::mutex> lock(latch_);
  return EvictImpl(frame_id);
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  BUSTUB_ASSERT(frame_id < static_cast<int32_t>(max_frame_capacity_), "the frame_id dxceeding capacity");
  std::lock_guard<std::mutex> gurad(latch_);
  auto map_it = nodes_.find(frame_id);
  if (map_it == nodes_.end()) {
    if (nodes_.size() == max_frame_capacity_ && !EvictImpl(nullptr)) {
      // 满了，并且也没办法排除一个
      // std::cout << "don't have space" << std::endl;
    } else {
      non_k_nodes_fifo_.push_back(frame_id);
      nodes_[frame_id] = NodePair(--(non_k_nodes_fifo_.end()), LRUKNode(frame_id));
    }
  } else {  // 本来就有
    auto list_it = map_it->second.first;
    auto &node = map_it->second.second;
    node.SetK();
    if (node.GetK() >= k_) {  // 需要从non_k_nodes_转移到k_nodes_头部
      k_nodes_lru_.splice(k_nodes_lru_.begin(), non_k_nodes_fifo_, list_it);
    }
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> guard(latch_);
  auto map_it = nodes_.find(frame_id);
  BUSTUB_ASSERT(map_it != nodes_.end(), "set a non-available frame");

  if (map_it != nodes_.end()) {
    auto &node = map_it->second.second;
    bool old_evictable = node.GetIsEvictable();
    if (old_evictable != set_evictable) {
      if (old_evictable) {
        --replacer_size_;
      } else {
        ++replacer_size_;
      }
      node.SetIsEvictable(set_evictable);
    }
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(latch_);
  RemoveImpl(frame_id);
}

// auto LRUKReplacer::Size() -> size_t { return 0; }
auto LRUKReplacer::Size() -> size_t { return replacer_size_; }

void LRUKReplacer::RemoveImpl(frame_id_t frame_id) {
  auto map_it = nodes_.find(frame_id);
  if (map_it != nodes_.end()) {
    auto &node_pair = map_it->second;
    auto &list_it = node_pair.first;
    auto &node = node_pair.second;

    BUSTUB_ASSERT(node.GetIsEvictable(), "the frame is non-evictable");

    if (node.GetIsEvictable()) {
      if (node.GetK() >= k_) {
        k_nodes_lru_.erase(list_it);
      } else {
        non_k_nodes_fifo_.erase(list_it);
      }
      nodes_.erase(map_it);
      --replacer_size_;
    }
  }
}

auto LRUKReplacer::EvictImpl(frame_id_t *frame_id) -> bool {
  if (replacer_size_ == 0) {
    return false;
  }

  // 先从non_k_node_fifo_正序fifo找到第一个可以消除的
  for (auto fid : non_k_nodes_fifo_) {
    auto &node = nodes_[fid].second;
    if (node.GetIsEvictable()) {
      *frame_id = node.GetFrameId();
      RemoveImpl(*frame_id);
      return true;
    }
  }
  // 找不到就从k_nond_lru_逆序lru找到
  for (auto it = k_nodes_lru_.rbegin(); it != k_nodes_lru_.rend(); ++it) {
    auto &node = nodes_[*it].second;
    if (node.GetIsEvictable()) {
      *frame_id = node.GetFrameId();
      RemoveImpl(*frame_id);
      return true;
    }
  }
  return true;
}

}  // namespace bustub
