#include <iostream>
#include <list>
#include <unordered_map>
#include <utility>
#include "buffer/lru_k_replacer.h"
#include "common/config.h"

auto main() -> int {
  bustub::LRUKReplacer replacer(10, 2);
  replacer.RecordAccess(0);
  replacer.RecordAccess(1);
  replacer.RecordAccess(2);
  replacer.RecordAccess(3);
  replacer.RecordAccess(4);
  replacer.RecordAccess(5);
  replacer.RecordAccess(6);

  replacer.SetEvictable(0, false);
  replacer.SetEvictable(1, false);
  replacer.SetEvictable(2, false);
  replacer.SetEvictable(3, false);
  replacer.SetEvictable(4, false);

  replacer.RecordAccess(0);

  replacer.Evict(nullptr);
  replacer.Evict(nullptr);
  replacer.Evict(nullptr);

  // using NodePair = std::pair<std::list<bustub::frame_id_t>::iterator, bustub::LRUKNode>;

  // std::unordered_map<bustub::frame_id_t, NodePair> myset = {{111, NodePair{nullptr, bustub::LRUKNode(555)}}};

  // std::unordered_map<bustub::frame_id_t, std::pair<std::list<bustub::frame_id_t>::iterator, bustub::LRUKNode>> myset
  // = {
  //   {111, std::pair<std::list<bustub::frame_id_t>::iterator, bustub::LRUKNode>{nullptr, bustub::LRUKNode(555)}}
  // };

  return 0;
}
