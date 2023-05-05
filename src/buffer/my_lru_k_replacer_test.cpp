#include "buffer/lru_k_replacer.h"


auto main() -> int {
  bustub::LRUKReplacer replacer(5, 2);
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

  return 0;
}