//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.h
//
// Identification: src/include/execution/executors/index_scan_executor.h
//
// Copyright (c) 2015-20, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

#include "catalog/catalog.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/index_scan_plan.h"
#include "storage/index/b_plus_tree_index.h"
#include "storage/index/index_iterator.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * IndexScanExecutor executes an index scan over a table.
 */

class IndexScanExecutor : public AbstractExecutor {
 public:
  /**
   * Creates a new index scan executor.
   * @param exec_ctx the executor context
   * @param plan the index scan plan to be executed
   */
  IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan);

  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  void Init() override;

  auto Next(Tuple *tuple, RID *rid) -> bool override;

 private:
  /** The index scan plan node to be executed. */
  const IndexScanPlanNode *plan_;
  table_oid_t table_oid_;
  // BPlusTreeIndexForTwoIntegerColumn *tree_;

  TableInfo *table_info_;
  // FIXME(gukele): 索引扫描如何加锁！这里直接没有考虑并发的情况，应该是只有begin iterator,直到找到第一个不满足索引
  // 开闭区间[iter_, iter_end_)
  BPlusTreeIndexIteratorForTwoIntegerColumn iter_;
  BPlusTreeIndexIteratorForTwoIntegerColumn iter_end_;
};
}  // namespace bustub
