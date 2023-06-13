//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  // throw NotImplementedException("IndexScanExecutor is not implemented");
  auto index_info = GetExecutorContext()->GetCatalog()->GetIndex(plan_->GetIndexOid());
  table_info_ = GetExecutorContext()->GetCatalog()->GetTable(index_info->table_name_);

  auto tree = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info->index_.get());
  iter_ = tree->GetBeginIterator();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_.IsEnd()) {
    return false;
  }

  *rid = (*iter_).second;
  *tuple = table_info_->table_->GetTuple(*rid).second;
  ++iter_;

  return true;
}

}  // namespace bustub
