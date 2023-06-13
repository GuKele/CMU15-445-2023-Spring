//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include <memory>
#include <optional>
#include "common/config.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "storage/table/table_iterator.h"
#include "storage/table/tuple.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  // throw NotImplementedException("SeqScanExecutor is not implemented");
  auto exec_ctx = GetExecutorContext();
  auto table_info = exec_ctx->GetCatalog()->GetTable(plan_->GetTableOid());
  // *iterator_ = table_info->table_->MakeEagerIterator();
  iterator_.emplace(table_info->table_->MakeEagerIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto &iter = iterator_.value();
  while (!iter.IsEnd()) {
    auto [tuple_meta, tup] = iter.GetTuple();
    if (!tuple_meta.is_deleted_) {
      *tuple = std::move(tup);
      *rid = tuple->GetRid();
      ++iter;
      return true;
    }
    ++iter;
  }
  // std::cout << plan_->OutputSchema().ToString() << std::endl;

  return false;
}

}  // namespace bustub
