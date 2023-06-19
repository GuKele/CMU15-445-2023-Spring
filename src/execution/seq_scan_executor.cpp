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
#include "common/exception.h"
#include "common/rid.h"
#include "concurrency/lock_manager.h"
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
  // TODO(gukele):MakeIterator is introduced to avoid the Halloween problem in Project 3's UpdateExecutor, but you do not need it now.
  iterator_.emplace(table_info->table_->MakeEagerIterator());
  // TODO(gukele):存在问题，按理说RR下应该是加S表锁，如果是DELETE ... WHERE ...应该是SIX锁。
  // 但是好像是项目刚增加了意向锁，所以目前使用意向锁+行锁来实现
  try {
    if(exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED &&
      !exec_ctx_->GetLockManager()->LockTable(exec_ctx->GetTransaction(), LockManager::LockMode::INTENTION_SHARED, plan_->GetTableOid())) {
      throw ExecutionException("seq scan lock table share failed");
      }
  } catch (const TransactionAbortException &e) {
    throw ExecutionException("seq scan TransactionAbort");
  }
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
