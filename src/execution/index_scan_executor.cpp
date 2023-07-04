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
#include <vector>
#include "catalog/catalog.h"
#include "common/macros.h"
#include "storage/index/b_plus_tree_index.h"
#include "storage/index/generic_key.h"
#include "storage/index/index.h"
#include "storage/table/tuple.h"
#include "type/value.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  // throw NotImplementedException("IndexScanExecutor is not implemented");
  auto exec_ctx = GetExecutorContext();
  auto catalog = exec_ctx->GetCatalog();
  auto index_info = catalog->GetIndex(plan_->GetIndexOid());
  table_info_ = catalog->GetTable(index_info->table_name_);
  const auto txn = exec_ctx->GetTransaction();

  try {
    const auto &table_name = index_info->table_name_;
    table_oid_ = catalog->GetTable(table_name)->oid_;

    if (exec_ctx->IsDelete()) {  // 写锁
      // 需要判断是否持有更高级的锁,从而避免反向锁升级
      if (!txn->IsTableIntentionExclusiveLocked(table_oid_) &&
          !txn->IsTableSharedIntentionExclusiveLocked(table_oid_) && !txn->IsTableExclusiveLocked(table_oid_)) {
        if (!exec_ctx->GetLockManager()->LockTable(txn, LockManager::LockMode::INTENTION_EXCLUSIVE, table_oid_)) {
          throw ExecutionException("index scan lock table IX failed");
        }
      }
    } else {  // 读锁
      if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
        if (!txn->IsTableIntentionSharedLocked(table_oid_) && !txn->IsTableIntentionExclusiveLocked(table_oid_) &&
            !txn->IsTableSharedLocked(table_oid_) && !txn->IsTableSharedIntentionExclusiveLocked(table_oid_) &&
            !txn->IsTableExclusiveLocked(table_oid_)) {
          if (!exec_ctx->GetLockManager()->LockTable(txn, LockManager::LockMode::INTENTION_SHARED, table_oid_)) {
            throw ExecutionException("index scan lock table IS failed");
          }
        }
      }
    }
  } catch (const TransactionAbortException &e) {
    throw ExecutionException("index scan TransactionAbort");
  }

  auto b_plus_tree_index = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info->index_.get());
  if (plan_->filter_predicate_ != nullptr) {
    BUSTUB_ASSERT(plan_->ranges_.size() == 1, "we only implement one range index scan plan");
    auto comparator = IntegerComparatorType(&index_info->key_schema_);
    auto range = plan_->ranges_.front();
    auto begin_key_values = range.first;
    auto begin_key_tuple = Tuple(begin_key_values, &index_info->key_schema_);
    IntegerKeyType begin_index_key;
    begin_index_key.SetFromKey(begin_key_tuple);
    iter_ = b_plus_tree_index->GetBeginIterator(begin_index_key);

    auto end_key_values = range.second;
    auto end_key_tuple = Tuple(end_key_values, &index_info->key_schema_);
    IntegerKeyType end_index_key;
    end_index_key.SetFromKey(end_key_tuple);
    iter_end_ = b_plus_tree_index->GetBeginIterator(end_index_key);
    // 开闭区间[iter_, iter_end_),所以如果不是end,并且我们恰好得到了end_index_key的iterator,那么++
    if (!iter_end_.IsEnd() && comparator(iter_end_->first, end_index_key) == 0) {
      ++iter_end_;
    }
  } else {
    iter_ = b_plus_tree_index->GetBeginIterator();
    iter_end_ = b_plus_tree_index->GetEndIterator();
  }
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto txn = GetExecutorContext()->GetTransaction();
  while (iter_ != iter_end_) {
    BUSTUB_ASSERT(!iter_.IsEnd(), "Should not access end iterator!");
    // *rid = (*iter_).second;
    *rid = iter_->second;
    ++iter_;

    // 加锁
    auto lock_mode = LockManager::LockMode::SHARED;
    bool is_this_executor_get_row_lock = false;  // 表示是否是由本算子加上的lock
    try {
      if (exec_ctx_->IsDelete()) {  // 写锁
        lock_mode = LockManager::LockMode::EXCLUSIVE;
        if (!txn->IsRowExclusiveLocked(table_oid_, *rid)) {
          if (!exec_ctx_->GetLockManager()->LockRow(txn, lock_mode, table_oid_, *rid)) {
            throw ExecutionException("index scan lock row X failed");
          }
          is_this_executor_get_row_lock = true;
        }
      } else {  // 读锁
        if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
          if (!txn->IsRowSharedLocked(table_oid_, *rid) && !txn->IsRowExclusiveLocked(table_oid_, *rid)) {
            if (!exec_ctx_->GetLockManager()->LockRow(txn, lock_mode, table_oid_, *rid)) {
              throw ExecutionException("index scan lock row S failed");
            }
            is_this_executor_get_row_lock = true;
          }
        }
      }
    } catch (const TransactionAbortException &e) {
      throw ExecutionException("index scan TransactionAbort");
    }

    auto [tuple_meta, scan_tuple] = table_info_->table_->GetTuple(*rid);

    // 释放锁 RC下 row S 锁可以直接释放吧。
    try {
      if (txn->IsRowSharedLocked(table_oid_, *rid) && txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
        BUSTUB_ASSERT(txn->IsRowSharedLocked(table_oid_, *rid), "RC do not hold S row ??");
        if (!exec_ctx_->GetLockManager()->UnlockRow(txn, table_oid_, *rid)) {
          throw ExecutionException("index scan unlock row S failed");
        }
      }
    } catch (const TransactionAbortException &e) {
      throw ExecutionException("index scan TransactionAbort");
    }

    if (!tuple_meta.is_deleted_) {
      if (plan_->filter_predicate_) {
        auto value = plan_->filter_predicate_->Evaluate(&scan_tuple, plan_->OutputSchema());
        if (!value.IsNull() && value.GetAs<bool>()) {
          *tuple = std::move(scan_tuple);
          return true;
        }
        // 不满足filter的，并且本executor得到的行锁，并且不是之前已经释放了的RC下S锁
        if (is_this_executor_get_row_lock && !(txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED &&
                                               lock_mode == LockManager::LockMode::SHARED)) {
          try {
            if (!exec_ctx_->GetLockManager()->UnlockRow(txn, table_oid_, *rid, true)) {
              throw ExecutionException("Force unlocking of S lock on row that do not meet the filter failed");
            }
          } catch (const std::exception &) {
            throw ExecutionException("index scan TransactionAbort");
          }
        }
      } else {
        *tuple = std::move(scan_tuple);
        return true;
      }
    }
  }

  return false;
}

}  // namespace bustub
