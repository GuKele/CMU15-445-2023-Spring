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
#include "common/macros.h"
#include "common/rid.h"
#include "concurrency/lock_manager.h"
#include "concurrency/transaction.h"
#include "execution/expressions/comparison_expression.h"
#include "storage/table/table_iterator.h"
#include "storage/table/tuple.h"
#include "type/type.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  // throw NotImplementedException("SeqScanExecutor is not implemented");
  auto exec_ctx = GetExecutorContext();
  auto table_info = exec_ctx->GetCatalog()->GetTable(plan_->GetTableOid());
  // TODO(gukele): 存在问题，按理说RR下应该是加S表锁，如果是DELETE ... WHERE ...应该是SIX锁。
  // 但是好像因为历史问题，项目刚增加了意向锁的实现，所以目前使用意向锁+行锁来实现
  try {
    const auto txn = exec_ctx->GetTransaction();
    const auto oid = plan_->GetTableOid();
    if (exec_ctx_->IsDelete()) {  // 写锁
      /*
       * If the current operation is delete (by checking executor context
       * IsDelete(), which will be set to true for DELETE and UPDATE), you should
       * assume all tuples scanned will be deleted, and you should take X locks on
       * the table and tuple as necessary in step 2.
       */

      // 需要判断是否持有更高级的锁,从而避免反向锁升级
      if (!txn->IsTableIntentionExclusiveLocked(oid) && !txn->IsTableSharedIntentionExclusiveLocked(oid) &&
          !txn->IsTableExclusiveLocked(oid)) {
        if (!exec_ctx_->GetLockManager()->LockTable(txn, LockManager::LockMode::INTENTION_EXCLUSIVE, oid)) {
          throw ExecutionException("seq scan lock table IX failed");
        }
      }
    } else {  // 读锁
      if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
        if (!txn->IsTableIntentionSharedLocked(oid) && !txn->IsTableIntentionExclusiveLocked(oid) &&
            !txn->IsTableSharedLocked(oid) && !txn->IsTableSharedIntentionExclusiveLocked(oid) &&
            !txn->IsTableExclusiveLocked(oid)) {
          if (!exec_ctx_->GetLockManager()->LockTable(txn, LockManager::LockMode::INTENTION_SHARED, oid)) {
            throw ExecutionException("seq scan lock table IS failed");
          }
        }
      }
    }
  } catch (const TransactionAbortException &e) {
    throw ExecutionException("seq scan TransactionAbort");
  }

  /*
   * // NOTE(gukele): Halloween problem。
   * MakeIterator is introduced to avoid the Halloween problem in Project 3's
   * UpdateExecutor, but you do not need it now.
   * 万圣节问题，就是update有两种实现，一种是原地修改，一种是删除+插入，那么在遍历表并且做修改的时候，我们如果
   * 不记录当时表最大的数量，就会把后来更新(标记删除，然后表最后插入)的那写数据重新遍历，直到所有人都不满足update的条件了
   */

  // 不用索引的时候，使用MakeIterator()无法通过测试，因为会delete + insert,然后有个全表扫描，肯定扫不全
  // 因为后边会有新插入的，类似于万圣节问题，只不过现在是希望能看到新插入的
  // iterator_.emplace(table_info->table_->MakeIterator());
  iterator_.emplace(table_info->table_->MakeEagerIterator());
}

/*
 * In Init:
 *   take a table lock. Get an iterator by using MakeEagerIterator instead of MakeIterator.
 *   (MakeIterator is introduced to avoid the Halloween problem in Project 3's UpdateExecutor, but you do not need it
 * now.) In Next: 1.Get the current position of the table iterator. 2.Lock the tuple as needed for the isolation level.
 *   3.Fetch the tuple. Check tuple meta, and if you have implemented filter pushdown to scan,
 *     check the predicate.
 *   4.If the tuple should not be read by this transaction, force unlock the row.
 *     Otherwise, unlock the row as needed for the isolation level.
 *   5.If the current operation is delete (by checking executor context IsDelete(),
 *     which will be set to true for DELETE and UPDATE), you should assume all tuples scanned will be deleted,
 *     and you should take X locks on the table and tuple as necessary in step 2.
 */

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!iterator_) {
    throw Exception(ExceptionType::EXECUTION, "std::optional iterator should have value");
  }
  auto &iter = iterator_.value();
  auto txn = exec_ctx_->GetTransaction();
  auto scan_oid = plan_->GetTableOid();

  while (!iter.IsEnd()) {
    auto scan_rid = iter.GetRID();
    // 加锁
    auto lock_mode = LockManager::LockMode::SHARED;
    bool is_this_executor_get_row_lock = false;  // 表示是否是由本算子加上的lock
    try {
      if (exec_ctx_->IsDelete()) {  // 写锁
        lock_mode = LockManager::LockMode::EXCLUSIVE;
        if (!txn->IsRowExclusiveLocked(scan_oid, scan_rid)) {
          if (!exec_ctx_->GetLockManager()->LockRow(txn, lock_mode, scan_oid, scan_rid)) {
            throw ExecutionException("seq scan lock row X failed");
          }
          is_this_executor_get_row_lock = true;
        }
      } else {  // 读锁
        if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
          if (!txn->IsRowSharedLocked(scan_oid, scan_rid) && !txn->IsRowExclusiveLocked(scan_oid, scan_rid)) {
            if (!exec_ctx_->GetLockManager()->LockRow(txn, lock_mode, scan_oid, scan_rid)) {
              throw ExecutionException("seq scan lock row S failed");
            }
            is_this_executor_get_row_lock = true;
          }
        }
      }
    } catch (const TransactionAbortException &e) {
      throw ExecutionException("seq scan TransactionAbort");
    }

    auto [scan_tuple_meta, scan_tuple] = iter.GetTuple();
    ++iter;

    // 释放锁 RC下 row S 锁可以直接释放吧。
    try {
      if (txn->IsRowSharedLocked(scan_oid, scan_rid) && txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
        BUSTUB_ASSERT(txn->IsRowSharedLocked(scan_oid, scan_rid), "RC do not hold S row ??");
        if (!exec_ctx_->GetLockManager()->UnlockRow(txn, scan_oid, scan_rid)) {
          throw ExecutionException("seq scan unlock row S failed");
        }
      }
    } catch (const TransactionAbortException &e) {
      throw ExecutionException("seq scan TransactionAbort");
    }

    if (!scan_tuple_meta.is_deleted_) {
      if (plan_->filter_predicate_) {
        // std::cout << plan_->filter_predicate_->ToString() << std::endl;optimizer_custom_rules.cpp
        auto value = plan_->filter_predicate_->Evaluate(&scan_tuple, plan_->OutputSchema());
        if (!value.IsNull() && value.GetAs<bool>()) {
          *rid = scan_rid;
          *tuple = std::move(scan_tuple);
          return true;
        }
        // 不满足filter的，并且本executor得到的行锁，并且不是之前已经释放了的RC下S锁
        if (is_this_executor_get_row_lock && !(txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED &&
                                               lock_mode == LockManager::LockMode::SHARED)) {
          try {
            if (!exec_ctx_->GetLockManager()->UnlockRow(txn, scan_oid, scan_rid, true)) {
              throw ExecutionException("Force unlocking of S lock on row that do not meet the filter failed");
            }
          } catch (const std::exception &) {
            throw ExecutionException("seq scan TransactionAbort");
          }
        }
      } else {
        *rid = scan_rid;
        *tuple = std::move(scan_tuple);
        return true;
      }
    }
  }

  // std::cout << plan_->OutputSchema().ToString() << std::endl;

  // TODO(gukele): 如果是本算子加上的IS表锁，并且是RC，检查无本表的读锁以后，应该直接释放IS表锁吧
  return false;
}

}  // namespace bustub
