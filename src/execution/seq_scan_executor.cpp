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
#include "storage/table/table_iterator.h"
#include "storage/table/tuple.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  // throw NotImplementedException("SeqScanExecutor is not implemented");
  auto exec_ctx = GetExecutorContext();
  auto table_info = exec_ctx->GetCatalog()->GetTable(plan_->GetTableOid());
  // TODO(gukele): figure out Halloween problem。
  // MakeIterator is introduced to avoid the Halloween problem in Project 3's
  // UpdateExecutor, but you do not need it now.
  iterator_.emplace(table_info->table_->MakeEagerIterator());
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

      // TODO(gukele): 是否需要判断是否持有更高级的锁？从而避免反向锁升级
      if (!txn->IsTableIntentionExclusiveLocked(oid) && !txn->IsTableSharedIntentionExclusiveLocked(oid) &&
          !txn->IsTableExclusiveLocked(oid)) {
        if (!exec_ctx_->GetLockManager()->LockTable(txn, LockManager::LockMode::INTENTION_EXCLUSIVE, oid)) {
          throw ExecutionException("seq scan lock table IX failed");
        }
      }
      // if(txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {

      // } else if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {

      // }
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
      // if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      //   if(!txn->IsTableIntentionSharedLocked(oid) && !txn->IsTableIntentionExclusiveLocked(oid) &&
      //   !txn->IsTableSharedLocked(oid) && !txn->IsTableSharedIntentionExclusiveLocked(oid) &&
      //   !txn->IsTableExclusiveLocked(oid)) {
      //     if (!exec_ctx_->GetLockManager()->LockTable(txn, LockManager::LockMode::INTENTION_SHARED, oid)) {
      //       throw ExecutionException("seq scan lock table IS failed");
      //     }
      //   }
      // } // TODO(gukele):REPEATABLE_READ应该是也加is？只不过行s锁不会提前释放
      // else if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
      //   if(!txn->IsTableSharedLocked(oid) && !txn->IsTableSharedIntentionExclusiveLocked(oid) &&
      //   !txn->IsTableExclusiveLocked(oid)) {
      //     if (!exec_ctx_->GetLockManager()->LockTable(txn, LockManager::LockMode::SHARED, oid)) {
      //       throw ExecutionException("seq scan lock table S failed");
      //     }
      //   }
      // }
    }
  } catch (const TransactionAbortException &e) {
    throw ExecutionException("seq scan TransactionAbort");
  }
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
  auto &iter = iterator_.value();
  auto txn = exec_ctx_->GetTransaction();
  auto oid = plan_->GetTableOid();

  while (!iter.IsEnd()) {
    // 加锁
    auto lock_mode = LockManager::LockMode::SHARED;
    // bool is_this_executor_get_shared_row = false; //表示是否是由本算子加上的shared_row
    try {
      if (exec_ctx_->IsDelete()) {  // 写锁
        lock_mode = LockManager::LockMode::EXCLUSIVE;
        if (!txn->IsRowExclusiveLocked(oid, iter.GetRID())) {
          if (!exec_ctx_->GetLockManager()->LockRow(txn, lock_mode, oid, iter.GetRID())) {
            throw ExecutionException("seq scan lock row X failed");
          }
        }
      } else {  // 读锁
        if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
          if (!txn->IsRowSharedLocked(oid, iter.GetRID()) && !txn->IsRowExclusiveLocked(oid, iter.GetRID())) {
            if (!exec_ctx_->GetLockManager()->LockRow(txn, lock_mode, oid, iter.GetRID())) {
              throw ExecutionException("seq scan lock row S failed");
            }
            // is_this_executor_get_shared_row = true;
          }
        }
      }
    } catch (const TransactionAbortException &e) {
      throw ExecutionException("seq scan TransactionAbort");
    }

    auto [tuple_meta, tup] = iter.GetTuple();

    // 释放锁 RC下 row S 锁可以直接释放吧。
    try {
      if (txn->IsRowSharedLocked(oid, iter.GetRID()) && txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
        BUSTUB_ASSERT(txn->IsRowSharedLocked(oid, iter.GetRID()), "RC do not hold S row ??");
        if (!exec_ctx_->GetLockManager()->UnlockRow(txn, oid, iter.GetRID())) {
          throw ExecutionException("seq scan unlock row S failed");
        }
      }
    } catch (const TransactionAbortException &e) {
      throw ExecutionException("seq scan TransactionAbort");
    }

    if (!tuple_meta.is_deleted_) {
      // TODO(gukele): if you have implemented filter pushdown to scan, check the predicate.
      // 如果不符合谓词，那么即使是RR也使用UnlockRow(force = true)？？？？
      // 那如果是事物中别的算子加的读锁呢
      // if(/*不符合谓词*/) {
      //   if(/*本算子加上的shared_lock*/) {
      //     if(/*RR*/) {
      //       UnlockRow(force = true);
      //     }
      //   }
      // }
      *tuple = std::move(tup);
      *rid = tuple->GetRid();
      ++iter;
      return true;
    }
    // TODO(gukele):
    // else {
    //   //被删的也应该UnlockRow(force = true)吧？
    // }
    ++iter;
  }

  // std::cout << plan_->OutputSchema().ToString() << std::endl;

  // TODO(gukele): 如果是本算子加上的IS表锁，并且是读提交，检查无本表的读锁以后，应该直接释放IS表锁吧

  return false;
}

}  // namespace bustub
