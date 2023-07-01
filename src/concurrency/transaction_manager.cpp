//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <mutex>  // NOLINT
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
namespace bustub {

void TransactionManager::Commit(Transaction *txn) {
  // Release all the locks.
  ReleaseLocks(txn);

  txn->SetState(TransactionState::COMMITTED);
}

void TransactionManager::Abort(Transaction *txn) {
  /* TODO: revert all the changes in write set */

  txn->LockTxn();

  auto &table_write_set = *txn->GetWriteSet();
  while (!table_write_set.empty()) {
    auto &item = table_write_set.back();

    if (item.wtype_ == WType::INSERT) {
      TupleMeta tuple_meta{INVALID_TXN_ID, INVALID_TXN_ID, true};
      item.table_heap_->UpdateTupleMeta(tuple_meta, item.rid_);
    } else if (item.wtype_ == WType::DELETE) {
      TupleMeta tuple_meta{INVALID_TXN_ID, INVALID_TXN_ID, false};
      item.table_heap_->UpdateTupleMeta(tuple_meta, item.rid_);
    } else {
      throw Exception(ExceptionType::EXECUTION,
                      "Impossible, we use IndexWriteRecord to express a actual TableWriteRecord updating in place");
    }

    table_write_set.pop_back();
  }

  auto &index_write_set = *txn->GetIndexWriteSet();
  while (!index_write_set.empty()) {
    // NOTE(gukele): 应该逆序处理，设想该事务先插入一行，然后又删除刚插入的一行，而正序处理会出现错误的结果
    auto &item = index_write_set.back();
    auto &tid = item.table_oid_;
    auto &rid = item.rid_;
    auto table_info = item.catalog_->GetTable(tid);
    auto &table = *table_info->table_;
    auto &new_tuple = item.tuple_;
    auto &old_tuple = item.old_tuple_;

    if (item.wtype_ == WType::INSERT) {
      const auto indexes_info = item.catalog_->GetTableIndexes(table_info->name_);
      for (const auto &index_info : indexes_info) {
        auto new_key_tuple =
            new_tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
        index_info->index_->DeleteEntry(new_key_tuple, rid, txn);
      }
    } else if (item.wtype_ == WType::DELETE) {
      const auto indexes_info = item.catalog_->GetTableIndexes(table_info->name_);
      for (const auto &index_info : indexes_info) {
        auto old_key_tuple =
            old_tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
        // FIXME(gukele): 如果回滚失败了呢？。。。因为我们锁管理器是对tuple上锁而非对索引上锁
        if (!index_info->index_->InsertEntry(old_key_tuple, rid, txn)) {
          throw ExecutionException("Roll back false!!!!");
        }
      }
    } else if (item.wtype_ == WType::UPDATE) {
      // 当为update时，实际上表示对tuple的原地修改
      table.UpdateTupleInPlaceUnsafe({INVALID_TXN_ID, INVALID_TXN_ID, false}, old_tuple, rid);
    }

    index_write_set.pop_back();
  }

  /*
  auto &index_write_set = *txn->GetIndexWriteSet();
  while (!index_write_set.empty()) {
    // NOTE(gukele): 应该逆序处理，设想该事务先插入一行，然后又删除刚插入的一行，而正序处理会出现错误的结果
    auto &item = index_write_set.back();
    auto &tid = item.table_oid_;
    auto &rid = item.rid_;
    auto table_info = item.catalog_->GetTable(tid);
    auto &table = *table_info->table_;
    auto &new_tuple = item.tuple_;
    auto &old_tuple = item.old_tuple_;

    if (item.wtype_ == WType::INSERT) {
      table.UpdateTupleMeta(TupleMeta{INVALID_TXN_ID, INVALID_TXN_ID, true}, rid);
      const auto indexes_info = item.catalog_->GetTableIndexes(table_info->name_);
      for (const auto &index_info : indexes_info) {
        auto new_key_tuple =
            new_tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
        index_info->index_->DeleteEntry(new_key_tuple, rid, txn);
      }
    } else if (item.wtype_ == WType::DELETE) {
      table.UpdateTupleMeta(TupleMeta{INVALID_TXN_ID, INVALID_TXN_ID, false}, rid);
      const auto indexes_info = item.catalog_->GetTableIndexes(table_info->name_);
      for (const auto &index_info : indexes_info) {
        auto old_key_tuple =
            old_tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
        index_info->index_->InsertEntry(old_key_tuple, rid, txn);
      }
    } else if (item.wtype_ == WType::UPDATE) {
      table.UpdateTupleInPlaceUnsafe({}, old_tuple, rid);

      // for (auto index_oid : item.index_oid_s_) {
      //   auto index_info = item.catalog_->GetIndex(index_oid);
      //   auto old_key_tuple =
      //       old_tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      //   auto new_key_tuple =
      //       new_tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());

      //   index_info->index_->DeleteEntry(new_key_tuple, rid, txn);
      //   index_info->index_->InsertEntry(old_key_tuple, rid, txn);
      // }

      auto &index_oid = item.index_oid_;
      auto index_info = item.catalog_->GetIndex(index_oid);
      auto old_key_tuple =
          old_tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      auto new_key_tuple =
          new_tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());

      index_info->index_->DeleteEntry(new_key_tuple, rid, txn);
      index_info->index_->InsertEntry(old_key_tuple, rid, txn);
    }

    index_write_set.pop_back();
  }
  */

  txn->UnlockTxn();

  ReleaseLocks(txn);

  txn->SetState(TransactionState::ABORTED);
}

void TransactionManager::BlockAllTransactions() { UNIMPLEMENTED("block is not supported now!"); }

void TransactionManager::ResumeTransactions() { UNIMPLEMENTED("resume is not supported now!"); }

}  // namespace bustub
