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

  auto &write_set = *txn->GetIndexWriteSet();
  while (!write_set.empty()) {
    auto &item = write_set.front();
    auto &tid = item.table_oid_;
    auto &rid = item.rid_;
    auto table_info = item.catalog_->GetTable(tid);
    auto &table = *table_info->table_;
    auto &new_tuple = item.tuple_;
    auto &old_tuple = item.old_tuple_;

    if (item.wtype_ == WType::INSERT) {
      table.UpdateTupleMeta(TupleMeta{INVALID_TXN_ID, INVALID_TXN_ID, true}, rid);
      const auto indexs_info = item.catalog_->GetTableIndexes(table_info->name_);
      for (const auto &index_info : indexs_info) {
        auto new_key_tuple =
            new_tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
        index_info->index_->DeleteEntry(new_key_tuple, rid, txn);
      }
    } else if (item.wtype_ == WType::DELETE) {
      table.UpdateTupleMeta(TupleMeta{INVALID_TXN_ID, INVALID_TXN_ID, false}, rid);
      const auto indexs_info = item.catalog_->GetTableIndexes(table_info->name_);
      for (const auto &index_info : indexs_info) {
        auto old_key_tuple =
            old_tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
        index_info->index_->InsertEntry(old_key_tuple, rid, txn);
      }
    } else if (item.wtype_ == WType::UPDATE) {
      table.UpdateTupleInPlaceUnsafe({}, old_tuple, rid);

      for (auto index_oid : item.index_oid_s_) {
        auto index_info = item.catalog_->GetIndex(index_oid);
        auto old_key_tuple =
            old_tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
        auto new_key_tuple =
            new_tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());

        index_info->index_->DeleteEntry(new_key_tuple, rid, txn);
        index_info->index_->InsertEntry(old_key_tuple, rid, txn);
      }
    }

    write_set.pop_front();
  }

  txn->UnlockTxn();

  ReleaseLocks(txn);

  txn->SetState(TransactionState::ABORTED);
}

void TransactionManager::BlockAllTransactions() { UNIMPLEMENTED("block is not supported now!"); }

void TransactionManager::ResumeTransactions() { UNIMPLEMENTED("resume is not supported now!"); }

}  // namespace bustub
