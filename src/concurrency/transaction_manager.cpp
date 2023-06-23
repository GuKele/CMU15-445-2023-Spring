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
  while(!write_set.empty()) {
    auto &item = write_set.front();
    auto &tid = item.table_oid_;
    auto &rid = item.rid_;
    auto table_info = item.catalog_->GetTable(tid);
    auto &table = *table_info->table_;
    auto indexs_info = item.catalog_->GetTableIndexes(table_info->name_);

    if(item.wtype_ == WType::INSERT) {
      table.UpdateTupleMeta(TupleMeta{INVALID_TXN_ID, INVALID_TXN_ID, true}, rid);
      
    } else if(item.wtype_ == WType::DELETE) {
      table.UpdateTupleMeta(TupleMeta{INVALID_TXN_ID, INVALID_TXN_ID, false}, rid);
    } else if(item.wtype_ == WType::UPDATE) {

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
