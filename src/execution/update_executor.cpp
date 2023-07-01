//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <cstddef>
#include <cstdint>
#include <memory>
#include <unordered_set>
#include <vector>

#include "common/config.h"
#include "common/macros.h"
#include "concurrency/lock_manager.h"
#include "concurrency/transaction.h"
#include "execution/executors/update_executor.h"
#include "storage/table/tuple.h"
#include "type/type.h"
#include "type/type_id.h"
#include "type/value_factory.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have
  // perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() {
  // throw NotImplementedException("UpdateExecutor is not implemented");
  // std::cout << plan_->ToString() << std::endl;

  child_executor_->Init();
  is_update_finished_ = false;

  table_info_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->TableOid());
  auto txn = GetExecutorContext()->GetTransaction();
  auto oid = plan_->TableOid();
  // FIXME(gukele): 事务加锁？
  try {
    if (!txn->IsTableIntentionExclusiveLocked(oid) && !txn->IsTableSharedIntentionExclusiveLocked(oid) &&
        !txn->IsTableExclusiveLocked(oid)) {
      if (!exec_ctx_->GetLockManager()->LockTable(txn, LockManager::LockMode::INTENTION_EXCLUSIVE, oid)) {
        throw ExecutionException("Update lock table IX failed");
      }
    }
  } catch (const TransactionAbortException &e) {
    throw ExecutionException("Update TransactionAbort");
  }
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_update_finished_) {
    return false;
  }

  auto table_info = GetExecutorContext()->GetCatalog()->GetTable(plan_->TableOid());
  auto indexes_info = GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info->name_);
  auto txn = GetExecutorContext()->GetTransaction();
  Tuple old_tuple{};
  RID old_rid{};

  int32_t update_cnt = 0;
  while (child_executor_->Next(&old_tuple, &old_rid)) {
    std::vector<Value> values{};
    values.reserve(child_executor_->GetOutputSchema().GetColumnCount());
    bool can_update_in_place = true;

    std::unordered_set<uint32_t> diff_column_idx;
    for (std::size_t i = 0; i < plan_->target_expressions_.size(); ++i) {
      const auto &expr = plan_->target_expressions_[i];
      auto new_column_value = expr->Evaluate(&old_tuple, child_executor_->GetOutputSchema());
      auto old_column_value = old_tuple.GetValue(&child_executor_->GetOutputSchema(), i);
      if (new_column_value.CompareEquals(old_column_value) == CmpBool::CmpFalse ||
          new_column_value.CompareEquals(old_column_value) == CmpBool::CmpNull) {
        diff_column_idx.insert(i);
        if (new_column_value.GetTypeId() == TypeId::VARCHAR &&
            new_column_value.GetLength() > old_column_value.GetLength()) {
          can_update_in_place = false;
        }
      }
      values.emplace_back(new_column_value);
    }

    auto new_tuple = Tuple{std::move(values), &child_executor_->GetOutputSchema()};

    // TODO(gukele): for the gradescope test, we can't change IndexWriteRecord
    // exec_ctx_->GetTransaction()->AppendIndexWriteRecord(
    //     {old_rid, plan_->TableOid(), WType::UPDATE, new_tuple, old_tuple, index_oid_s, exec_ctx_->GetCatalog()});

    // TODO(gukele): why insert bug!!!
    // auto new_rid = table_info->table_->InsertTuple({}, new_tuple,
    // exec_ctx_->GetLockManager(),
    // exec_ctx_->GetTransaction(),plan_->TableOid());
    // 不知道什么插入操作后while循环不退出，使得删除再插入来模拟更新无法实现，所以使用了update(文档中说除非为了project4冲榜，否则不要用)

    // FIXME(gukele): 当含有变长VARCHAR类型的修改时，保证新的VARCHAR长度不会超过旧的VARCHAR对象才能原地修改
    RID new_rid{};
    if (can_update_in_place) {  // 原地修改
      new_rid = old_rid;
      table_info->table_->UpdateTupleInPlaceUnsafe({}, new_tuple, new_rid);
      // 无法利用TableWriteRecord来恢复原地修改啊！！
      // 所以用IndexWriteRecord wtype_=update的时候表示这是table修改而非index修改
      // TableWriteRecord table_write_record(table_info->oid_, );
      // table_write_record.wtype_ = WType::UPDATE;
      IndexWriteRecord actual_table_write_record(old_rid, table_info->oid_, WType::UPDATE, new_tuple, -1,
                                                 exec_ctx_->GetCatalog());
      actual_table_write_record.old_tuple_ = old_tuple;
      txn->LockTxn();
      txn->AppendIndexWriteRecord(actual_table_write_record);
      txn->UnlockTxn();
    } else {  // 先插入后删除
      auto o_rid = table_info->table_->InsertTuple({}, new_tuple, exec_ctx_->GetLockManager(), txn, table_info->oid_);
      if (o_rid) {
        new_rid = *o_rid;

        try {
          if (!exec_ctx_->GetLockManager()->LockRow(txn, LockManager::LockMode::EXCLUSIVE, table_info->oid_, new_rid)) {
            throw ExecutionException("Update lock row X failed");
          }
        } catch (const TransactionAbortException &e) {
          throw ExecutionException("Update TransactionAbort");
        }

        txn->LockTxn();
        TableWriteRecord insert_table_write_record(table_info->oid_, new_rid, table_info->table_.get());
        insert_table_write_record.wtype_ = WType::INSERT;
        txn->AppendTableWriteRecord(insert_table_write_record);
        txn->UnlockTxn();
      } else {
        *tuple = Tuple{std::vector<Value>{ValueFactory::GetIntegerValue(0)}, &GetOutputSchema()};
        is_update_finished_ = true;
        txn->SetState(TransactionState::ABORTED);
        return false;
      }

      // FIXME(gukele): the insert and delete txn id
      TupleMeta meta{INVALID_TXN_ID, INVALID_TXN_ID, true};
      table_info->table_->UpdateTupleMeta(meta, old_rid);
      TableWriteRecord delete_table_write_record(table_info->oid_, old_rid, table_info->table_.get());
      delete_table_write_record.wtype_ = WType::DELETE;
      txn->LockTxn();
      txn->AppendTableWriteRecord(delete_table_write_record);
      txn->UnlockTxn();
    }

    // delete old index and insert new index
    // 只更新改变了的索引
    std::vector<index_oid_t> index_oid_s;
    for (auto index_info : indexes_info) {
      bool has_change = false;
      for (auto key_attr : index_info->index_->GetKeyAttrs()) {
        if (diff_column_idx.count(key_attr) == 1) {
          index_oid_s.push_back(index_info->index_oid_);
          has_change = true;
          break;
        }
      }
      if (has_change) {
        auto new_key_tuple = new_tuple.KeyFromTuple(child_executor_->GetOutputSchema(), index_info->key_schema_,
                                                    index_info->index_->GetKeyAttrs());
        auto succeed = index_info->index_->InsertEntry(new_key_tuple, new_rid, exec_ctx_->GetTransaction());
        if (!succeed) {
          *tuple = Tuple{std::vector<Value>{ValueFactory::GetIntegerValue(0)}, &GetOutputSchema()};
          is_update_finished_ = true;
          txn->SetState(TransactionState::ABORTED);
          return false;
        }

        auto old_key_tuple = old_tuple.KeyFromTuple(child_executor_->GetOutputSchema(), index_info->key_schema_,
                                                    index_info->index_->GetKeyAttrs());
        index_info->index_->DeleteEntry(old_key_tuple, old_rid, exec_ctx_->GetTransaction());

        // 原地更新使用一条IndexWriteRecord就可以表示了,因为原地更新，rid是相同的。
        // 但是因为无法利用TableWriteRecord来恢复原地修改啊！！所以用IndexWriteRecord
        // wtype_=update的时候表示这是table修改而非index修改 if(can_update_in_place) {
        //   BUSTUB_ASSERT(new_rid == old_rid, "Updating in place should have same rid!");
        //   IndexWriteRecord update_index_write_record(new_rid, plan_->TableOid(), WType::UPDATE, new_tuple,
        //       index_info->index_oid_, exec_ctx_->GetCatalog());
        //   update_index_write_record.old_tuple_ = old_tuple;
        //   txn->LockTxn();
        //   txn->AppendIndexWriteRecord(update_index_write_record);
        //   txn->UnlockTxn();
        // } else {
        IndexWriteRecord insert_index_write_record(new_rid, plan_->TableOid(), WType::INSERT, new_tuple,
                                                   index_info->index_oid_, exec_ctx_->GetCatalog());
        txn->LockTxn();
        txn->AppendIndexWriteRecord(insert_index_write_record);
        txn->UnlockTxn();

        IndexWriteRecord delete_index_write_record(old_rid, table_info->oid_, WType::DELETE, new_tuple,
                                                   index_info->index_oid_, exec_ctx_->GetCatalog());
        delete_index_write_record.old_tuple_ = old_tuple;
        txn->LockTxn();
        txn->AppendIndexWriteRecord(delete_index_write_record);
        txn->UnlockTxn();
        // }
      }
    }

    ++update_cnt;
  }
  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());
  values.emplace_back(TypeId::INTEGER, update_cnt);
  *tuple = Tuple{std::move(values), &GetOutputSchema()};
  is_update_finished_ = true;

  return true;
}

}  // namespace bustub
