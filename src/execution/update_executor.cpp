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
#include "concurrency/transaction.h"
#include "execution/executors/update_executor.h"
#include "storage/table/tuple.h"
#include "type/type.h"
#include "type/type_id.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have
  // perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() {
  // throw NotImplementedException("UpdateExecutor is not implemented");
  child_executor_->Init();
  table_info_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->TableOid());
}

// auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
//   if(is_end_) {
//     return false;
//   }

//   auto table_info =
//   GetExecutorContext()->GetCatalog()->GetTable(plan_->TableOid()); auto
//   indexs_info =
//   GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info->name_);
//   Tuple old_tuple{};
//   RID old_rid{};

//   int32_t update_cnt = 0;
//   while(child_executor_->Next(&old_tuple, &old_rid)) {
//     // std::cout << "what happend????" << std::endl;

//     std::vector<Value> values{};
//     for(const auto &expr : plan_->target_expressions_) {
//       auto value = expr->Evaluate(&old_tuple,
//       child_executor_->GetOutputSchema());
//       values.emplace_back(std::move(value));
//     }
//     Tuple new_tuple{values, &child_executor_->GetOutputSchema()};

//     // tuple
//     table_info->table_->UpdateTupleMeta(TupleMeta{INVALID_TXN_ID,
//     INVALID_TXN_ID, true}, old_rid); auto new_rid =
//     table_info->table_->InsertTuple(TupleMeta{INVALID_TXN_ID, INVALID_TXN_ID,
//     false}, new_tuple).value();

//     // indexs
//     for(auto index_info : indexs_info) {
//       auto old_key_tuple =
//       tuple->KeyFromTuple(child_executor_->GetOutputSchema(),
//       index_info->key_schema_, index_info->index_->GetKeyAttrs());
//       index_info->index_->DeleteEntry(old_key_tuple, old_rid,
//       exec_ctx_->GetTransaction()); auto new_key_tuple =
//       new_tuple.KeyFromTuple(child_executor_->GetOutputSchema(),
//       index_info->key_schema_, index_info->index_->GetKeyAttrs());
//       index_info->index_->InsertEntry(new_key_tuple, new_rid,
//       exec_ctx_->GetTransaction());
//     }
//     ++update_cnt;
//   }
//   std::vector<Value> values{};
//   values.reserve(GetOutputSchema().GetColumnCount());
//   values.emplace_back(INTEGER, update_cnt);
//   *tuple = Tuple{values, &GetOutputSchema()};
//   is_end_ = true;

//   return true;
// }

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_end_) {
    return false;
  }

  auto table_info = GetExecutorContext()->GetCatalog()->GetTable(plan_->TableOid());
  auto indexs_info = GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info->name_);
  Tuple old_tuple{};
  RID old_rid{};

  int32_t update_cnt = 0;
  while (child_executor_->Next(&old_tuple, &old_rid)) {
    std::vector<Value> values{};
    values.reserve(child_executor_->GetOutputSchema().GetColumnCount());
    std::unordered_set<uint32_t> diff_key_attrs;
    for (std::size_t i = 0; i < plan_->target_expressions_.size(); ++i) {
      const auto &expr = plan_->target_expressions_[i];
      auto new_value = expr->Evaluate(&old_tuple, child_executor_->GetOutputSchema());
      auto old_value = old_tuple.GetValue(&child_executor_->GetOutputSchema(), i);
      if (new_value.CompareEquals(old_value) == CmpBool::CmpFalse ||
          new_value.CompareEquals(old_value) == CmpBool::CmpNull) {
        diff_key_attrs.insert(i);
      }
      values.emplace_back(new_value);
    }
    auto new_tuple = Tuple{std::move(values), &child_executor_->GetOutputSchema()};

    std::cout << update_cnt << " update : " << new_tuple.ToString(&child_executor_->GetOutputSchema()) << std::endl;

    // delete old and insert new
    // 只更新改变了的索引
    std::vector<index_oid_t> index_oid_s;
    for (auto index_info : indexs_info) {
      bool change = false;
      for (auto key_attr : index_info->index_->GetKeyAttrs()) {
        if (diff_key_attrs.count(key_attr) == 1) {
          index_oid_s.push_back(index_info->index_oid_);
          change = true;
          break;
        }
      }
      if (change) {
        auto old_key_tuple = old_tuple.KeyFromTuple(child_executor_->GetOutputSchema(), index_info->key_schema_,
                                                    index_info->index_->GetKeyAttrs());
        index_info->index_->DeleteEntry(old_key_tuple, old_rid, exec_ctx_->GetTransaction());

        auto new_key_tuple = new_tuple.KeyFromTuple(child_executor_->GetOutputSchema(), index_info->key_schema_,
                                                    index_info->index_->GetKeyAttrs());
        index_info->index_->InsertEntry(new_key_tuple, old_rid, exec_ctx_->GetTransaction());
      }
    }
    exec_ctx_->GetTransaction()->AppendIndexWriteRecord(
        {old_rid, plan_->TableOid(), WType::UPDATE, new_tuple, old_tuple, index_oid_s, exec_ctx_->GetCatalog()});

    // delete old indexs
    // for (auto index_info : indexs_info) {
    //   auto old_key_tuple =
    //   old_tuple.KeyFromTuple(child_executor_->GetOutputSchema(),
    //   index_info->key_schema_,
    //                                                 index_info->index_->GetKeyAttrs());
    //   index_info->index_->DeleteEntry(old_key_tuple, old_rid,
    //   exec_ctx_->GetTransaction());
    // }

    // TODO(gukele) why insert bug!!!
    // auto new_rid = table_info->table_->InsertTuple({}, new_tuple,
    // exec_ctx_->GetLockManager(),
    // exec_ctx_->GetTransaction(),plan_->TableOid());
    // TODO(gukele)
    // 不知道什么插入操作后while循环不退出，使得删除再插入来模拟更新无法实现，所以使用了update(文档中说除非为了project4冲榜，否则不要用)
    table_info->table_->UpdateTupleInPlaceUnsafe({}, new_tuple, old_rid);

    // insert new indexs
    // for (auto index_info : indexs_info) {
    //   auto new_key_tuple =
    //   new_tuple.KeyFromTuple(child_executor_->GetOutputSchema(),
    //   index_info->key_schema_,
    //                                               index_info->index_->GetKeyAttrs());
    //   index_info->index_->InsertEntry(new_key_tuple, old_rid,
    //   exec_ctx_->GetTransaction());
    // }

    ++update_cnt;
  }
  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());
  values.emplace_back(TypeId::INTEGER, update_cnt);
  *tuple = Tuple{std::move(values), &GetOutputSchema()};
  is_end_ = true;

  return true;
}

}  // namespace bustub
