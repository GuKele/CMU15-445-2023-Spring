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
#include <cstdint>
#include <memory>
#include <vector>

#include "common/config.h"
#include "execution/executors/update_executor.h"
#include "storage/table/tuple.h"
#include "type/type.h"
#include "type/type_id.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() {
  // throw NotImplementedException("UpdateExecutor is not implemented");
  child_executor_->Init();
  table_info_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid());
}

// auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
//   if(is_end_) {
//     return false;
//   }

//   auto table_info = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid());
//   auto indexs_info = GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info->name_);
//   Tuple old_tuple{};
//   RID old_rid{};

//   int32_t update_cnt = 0;
//   while(child_executor_->Next(&old_tuple, &old_rid)) {
//     // std::cout << "what happend????" << std::endl;

//     std::vector<Value> values{};
//     for(const auto &expr : plan_->target_expressions_) {
//       auto value = expr->Evaluate(&old_tuple, child_executor_->GetOutputSchema());
//       values.emplace_back(std::move(value));
//     }
//     Tuple new_tuple{values, &child_executor_->GetOutputSchema()};

//     // tuple
//     table_info->table_->UpdateTupleMeta(TupleMeta{INVALID_TXN_ID, INVALID_TXN_ID, true}, old_rid);
//     auto new_rid = table_info->table_->InsertTuple(TupleMeta{INVALID_TXN_ID, INVALID_TXN_ID, false},
//     new_tuple).value();

//     // indexs
//     for(auto index_info : indexs_info) {
//       auto old_key_tuple = tuple->KeyFromTuple(child_executor_->GetOutputSchema(), index_info->key_schema_,
//       index_info->index_->GetKeyAttrs()); index_info->index_->DeleteEntry(old_key_tuple, old_rid,
//       exec_ctx_->GetTransaction()); auto new_key_tuple = new_tuple.KeyFromTuple(child_executor_->GetOutputSchema(),
//       index_info->key_schema_, index_info->index_->GetKeyAttrs()); index_info->index_->InsertEntry(new_key_tuple,
//       new_rid, exec_ctx_->GetTransaction());
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

  auto table_info = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid());
  auto indexs_info = GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info->name_);
  Tuple old_tuple{};
  RID old_rid{};

  int32_t update_cnt = 0;
  while (child_executor_->Next(&old_tuple, &old_rid)) {
    std::vector<Value> values{};
    values.reserve(child_executor_->GetOutputSchema().GetColumnCount());
    for (const auto &expr : plan_->target_expressions_) {
      auto value = expr->Evaluate(&old_tuple, child_executor_->GetOutputSchema());
      values.emplace_back(value);
    }
    auto new_tuple = Tuple{std::move(values), &child_executor_->GetOutputSchema()};

    std::cout << update_cnt << " update : " << new_tuple.ToString(&child_executor_->GetOutputSchema()) << std::endl;

    // delete old
    // table_info->table_->UpdateTupleMeta(TupleMeta{INVALID_TXN_ID, INVALID_TXN_ID, true}, old_rid);
    // TODO(gukele): 只更新改变了的索引
    for (auto index_info : indexs_info) {
      auto old_key_tuple = old_tuple.KeyFromTuple(child_executor_->GetOutputSchema(), index_info->key_schema_,
                                                  index_info->index_->GetKeyAttrs());
      auto new_key_tuple = new_tuple.KeyFromTuple(child_executor_->GetOutputSchema(), index_info->key_schema_,
                                                  index_info->index_->GetKeyAttrs());

      // index_info->index_->
      index_info->index_->DeleteEntry(old_key_tuple, old_rid, exec_ctx_->GetTransaction());
      index_info->index_->InsertEntry(new_key_tuple, old_rid, exec_ctx_->GetTransaction());
    }
    // insert new
    // TODO(gukele) why insert bug!!!
    // auto new_rid = table_info->table_->InsertTuple({}, new_tuple, exec_ctx_->GetLockManager(),
    // exec_ctx_->GetTransaction(),plan_->GetTableOid());
    // TODO(gukele)
    // 不知道什么插入操作后while循环不退出，使得删除再插入来模拟更新无法实现，所以使用了update(文档中说除非为了project4冲榜，否则不要用)
    table_info->table_->UpdateTupleInPlaceUnsafe({}, new_tuple, old_rid);
    for (auto index_info : indexs_info) {
      auto new_key_tuple = new_tuple.KeyFromTuple(child_executor_->GetOutputSchema(), index_info->key_schema_,
                                                  index_info->index_->GetKeyAttrs());
      index_info->index_->InsertEntry(new_key_tuple, old_rid, exec_ctx_->GetTransaction());
      // index_info->index_->InsertEntry(new_key_tuple, *new_rid, exec_ctx_->GetTransaction());
    }

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
