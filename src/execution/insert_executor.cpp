//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdint>
#include <iostream>
#include <memory>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "execution/executor_context.h"
#include "execution/executors/insert_executor.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  // throw NotImplementedException("InsertExecutor is not implemented");
  child_executor_->Init();
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_end_) {
    return false;
  }

  Tuple insert_tuple{};
  RID insert_rid{};
  auto insert_table_info = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid());
  auto indexs_info = GetExecutorContext()->GetCatalog()->GetTableIndexes(insert_table_info->name_);

  int32_t insert_cnt = 0;
  while (child_executor_->Next(&insert_tuple, &insert_rid)) {
    // TODO(gukele) other args
    // auto o_rid = insert_table_info->table_->InsertTuple({}, *tuple);
    auto o_rid = insert_table_info->table_->InsertTuple({}, insert_tuple, exec_ctx_->GetLockManager(),
                                                        exec_ctx_->GetTransaction(), plan_->GetTableOid());

    if (o_rid.has_value()) {
      insert_rid = *o_rid;
    } else {
      throw Exception(ExceptionType::OUT_OF_RANGE, "Insert Error");
    }
    for (auto index_info : indexs_info) {
      auto key_tuple = insert_tuple.KeyFromTuple(child_executor_->GetOutputSchema(), index_info->key_schema_,
                                                 index_info->index_->GetKeyAttrs());
      index_info->index_->InsertEntry(key_tuple, insert_rid, exec_ctx_->GetTransaction());
    }
    ++insert_cnt;
  }
  // std::cout << "the output : " << plan_->OutputSchema().ToString() << std::endl;
  // std::cout << "insert count : " << insert_cnt << std::endl;

  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());
  values.emplace_back(INTEGER, insert_cnt);
  *tuple = Tuple{std::move(values), &GetOutputSchema()};
  is_end_ = true;

  return true;
}

}  // namespace bustub
