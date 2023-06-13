//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdint>
#include <memory>

#include "common/config.h"
#include "common/rid.h"
#include "execution/executors/delete_executor.h"
#include "storage/table/tuple.h"
#include "type/type.h"
#include "type/type_id.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  // throw NotImplementedException("DeleteExecutor is not implemented");
  child_executor_->Init();
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_end_) {
    return false;
  }
  auto table_info = GetExecutorContext()->GetCatalog()->GetTable(plan_->TableOid());
  auto indexs_info = GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info->name_);
  Tuple delete_tuple{};
  RID delete_rid{};

  int32_t delete_cnt = 0;
  while (child_executor_->Next(&delete_tuple, &delete_rid)) {
    TupleMeta meta{INVALID_TXN_ID, INVALID_TXN_ID, true};
    // meta.is_deleted_ = true;
    table_info->table_->UpdateTupleMeta(meta, delete_rid);

    for (const auto &index_info : indexs_info) {
      auto key_tuple = delete_tuple.KeyFromTuple(child_executor_->GetOutputSchema(), index_info->key_schema_,
                                                 index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(key_tuple, delete_rid, exec_ctx_->GetTransaction());
    }
    ++delete_cnt;
  }
  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());
  values.emplace_back(Value{INTEGER, delete_cnt});
  *tuple = Tuple{values, &GetOutputSchema()};
  is_end_ = true;

  return true;
}

}  // namespace bustub
