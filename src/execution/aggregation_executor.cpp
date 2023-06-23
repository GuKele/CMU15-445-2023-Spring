//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"
#include "execution/plans/aggregation_plan.h"
#include "storage/table/tuple.h"
#include "type/type.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
      aht_iter_(aht_.End()) {}

void AggregationExecutor::Init() {
  child_executor_->Init();
  aht_.Clear();
  aht_iter_ = aht_.Begin();
  Tuple tuple{};
  RID rid{};

  while (child_executor_->Next(&tuple, &rid)) {
    auto agg_key = MakeAggregateKey(&tuple);
    auto agg_val = MakeAggregateValue(&tuple);
    aht_.InsertCombine(agg_key, agg_val);
  }

  // TODO(gukele)
  // when we have zero tuple to aggregate
  if (aht_.Begin() == aht_.End() && GetOutputSchema().GetColumnCount() == 1) {
    aht_.InsertInitCombine();
  }

  aht_iter_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (aht_iter_ == aht_.End()) {
    return false;
  }
  std::vector<Value> values{};
  values.insert(values.end(), aht_iter_.Key().group_bys_.begin(), aht_iter_.Key().group_bys_.end());
  values.insert(values.end(), aht_iter_.Val().aggregates_.begin(), aht_iter_.Val().aggregates_.end());
  *tuple = Tuple{std::move(values), &GetOutputSchema()};
  ++aht_iter_;

  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
