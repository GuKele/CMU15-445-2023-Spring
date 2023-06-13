#include "execution/executors/sort_executor.h"
#include <algorithm>
#include "binder/bound_order_by.h"
#include "storage/table/tuple.h"
#include "type/type.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  // throw NotImplementedException("SortExecutor is not implemented");
  child_executor_->Init();
  output_tuples_.clear();
  index_ = 0;

  Tuple tuple{};
  RID rid{};
  while (child_executor_->Next(&tuple, &rid)) {
    output_tuples_.emplace_back(tuple);
  }

  const auto &order_bys = plan_->GetOrderBy();

  auto comparator = [&order_bys, this](const Tuple &lhs, const Tuple &rhs) {
    for (const auto &[order_by_type, expr] : order_bys) {
      auto l = expr->Evaluate(&lhs, this->child_executor_->GetOutputSchema());
      auto r = expr->Evaluate(&rhs, this->child_executor_->GetOutputSchema());
      if (l.CompareEquals(r) == CmpBool::CmpTrue) {
        continue;
      }
      // bool less = (l.CompareLessThan(r) == CmpBool::CmpTrue);
      if (order_by_type == OrderByType::DESC) {
        return l.CompareGreaterThan(r) == CmpBool::CmpTrue;
      }
      // OrderByType::DEFAULT == OrderByType::ASC
      return l.CompareLessThan(r) == CmpBool::CmpTrue;
    }
    return false;
  };

  std::sort(output_tuples_.begin(), output_tuples_.end(), comparator);
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (index_ < output_tuples_.size()) {
    *tuple = output_tuples_[index_++];
    return true;
  }
  return false;
}

}  // namespace bustub
