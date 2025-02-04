#include <memory>
#include "common/macros.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement sort + limit -> top N optimizer rule
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSortLimitAsTopN(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::Limit) {
    const auto &limit_plan = dynamic_cast<const LimitPlanNode &>(*optimized_plan);
    BUSTUB_ASSERT(limit_plan.children_.size() == 1, "Limit plan should have at most one child plan.");
    if (const auto sort_plan = dynamic_cast<const SortPlanNode *>(limit_plan.GetChildPlan().get());
        sort_plan != nullptr) {
      BUSTUB_ENSURE(sort_plan->children_.size() == 1, "Sort Plan should have exactly 1 child.");
      auto order_bys = sort_plan->GetOrderBy();
      auto n = limit_plan.GetLimit();
      return std::make_shared<TopNPlanNode>(optimized_plan->output_schema_, sort_plan->GetChildPlan(), order_bys, n);
    }
  }
  return optimized_plan;
}

}  // namespace bustub
