#include <algorithm>
#include <cstddef>
#include <memory>
#include <vector>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Spring: You should at least support join keys of the form:
  // 1. <column expr> = <column expr>
  // 2. <column expr> = <column expr> AND <column expr> = <column expr>
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    // Has exactly one child
    BUSTUB_ENSURE(nlj_plan.children_.size() == 2, "NLJ should have exactly 2 children.");

    /*
     * // TODO(gukele):
     * (#0.2=#1.1) and (#1.2=#0.1) and (#0.1 > 10) ...
     * 后边(#0.1 > 10) column 与 constant 比较可以下推到join的child plan
     */

    // ((#0.2=#1.1)and(#1.2=#0.1))
    if (const auto logic_expr = dynamic_cast<LogicExpression *>(nlj_plan.Predicate().get());
        logic_expr != nullptr && logic_expr->logic_type_ == LogicType::And) {
      BUSTUB_ENSURE(logic_expr->GetChildren().size() == 2, "we only support two ComparisonExpression");
      if (const auto left_comp_expr = dynamic_cast<ComparisonExpression *>(logic_expr->GetChildAt(0).get());
          left_comp_expr != nullptr && left_comp_expr->comp_type_ == ComparisonType::Equal) {
        if (const auto right_comp_expr = dynamic_cast<ComparisonExpression *>(logic_expr->GetChildAt(1).get());
            right_comp_expr != nullptr && right_comp_expr->comp_type_ == ComparisonType::Equal) {
          std::vector<ComparisonExpression *> comp_exprs{left_comp_expr, right_comp_expr};
          std::vector<AbstractExpressionRef> left_exprs{};
          std::vector<AbstractExpressionRef> right_exprs{};
          // TODO(gukele): only support ColumnValueExpression，note: ColumnValueExpression::tuple_ind_
          // FIXME(gukele): bug! 如果不满足comp_expr的child_expr是ColumnValueExpression应该放弃本优化。
          for (const auto comp_expr : comp_exprs) {
            BUSTUB_ENSURE(comp_expr->children_.size() == 2, "comp_expr should have exactly 2 children.");
            if (const auto column_expr = dynamic_cast<ColumnValueExpression *>(comp_expr->GetChildAt(0).get());
                column_expr != nullptr) {
              if (column_expr->GetTupleIdx() == 0) {
                left_exprs.emplace_back(comp_expr->GetChildAt(0));
              } else {
                right_exprs.emplace_back(comp_expr->GetChildAt(0));
              }
            }
            if (const auto column_expr = dynamic_cast<ColumnValueExpression *>(comp_expr->GetChildAt(1).get());
                column_expr != nullptr) {
              if (column_expr->GetTupleIdx() == 0) {
                left_exprs.emplace_back(comp_expr->GetChildAt(1));
              } else {
                right_exprs.emplace_back(comp_expr->GetChildAt(1));
              }
            }
          }
          BUSTUB_ASSERT(left_exprs.size() == right_exprs.size() && left_exprs.size() == 2, "error left_exprs size");
          return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                    nlj_plan.GetRightPlan(), std::move(left_exprs),
                                                    std::move(right_exprs), nlj_plan.GetJoinType());
        }
      }
    }

    // (#0.2=#1.1)
    if (const auto comp_expr = dynamic_cast<ComparisonExpression *>(nlj_plan.Predicate().get()); comp_expr != nullptr) {
      if (comp_expr->comp_type_ == ComparisonType::Equal) {
        BUSTUB_ENSURE(comp_expr->children_.size() == 2, "comp_expr should have exactly 2 children.");
        std::vector<AbstractExpressionRef> left_exprs{};
        std::vector<AbstractExpressionRef> right_exprs{};
        if (const auto left_child_column_expr = dynamic_cast<ColumnValueExpression *>(comp_expr->GetChildAt(0).get());
            left_child_column_expr != nullptr) {
          if (const auto right_child_column_expr =
                  dynamic_cast<ColumnValueExpression *>(comp_expr->GetChildAt(1).get());
              right_child_column_expr != nullptr) {
            if (left_child_column_expr->GetTupleIdx() == 0 && right_child_column_expr->GetTupleIdx() == 1) {
              left_exprs.emplace_back(comp_expr->GetChildAt(0));
              right_exprs.emplace_back(comp_expr->GetChildAt(1));
              BUSTUB_ASSERT(left_exprs.size() == right_exprs.size() && left_exprs.size() == 1, "error left_exprs size");
              return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                        nlj_plan.GetRightPlan(), std::move(left_exprs),
                                                        std::move(right_exprs), nlj_plan.GetJoinType());
            }
            if (left_child_column_expr->GetTupleIdx() == 1 && right_child_column_expr->GetTupleIdx() == 0) {
              left_exprs.emplace_back(comp_expr->GetChildAt(1));
              right_exprs.emplace_back(comp_expr->GetChildAt(0));
              BUSTUB_ASSERT(left_exprs.size() == right_exprs.size() && left_exprs.size() == 1, "error left_exprs size");
              return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                        nlj_plan.GetRightPlan(), std::move(left_exprs),
                                                        std::move(right_exprs), nlj_plan.GetJoinType());
            }
          }
        }
      }
    }
  }

  return optimized_plan;
}

}  // namespace bustub
