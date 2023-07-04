#include <sys/types.h>
#include <algorithm>
#include <array>
#include <cfloat>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include "binder/table_ref/bound_join_ref.h"
#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/arithmetic_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/aggregation_plan.h"
#include "execution/plans/delete_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/insert_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/values_plan.h"
#include "optimizer/optimizer.h"
#include "type/limits.h"
#include "type/type.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

// Note for 2023 Spring: You can add all optimizer rule implementations and
// apply the rules as you want in this file. Note that for some test cases, we
// force using starter rules, so that the configuration here won't take effects.
// Starter rule can be forcibly enabled by `set
// force_optimizer_starter_rule=yes`.

namespace bustub {

auto Optimizer::OptimizeCustom(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  /* 优化顺序很重要 */
  auto p = plan;
  {
    // TODO(gukele): 常量折叠需要完善
    p = OptimizeConstantFold(p);
  }

  {
    p = OptimizeEliminateTrueFilter(p);
    p = OptimizeEliminateFalseFilter(p);  // 可能产生空empty ValuesPlan
  }

  {
    // TODO(gukele): 应该在这里将所有空ValuesPlan都merge了
    // TODO(gukele): 应该和OptimizeColumnPruningForAnyType()一样，实现OptimizeRemoveEmptyValues()
    // 既可以避免多次遍历树浪费，同时也无需考虑谁先优化的问题，并且减少了这里所需要调用的接口。
    p = OptimizeRemoveProjectionEmptyValues(p);
    p = OptimizeRemoveNLJEmptyValues(p);
  }

  {
    // TODO(gukele): 实现merge projection + any plan
    p = OptimizeMergeProjection(p);  // project与下层输出完全一样，最多只是换了列的名字时，去除该project
    p = OptimizeEliminateContinuousProjection(p);
  }

  {
    // 目前不会产生新的projection,后续可能会更改
    // 可能对p4 leader board反而负优化
    p = OptimizeColumnPruning(p);
  }

  {
    // filter should be merged or pushed down
    // TODO(gukele): Filter's predicate push down to any plan
    p = OptimizeFilterNLJPredicatePushDown(p);  // push down可能会产生新的Filter,在merge filter之前使用

    // TODO(gukele): merge filter + any plan

    // 应该是冗余的，在OptimizeFilterNLJPredicatePushDown中，相当于所有Filter + NLJ的情况都被merge了
    // p = OptimizeMergeFilterNLJ(p);

    // 谓词filter下推 Filter + Scan -> Scan + Filter
    p = OptimizeMergeFilterScan(p);
  }

  p = OptimizeOrderByAsIndexScan(p);
  p = OptimizeSortLimitAsTopN(p);     // limit + sort -> Top-N
  p = OptimizeSeqScanAsIndexScan(p);  //
  p = OptimizeNLJAsHashJoin(p);       // NLJ -> Hash Join

  // NLJ -> Index Join

  /*
   * p3 Leader board Task (Optional): https://www.zhihu.com/people/Dlee-01/posts
   *   Query 2: Too Many Joins!
   *     Filter + Hash/Index Join -> Hash/Index Join + Filter
   *   Query 3: The Mad Data Scientist
   *     投影消除
   *       1.首先，如果
   * Projection算子要投影的列，跟它的子节点的输出列，是一模一样的，那么投影步骤就是一个无用操作，可以消除。 比如select
   * a,b from t在表 t 里面就正好就是 a b 两列，那就没必要 TableScan上面再做一次 Projection。
   *       2.然后，投影算子下面的子节点，又是另一个投影算子，那么子节点的投影操作就没有意义，可以消除。比如
   *         Projection(A) -> Projection(A,B,C)只需要保留 Projection(A)就够了。
   *       3.类似的，在投影消除规则里面，Aggregation 跟 Projection操作很类似。
   *         因为从 Aggregation 节点出来的都是具体的列，所以 Aggregation(A)-> Projection(A,B,C)中，这个 Projection
   *         也可以消除。
   *     列裁剪 column pruning
   *       列裁剪的算法实现是自顶向下地把算子过一遍。某个节点需要用到的列，等于它自己需要用到的列，加上它的父节点需要用到的列。
   *       可以发现，由上往下的节点，需要用到的列将越来越多。
   *
   *     遇到连续的两个 Projection，合并为 1 个，只取上层 Projection 所需列。
   *     遇到 Projection + Aggregation，改写 aggregates，截取 Projection
   * 中需要的项目，其余直接抛弃。 同样地，我个人认为 Column Pruning
   * 也是自顶向下地改写比较方便。具体实现是收集 Projection 里的所有
   *     column，然后改写下层节点，仅保留上层需要 project 的 column。这里的
   * column 不一定是表中的 column，而是一种广义的 column，例如 SELECT t1.x +
   * t1.y FROM t1 中的 t1.x + t1.y。
   */

  return p;
}

auto Optimizer::OptimizeEliminateContinuousProjection(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeEliminateContinuousProjection(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  /*
   * 1.首先，如果 Projection算子要投影的列，跟它的子节点的输出列，是一模一样的，那么投影步骤就是一个无用操作，可以消除。
   *   比如select a,b from t在表 t 里面就正好就是 a b 两列，那就没必要 TableScan上面再做一次 Projection。
   *   在merge_projection.cpp OptimizeMergeProjection()中实现了
   * 2.然后，投影算子下面的子节点，又是另一个投影算子，那么子节点的投影操作就没有意义，可以消除。比如
   *   Projection(A) -> Projection(A,B,C)只需要保留 Projection(A)就够了。
   * 3.类似的，在投影消除规则里面，Aggregation 跟 Projection操作很类似。
   *   因为从 Aggregation 节点出来的都是具体的列，所以 Aggregation(A)-> Projection(A,B,C)中，这个 Projection
   * 也可以消除。
   */

  // 实现第一层project全是ColumnValueExpression时，消除第二层的project
  // if (optimized_plan->GetType() == PlanType::Projection) {
  //   auto &project_plan = dynamic_cast<ProjectionPlanNode &>(*optimized_plan);
  //   if (auto child_plan = project_plan.GetChildPlan(); child_plan->GetType() == PlanType::Projection) {
  //     auto &child_project_plan = dynamic_cast<const ProjectionPlanNode &>(*child_plan);
  //     auto &project_exprs = project_plan.GetExpressions();
  //     auto &child_project_exprs = child_project_plan.GetExpressions();
  //     // If all items are column value expressions
  //     bool is_all_column_value_expression = true;
  //     for (const auto &project_expr : project_exprs) {
  //       auto column_value_expr = dynamic_cast<const ColumnValueExpression *>(project_expr.get());
  //       if (column_value_expr != nullptr) {
  //         if (column_value_expr->GetTupleIdx() == 0) {
  //           continue;
  //         }
  //       }
  //       is_all_column_value_expression = false;
  //       break;
  //     }
  //     if (is_all_column_value_expression) {
  //       std::vector<AbstractExpressionRef> new_exprs;
  //       for (const auto &expr : project_exprs) {
  //         auto column_value_expr = dynamic_cast<const ColumnValueExpression &>(*expr);
  //         new_exprs.emplace_back(child_project_exprs[column_value_expr.GetColIdx()]);
  //       }
  //       return std::make_shared<ProjectionPlanNode>(project_plan.output_schema_, std::move(new_exprs),
  //                                                   child_project_plan.GetChildPlan());
  //     }
  //   }
  // }

  // 第一层projection中的ColumnValueExpression全替换成第二层projection的expr
  if (optimized_plan->GetType() == PlanType::Projection) {
    auto &project_plan = dynamic_cast<ProjectionPlanNode &>(*optimized_plan);
    if (auto child_plan = project_plan.GetChildPlan(); child_plan->GetType() == PlanType::Projection) {
      auto &child_project_plan = dynamic_cast<const ProjectionPlanNode &>(*child_plan);
      auto &project_exprs = project_plan.GetExpressions();
      auto &child_project_exprs = child_project_plan.GetExpressions();

      std::vector<AbstractExpressionRef> new_exprs;
      new_exprs.reserve(project_exprs.size());
      for (const auto &expr : project_exprs) {
        new_exprs.emplace_back(RewriteExpressionForEliminateContinuousProjection(expr, child_project_exprs));
      }
      return std::make_shared<ProjectionPlanNode>(project_plan.output_schema_, std::move(new_exprs),
                                                  child_project_plan.GetChildPlan());
    }
  }

  // Aggregation(A)-> Projection(A,B,C)中，消除Projection
  // 太复杂了！！！！以后在做
  // if (optimized_plan->GetType() == PlanType::Aggregation) {
  //   auto &aggregation_plan = dynamic_cast<AggregationPlanNode &>(*optimized_plan);
  //   if (auto child_plan = aggregation_plan.GetChildPlan(); child_plan->GetType() == PlanType::Projection) {
  //     const auto &project_plan = dynamic_cast<const ProjectionPlanNode &>(*child_plan);
  //     return AggregationPlanNode()
  //   }
  // }

  return optimized_plan;
}

auto Optimizer::RewriteExpressionForEliminateContinuousProjection(
    const AbstractExpressionRef &expr, const std::vector<AbstractExpressionRef> &children_exprs)
    -> AbstractExpressionRef {
  std::vector<AbstractExpressionRef> children;
  for (const auto &child : expr->GetChildren()) {
    children.emplace_back(RewriteExpressionForEliminateContinuousProjection(child, children_exprs));
  }
  AbstractExpressionRef rewrite_expr = expr->CloneWithChildren(std::move(children));

  if (auto column_value_expr = dynamic_cast<ColumnValueExpression *>(rewrite_expr.get());
      column_value_expr != nullptr) {
    return children_exprs[column_value_expr->GetColIdx()];
  }

  return rewrite_expr;
}

auto Optimizer::OptimizeConstantFold(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeConstantFold(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  switch (optimized_plan->GetType()) {
    case PlanType::Filter: {
      auto &filter_plan = dynamic_cast<FilterPlanNode &>(*optimized_plan);
      auto &expr = filter_plan.predicate_;
      expr = RewriteExpressionForConstantFold(expr);
    } break;
    // TODO(gukele): ValuesPlanNode中都是ConstantValueExpression吧??
    case PlanType::Values: {
      auto &values_plan = dynamic_cast<ValuesPlanNode &>(*optimized_plan);
      auto &exprss = values_plan.values_;
      for (auto &exprs : exprss) {
        for (auto &expr : exprs) {
          expr = RewriteExpressionForConstantFold(expr);
        }
      }
    } break;
    default:
      break;
  }

  return optimized_plan;
}

auto Optimizer::RewriteExpressionForConstantFold(const AbstractExpressionRef &expr) -> AbstractExpressionRef {
  std::vector<AbstractExpressionRef> children;
  for (const auto &child : expr->GetChildren()) {
    children.emplace_back(RewriteExpressionForConstantFold(child));
  }
  AbstractExpressionRef rewrite_expr = expr->CloneWithChildren(std::move(children));

  // 1.常量之间运算
  if (auto arithmetic_expr = dynamic_cast<ArithmeticExpression *>(rewrite_expr.get()); arithmetic_expr != nullptr) {
    auto &children_exprs = arithmetic_expr->GetChildren();
    BUSTUB_ASSERT(children_exprs.size() == 2, "Arithmetic expression should have exactly two children expressions.");
    if (auto left_constant_value_expr = dynamic_cast<ConstantValueExpression *>(arithmetic_expr->GetChildAt(0).get());
        left_constant_value_expr != nullptr) {
      if (auto right_constant_value_expr =
              dynamic_cast<ConstantValueExpression *>(arithmetic_expr->GetChildAt(1).get());
          right_constant_value_expr != nullptr) {
        return std::make_shared<ConstantValueExpression>(arithmetic_expr->Evaluate(nullptr, Schema({})));
      }
    }
  }

  // 2.常量之间比较
  if (auto comparison_expr = dynamic_cast<ComparisonExpression *>(rewrite_expr.get()); comparison_expr != nullptr) {
    auto &children_exprs = comparison_expr->GetChildren();
    BUSTUB_ASSERT(children_exprs.size() == 2, "Comparison expression should have exactly two children expressions.");
    if (auto left_constant_value_expr = dynamic_cast<ConstantValueExpression *>(comparison_expr->GetChildAt(0).get());
        left_constant_value_expr != nullptr) {
      if (auto right_constant_value_expr =
              dynamic_cast<ConstantValueExpression *>(comparison_expr->GetChildAt(1).get());
          right_constant_value_expr != nullptr) {
        return std::make_shared<ConstantValueExpression>(comparison_expr->Evaluate(nullptr, Schema({})));
      }
    }
  }

  // 3.常量bool之间逻辑运算
  if (auto logic_expr = dynamic_cast<LogicExpression *>(rewrite_expr.get()); logic_expr != nullptr) {
    auto &children_exprs = logic_expr->GetChildren();
    BUSTUB_ASSERT(children_exprs.size() == 2, "Logic expression should have exactly two children expressions.");
    auto left_constant_value_expr = dynamic_cast<ConstantValueExpression *>(logic_expr->GetChildAt(0).get());
    auto right_constant_value_expr = dynamic_cast<ConstantValueExpression *>(logic_expr->GetChildAt(1).get());
    // 3.1两侧为常量bool的逻辑运算
    if (left_constant_value_expr != nullptr && right_constant_value_expr != nullptr) {
      return std::make_shared<ConstantValueExpression>(logic_expr->Evaluate(nullptr, Schema({})));
    }
    // 3.2一侧为常量bool的逻辑运算 and false、or true变为ConstantValueExpression，而and true、or false去掉常量bool部分
    ConstantValueExpression *constant_bool_expr = nullptr;
    AbstractExpressionRef non_constant_value_expr;
    bool is_left_a_constant_expr = false;  // 表示左侧表达式是否为常量bool
    if (left_constant_value_expr != nullptr) {
      is_left_a_constant_expr = true;
      constant_bool_expr = left_constant_value_expr;
      non_constant_value_expr = logic_expr->GetChildAt(1);
    } else if (right_constant_value_expr != nullptr) {
      constant_bool_expr = right_constant_value_expr;
      non_constant_value_expr = logic_expr->GetChildAt(0);
    }

    static auto get_bool_as_cmp_bool = [](const Value &val) -> CmpBool {
      if (val.IsNull()) {
        return CmpBool::CmpNull;
      }
      if (val.GetAs<bool>()) {
        return CmpBool::CmpTrue;
      }
      return CmpBool::CmpFalse;
    };

    if (constant_bool_expr != nullptr) {
      auto constant_bool = get_bool_as_cmp_bool(constant_bool_expr->val_);
      auto left_expr = logic_expr->GetChildAt(0);
      auto right_expr = logic_expr->GetChildAt(1);
      switch (logic_expr->logic_type_) {
        case LogicType::And:
          if (constant_bool == CmpBool::CmpFalse || constant_bool == CmpBool::CmpNull) {
            return is_left_a_constant_expr ? left_expr : right_expr;
          }
          if (constant_bool == CmpBool::CmpTrue) {
            return is_left_a_constant_expr ? right_expr : left_expr;
          }
          break;
        case LogicType::Or:
          if (constant_bool == CmpBool::CmpFalse || constant_bool == CmpBool::CmpNull) {
            return is_left_a_constant_expr ? right_expr : left_expr;
          }
          if (constant_bool == CmpBool::CmpTrue) {
            return is_left_a_constant_expr ? left_expr : right_expr;
          }
          break;
        default:
          break;
      }
    }
  }

  // 4.相同ColumnValueExpression比较
  /* BUG(gukele): 因为他可能ColumnValueExpression取得是一个表达式计算出的列，例如：
   * INSERT INTO tmp SELECT * FROM (
   *   SELECT
   *       result.src as src,
   *       graph.dst as dst,
   *       result.src_label as src_label,
   *       graph.dst_label as dst_label,
   *       result.distance + graph.distance as distance,
   *       steps + 1
   *   FROM
   *       result INNER JOIN graph ON result.dst = graph.src
   * ) WHERE distance = distance; -- filter null as we don't have is null func
   * === OPTIMIZER ===
   * Insert { table_oid=24 }
   *   Filter { predicate=(#0.4=#0.4) }
   *     Projection { exprs=[#0.0, #0.7, #0.2, #0.9, (#0.4+#0.10), (#0.5+1)] }
   *       HashJoin { type=Inner, left_key=[#0.1], right_key=[#1.0] }
   *         SeqScan { table=result }
   *         SeqScan { table=graph }
   *
   * 所以只能在filter下面是scan这种情况才能消除相同ColumnValueExpression比较
   *
   */

  // if (auto comparison_expr = dynamic_cast<ComparisonExpression *>(rewrite_expr.get()); comparison_expr != nullptr) {
  //   auto &children_exprs = comparison_expr->GetChildren();
  //   BUSTUB_ASSERT(children_exprs.size() == 2, "Comparison expression should have exactly two children expressions.");
  //   if (auto left_column_value_expr = dynamic_cast<ColumnValueExpression *>(comparison_expr->GetChildAt(0).get());
  //       left_column_value_expr != nullptr) {
  //     if (auto right_column_value_expr = dynamic_cast<ColumnValueExpression *>(comparison_expr->GetChildAt(1).get());
  //         right_column_value_expr != nullptr) {
  //       if (left_column_value_expr->GetColIdx() == right_column_value_expr->GetColIdx() &&
  //       left_column_value_expr->GetTupleIdx() == right_column_value_expr->GetTupleIdx()) {
  //         switch (comparison_expr->comp_type_) {
  //           case ComparisonType::Equal:
  //           case ComparisonType::GreaterThanOrEqual:
  //           case ComparisonType::LessThanOrEqual:
  //             return std::make_shared<ConstantValueExpression>(ValueFactory::GetBooleanValue(true));
  //             break;
  //           case ComparisonType::GreaterThan:
  //           case ComparisonType::LessThan:
  //           case ComparisonType::NotEqual:
  //             return std::make_shared<ConstantValueExpression>(ValueFactory::GetBooleanValue(false));
  //             break;
  //           default:
  //             break;
  //         }
  //       }
  //     }
  //   }
  // }

  return rewrite_expr;
}

auto Optimizer::OptimizeEliminateFalseFilter(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeEliminateFalseFilter(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::Filter) {
    const auto &filter_plan = dynamic_cast<const FilterPlanNode &>(*optimized_plan);

    if (IsPredicateFalse(filter_plan.GetPredicate())) {
      return std::make_shared<ValuesPlanNode>(filter_plan.children_[0]->output_schema_,
                                              std::vector<std::vector<AbstractExpressionRef>>{});
    }
  }

  return optimized_plan;
}

auto Optimizer::IsPredicateFalse(const AbstractExpressionRef &expr) -> bool {
  if (const auto *const_expr = dynamic_cast<const ConstantValueExpression *>(expr.get()); const_expr != nullptr) {
    // FIXME(gukele): || Value.IsNull()?
    return !const_expr->val_.CastAs(TypeId::BOOLEAN).GetAs<bool>();
  }
  return false;
}

auto Optimizer::OptimizeSeqScanAsIndexScan(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSeqScanAsIndexScan(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (plan->GetType() == PlanType::SeqScan) {
    auto &seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);
    if (seq_scan_plan.filter_predicate_ != nullptr) {
      /*
       * // TODO(gukele): 以后实现处理更复杂可以使用索引的情况
       * INDEX UNIQUE SCAN、INDEX RANGE SCAN、INDEX FULL SCAN、INDEX FAST FULL SCAN 、INDEX SKIP SCAN
       * (A > 10 && B < 50) || C > 10) 、 A = 10等逻辑应该使用index
       * x != 10、x = y这种逻辑不应该用index
       *
       *                                       LogicExpression
       *                                       /             \
       *                    ComparisonExpression             ComparisonExpression
       *                     /            \                        /            \
       * ColumnValueExpression   ConstantValueExpression ColumnValueExpression    ConstantValueExpression
       *
       */

      // TODO(gukele): 获取ranges应该单独提出来一个函数
      // FIXME(gukele): 索引区间还是[ )开闭区间比较好，这样double也可以方便找到下一个，用Value null值表示无穷
      auto indexes = catalog_.GetTableIndexes(seq_scan_plan.table_name_);
      if (auto optional_meta = CanUseIndexLookup(seq_scan_plan.filter_predicate_, indexes); optional_meta.has_value()) {
        auto [index_info, column_indexes, constant_values, comp_types] = *optional_meta;
        if (column_indexes.front() == 0) {
          auto constant_value = constant_values.front();
          auto type_id = constant_value.GetTypeId();

          auto last = ValueFactory::GetNullValueByType(type_id);
          auto next = ValueFactory::GetNullValueByType(type_id);
          auto min = Type::GetMinValue(type_id);
          auto max = Type::GetMaxValue(type_id);
          auto one = Value(type_id, 1);

          switch (type_id) {
            case TypeId::TINYINT:
            case TypeId::SMALLINT:
            case TypeId::INTEGER:
            case TypeId::BIGINT:
            case TypeId::TIMESTAMP:
              last = constant_value.CompareLessThan(min) == CmpBool::CmpTrue ? min : constant_value.Subtract(one);
              next = constant_value.CompareGreaterThan(max) == CmpBool::CmpTrue ? max : constant_value.Add(one);
              break;
            case TypeId::DECIMAL:
              throw NotImplementedException("Decimal type not implemented");
              {
                auto value = constant_value.GetAs<double>();
                auto max_double = std::nextafter(value, DBL_MAX);
                next = value == max_double ? constant_value : Value(type_id, max_double);
                // last = ?;
              }
              break;
            case TypeId::VARCHAR:
            case TypeId::BOOLEAN:
              throw NotImplementedException("Other type value not implemented");
            case INVALID:
            default:
              break;
          }

          std::vector<Value> begin;
          std::vector<Value> end;
          switch (comp_types.front()) {
            case ComparisonType::Equal:
              // ranges.emplace_back(constant_value, constant_value);
              begin.emplace_back(constant_value);
              end.emplace_back(constant_value);
              break;
            case ComparisonType::GreaterThan:
              // ranges.emplace_back(next, max);
              begin.emplace_back(next);
              end.emplace_back(max);
              break;
            case ComparisonType::GreaterThanOrEqual:
              // ranges.emplace_back(constant_value, max);
              begin.emplace_back(constant_value);
              end.emplace_back(max);
              break;
            case ComparisonType::LessThan:
              // ranges.emplace_back(min, last);
              begin.emplace_back(min);
              end.emplace_back(last);
              break;
            case ComparisonType::LessThanOrEqual:
              // ranges.emplace_back(min, constant_value);
              begin.emplace_back(min);
              end.emplace_back(constant_value);
              break;
            default:
              break;
          }

          auto &columns = index_info->key_schema_.GetColumns();
          for (std::size_t i = 1; i < columns.size(); ++i) {
            begin.emplace_back(Type::GetMinValue(columns[i].GetType()));
            end.emplace_back(Type::GetMaxValue(columns[i].GetType()));
          }

          std::vector<std::pair<std::vector<Value>, std::vector<Value>>> ranges{{std::move(begin), std::move(end)}};
          // 获取ranges完成

          return std::make_shared<IndexScanPlanNode>(optimized_plan->output_schema_, index_info->index_oid_,
                                                     index_info->name_, constant_values.size(),
                                                     seq_scan_plan.filter_predicate_, std::move(ranges));
        }
      }
    }
  }
  return optimized_plan;
}

auto Optimizer::CanComparisonUseIndexLookup(const AbstractExpressionRef &expr, const std::vector<IndexInfo *> &indexes)
    -> std::optional<std::tuple<IndexInfo *, uint32_t, Value, ComparisonType>> {
  if (auto comparison_expr = dynamic_cast<const ComparisonExpression *>(expr.get());
      comparison_expr != nullptr && comparison_expr->comp_type_ != ComparisonType::NotEqual) {
    auto &children_exprs = comparison_expr->GetChildren();
    BUSTUB_ASSERT(children_exprs.size() == 2, "Comparison expression should have exactly 2 children.");
    auto left_column_value_expr = dynamic_cast<ColumnValueExpression *>(children_exprs[0].get());
    auto right_constant_value_expr = dynamic_cast<ConstantValueExpression *>(children_exprs[1].get());
    // [[maybe_unused]] AbstractExpressionRef column_value_expr_ref;
    // [[maybe_unused]] AbstractExpressionRef constant_value_expr_ref;
    ColumnValueExpression *column_value_expr = nullptr;
    ConstantValueExpression *constant_value_expr = nullptr;
    if (left_column_value_expr != nullptr && right_constant_value_expr != nullptr) {
      // column_value_expr_ref = children_exprs[0];
      // constant_value_expr_ref = children_exprs[1];
      column_value_expr = left_column_value_expr;
      constant_value_expr = right_constant_value_expr;
    }
    auto left_constant_value_expr = dynamic_cast<ConstantValueExpression *>(children_exprs[0].get());
    auto right_column_value_expr = dynamic_cast<ColumnValueExpression *>(children_exprs[1].get());
    if (left_constant_value_expr != nullptr && right_column_value_expr != nullptr) {
      // column_value_expr_ref = children_exprs[1];
      // constant_value_expr_ref = children_exprs[0];
      column_value_expr = right_column_value_expr;
      constant_value_expr = left_constant_value_expr;
    }
    if (column_value_expr != nullptr) {
      auto column_index = column_value_expr->GetColIdx();
      for (const auto &index_info : indexes) {
        auto &key_attrs = index_info->index_->GetKeyAttrs();
        for (std::size_t i = 0; i < key_attrs.size(); ++i) {
          if (key_attrs[i] == column_index) {
            return std::tuple<IndexInfo *, uint32_t, Value, ComparisonType>{
                index_info, i, constant_value_expr->Evaluate(nullptr, Schema({})), comparison_expr->comp_type_};
          }
        }
      }
    }
  }
  return std::nullopt;
}

auto Optimizer::CanUseIndexLookup(const AbstractExpressionRef &expr, const std::vector<IndexInfo *> &indexes)
    -> std::optional<std::tuple<IndexInfo *, std::vector<uint32_t>, std::vector<Value>, std::vector<ComparisonType>>> {
  IndexInfo *index_info;
  std::vector<uint32_t> key_attributes;
  std::vector<Value> values;
  std::vector<ComparisonType> comparison_types;

  if (auto comparison_expr = dynamic_cast<ComparisonExpression *>(expr.get()); comparison_expr != nullptr) {
    if (auto optional_meta = CanComparisonUseIndexLookup(expr, indexes); optional_meta.has_value()) {
      auto [index_info, column_index, constant_value, comparison_type] = *optional_meta;
      key_attributes.push_back(column_index);
      values.push_back(constant_value);
      comparison_types.push_back(comparison_type);
      return std::tuple<IndexInfo *, std::vector<uint32_t>, std::vector<Value>, std::vector<ComparisonType>>{
          index_info, std::move(key_attributes), std::move(values), std::move(comparison_types)};
    }
  }

  if (auto logic_expr = dynamic_cast<LogicExpression *>(expr.get()); logic_expr != nullptr) {
    // 现在只简单的处理and的情况,并且只使用一个索引
    BUSTUB_ASSERT(expr->children_.size() == 2, "Logic expression should have exactly 2 children.");
    // TODO(gukele): call CanUseIndexLookup!
    auto left_comparison_meta = CanComparisonUseIndexLookup(expr->GetChildAt(0), indexes);
    auto right_comparison_meta = CanComparisonUseIndexLookup(expr->GetChildAt(1), indexes);
    if (logic_expr->logic_type_ == LogicType::And) {
      /* // TODO(gukele): 目前只处理and,并且是随便选一个索引
       * and
       *    用同一个索引
       *        不同列 看能否满足最左前缀，不能就选能用第一列的
       *        同一列 区间合并
       *    不用同一索引 检查有没有equal,有则使用equal,没有则随便用一个
       * or
       *   用同一索引
       *        不同列 不能使用索引
       *        同一列 两个区间
       *   不用同一索引 无法使用索引
       *
       *
       */
      if (left_comparison_meta.has_value() || right_comparison_meta.has_value()) {
        auto [left_index_info, left_column_index, left_constant_value, left_comparison_type] = *left_comparison_meta;
        auto [right_index_info, right_column_index, right_constant_value, right_comparison_type] =
            *right_comparison_meta;
        if (left_comparison_meta.has_value() || right_comparison_meta.has_value()) {
          if (left_comparison_meta.has_value()) {
            if (left_column_index == 0) {
              index_info = left_index_info;
              key_attributes.push_back(left_column_index);
              values.push_back(left_constant_value);
              comparison_types.push_back(left_comparison_type);
              return std::tuple<IndexInfo *, std::vector<uint32_t>, std::vector<Value>, std::vector<ComparisonType>>{
                  index_info, std::move(key_attributes), std::move(values), std::move(comparison_types)};
            }
          } else {
            if (right_column_index == 0) {
              index_info = right_index_info;
              key_attributes.push_back(right_column_index);
              values.push_back(right_constant_value);
              comparison_types.push_back(right_comparison_type);
              return std::tuple<IndexInfo *, std::vector<uint32_t>, std::vector<Value>, std::vector<ComparisonType>>{
                  index_info, std::move(key_attributes), std::move(values), std::move(comparison_types)};
            }
          }
        }
      }
    } else {
      throw NotImplementedException("Or logic not implemented");
      BUSTUB_ASSERT(true, "Or logic not implemented");
    }
  }
  return std::nullopt;
}

// auto Optimizer::OptimizeColumnPruningImpl(const AbstractPlanNodeRef &plan, std::unordered_map<uint32_t, uint32_t>
// &father_column_indexes_map) -> AbstractPlanNodeRef {
//   throw ExecutionException("Column pruning not implemented");

//   AbstractPlanNodeRef new_plan_without_children;

//   // 说明plan是根(或者是目前没有实现列裁剪的plan type)，不需要column pruning
//   std::unordered_map<uint32_t, uint32_t> column_indexes_map(father_column_indexes_map);
//   if(father_column_indexes_map.empty()) {
//     for(std::size_t i = 0 ; i < plan->OutputSchema().GetColumnCount() ; ++i) {
//       column_indexes_map.insert({i, i});
//     }
//   }

//   SchemaRef new_schema;
//   std::vector<Column> new_columns;
//   const auto &children = plan->GetChildren();
//   // TODO(gukele): 我们现在只修改Projection，DataSource，Aggregation这三种plan的返回列数
//   switch (plan->GetType()) {
//     case PlanType::Filter:
//       BUSTUB_ASSERT(children.size() == 1, "Filter must have exactly one children");

//       // 根据父亲需要哪些列和自己需要的列，只返回这些列，那么需要修改output schema
//       // 那么此时父亲的expr中的colum value expr如何变化，也就知道了
//       GetColumnIndexes(dynamic_cast<const FilterPlanNode &>(*plan).GetPredicate(), column_indexes_map);
//       {
//         std::size_t i = 0;
//         for(auto &[old_column_index, _] : column_indexes_map) {
//           // new_column_index = i++;
//           if(auto iter = father_column_indexes_map.find(old_column_index); iter != father_column_indexes_map.end()) {
//             iter->second = i;
//           }
//           new_columns.emplace_back(plan->OutputSchema().GetColumn(i));
//           ++i;
//         }
//       }
//       new_schema = std::make_shared<Schema>(new_columns);
//       break;
//     case PlanType::Aggregation:
//       BUSTUB_ASSERT(children.size() == 1, "Aggregation must have exactly one children");

//       const auto &agg_plan = dynamic_cast<const AggregationPlanNode &>(*plan);

//       // group by key
//       for(const auto &expr : agg_plan.GetGroupBys()) {
//         if(auto column_value_expr = dynamic_cast<ColumnValueExpression *>(expr.get()); column_value_expr != nullptr)
//         {
//           // BUSTUB_ASSERT(column_value_expr != nullptr, "Aggregate expr should be ColumnValueExpr");
//           column_indexes_map[column_value_expr->GetColIdx()] = column_value_expr->GetColIdx();
//         }
//       }

//       // 聚合函数去重
//       // std::unordered_map<uint32_t, uint32_t> aggregation_map;
//       const auto &aggregations = agg_plan.GetAggregates();
//       const auto &agg_types = agg_plan.GetAggregateTypes();
//       // FIXME(gukele): unordered_map
//       // 相同AggregationType、使用相同列的aggregation放在一起，方便去重
//       std::map<std::pair<AggregationType, uint32_t>, std::size_t> unique_agg;
//       for(std::size_t i = 0 ; i < aggregations.size() ; ++i) {
//         if(auto column_value_expr = dynamic_cast<ColumnValueExpression *>(aggregations[i].get()); column_value_expr
//         != nullptr) {
//           std::pair<AggregationType, uint32_t> key = {agg_types[i], column_value_expr->GetColIdx()};
//           if(unique_agg.find(key) == unique_agg.end()) {
//             unique_agg[key] = i;
//           }
//         } else {
//           // count(常数) == count(*)
//           throw ExecutionException("111聚合函数中出现非column value expr");
//         }
//       }

//       // 重写聚合函数 和 schema,修改father_column_indexes_map
//       std::vector<AbstractExpressionRef> new_aggregations;
//       {
//         std::size_t new_index = 0;
//         std::unordered_map<uint32_t, std::size_t> new_indexes;
//         for(const auto &[_, unique_old_index] : unique_agg) {
//           new_indexes[unique_old_index] = new_index++;
//           new_aggregations.emplace_back(aggregations[unique_old_index]);
//           new_columns.emplace_back( plan->OutputSchema().GetColumn(unique_old_index));
//         }
//         new_schema = Schema(new_columns);

//         for(auto &[old_index, new_index] : father_column_indexes_map) {
//           if(auto column_value_expr = dynamic_cast<ColumnValueExpression *>(aggregations[old_index].get());
//           column_value_expr != nullptr) {
//             // BUSTUB_ASSERT(column_value_expr != nullptr, "Aggregate expr should be ColumnValueExpr");
//             std::pair<AggregationType, uint32_t> key = {agg_types[old_index], column_value_expr->GetColIdx()};
//             new_index = new_indexes[unique_agg[key]];
//           } else {
//             throw ExecutionException("222聚合函数中出现非column value expr");
//           }
//         }
//       }
//       break;
//     case PlanType::SeqScan:
//     case PlanType::Sort:
//     case PlanType::NestedLoopJoin:
//     default:
//       // 还没实现的裁剪
//       break;
//   }

//   // 利用column_indexes来构造新的

//   // 孩子返回了自己的新column_indexes,自己重写自己的expr,然后才能写啊
//   std::vector<AbstractPlanNodeRef> new_children;
//   for (const auto &child : plan->GetChildren()) {
//     new_children.emplace_back(OptimizeColumnPruningImpl(child, column_indexes_map));
//   }
//   AbstractExpressionRef new_expr;

//   RewriteExpressionForColumnPruning(, column_indexes_map)
//   auto optimize_plan = FilterPlanNode(std::move(new_schema), new_expr, std::move(new_children));

//   new_plan_without_children = plan->CloneWithChildren(std::move(new_children));

//   return new_plan_without_children;
// }

auto Optimizer::OptimizeColumnPruning(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::unordered_map<uint32_t, uint32_t> unused{};
  return OptimizeColumnPruningForAnyType(plan, unused);
}

auto Optimizer::OptimizeColumnPruningForAnyType(const AbstractPlanNodeRef &plan,
                                                std::unordered_map<uint32_t, uint32_t> &father_column_indexes_map)
    -> AbstractPlanNodeRef {
  AbstractPlanNodeRef optimize_plan = plan;
  // 说明plan是根(或者还没实现列裁剪的类型)，不需要裁剪，只对孩子裁剪，最多只需要重写exprs
  if (father_column_indexes_map.empty()) {
    std::unordered_map<uint32_t, uint32_t> all_columns_needed_from_children;
    std::unordered_map<uint32_t, uint32_t> all_columns_needed_from_right_children_for_join;
    // 当父亲为空时，说明自己是根或者是没有实现列裁剪的plan类型，那么我们自己所有的列中所需要的孩子的列就是
    GetAllColumnsNeededFromChildren(optimize_plan, all_columns_needed_from_children);  // 自己不裁剪
    std::vector<AbstractPlanNodeRef> children;
    if (optimize_plan->GetType() != PlanType::NestedLoopJoin) {
      BUSTUB_ASSERT(optimize_plan->GetChildren().size() <= 1, "最多只有一个孩子!");
      for (const auto &child : optimize_plan->GetChildren()) {
        children.emplace_back(OptimizeColumnPruningForAnyType(child, all_columns_needed_from_children));
      }
    } else {
      // 分裂column_indexes_map
      const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimize_plan);
      uint32_t pivot = nlj_plan.GetLeftPlan()->OutputSchema().GetColumnCount();
      for (auto iter = all_columns_needed_from_children.begin(); iter != all_columns_needed_from_children.end();) {
        if (iter->first >= pivot) {
          all_columns_needed_from_right_children_for_join.insert({iter->first - pivot, iter->second - pivot});
          iter = all_columns_needed_from_children.erase(iter);
        } else {
          ++iter;
        }
      }
      children.emplace_back(OptimizeColumnPruningForAnyType(nlj_plan.GetLeftPlan(), all_columns_needed_from_children));
      children.emplace_back(
          OptimizeColumnPruningForAnyType(nlj_plan.GetRightPlan(), all_columns_needed_from_right_children_for_join));
    }

    optimize_plan = optimize_plan->CloneWithChildren(std::move(children));

    // 只有其孩子列裁剪改变了output schema,父亲才需要重写表达式.
    // FIXME(gukele): 目前只处理只有一个孩子的情况

    if (!optimize_plan->GetChildren().empty()) {
      switch (optimize_plan->GetChildAt(0)->GetType()) {
        case PlanType::Aggregation:
        case PlanType::Filter:
        case PlanType::Projection:
          if (optimize_plan->GetType() == PlanType::NestedLoopJoin) {
            optimize_plan = RewriteExpressionForColumnPruning(optimize_plan, all_columns_needed_from_children,
                                                              all_columns_needed_from_right_children_for_join);
          } else {
            optimize_plan = RewriteExpressionForColumnPruning(optimize_plan, all_columns_needed_from_children);
          }
          break;
        case PlanType::SeqScan:
        case PlanType::NestedLoopJoin:
        case PlanType::Sort:
        default:
          // 如果第二个孩子进行了列裁剪，我们同样需要重写expr
          if (optimize_plan->GetChildren().size() > 1 &&
              (optimize_plan->GetChildAt(1)->GetType() == PlanType::Aggregation ||
               optimize_plan->GetChildAt(1)->GetType() == PlanType::Projection ||
               optimize_plan->GetChildAt(1)->GetType() == PlanType::Filter)) {
            if (optimize_plan->GetType() == PlanType::NestedLoopJoin) {
              optimize_plan = RewriteExpressionForColumnPruning(optimize_plan, all_columns_needed_from_children,
                                                                all_columns_needed_from_right_children_for_join);
            }
          }
          break;
      }
    }

    return optimize_plan;
  }

  switch (plan->GetType()) {
    case PlanType::Aggregation:
      optimize_plan = OptimizeColumnPruningForAggregation(plan, father_column_indexes_map);
      break;
    case PlanType::Filter:
      optimize_plan = OptimizeColumnPruningForFilter(plan, father_column_indexes_map);
      break;
    case PlanType::Projection:
      optimize_plan = OptimizeColumnPruningForProjection(plan, father_column_indexes_map);
      break;
    case PlanType::SeqScan:
      // 没有孩子,目前也不裁剪
      optimize_plan = plan;
      break;
    case PlanType::Sort:
    case PlanType::NestedLoopJoin:
    default:
      // // NOTE(gukele): 还没实现，目前不进行列裁剪，但是对孩子进行列裁剪，并且可能重写自己的exprs
      // std::unordered_map<uint32_t, uint32_t> all_column_need_from_children;
      // std::unordered_map<uint32_t, uint32_t> all_column_need_from_children_for_join_right;
      // GetAllColumnsNeededFromChildren(plan, all_column_need_from_children);
      // std::vector<AbstractPlanNodeRef> children;
      // if(plan->GetType() != PlanType::NestedLoopJoin) {
      //   BUSTUB_ASSERT(plan->GetChildren().size() <= 1, "最多只有一个孩子!");
      //   for (const auto &child : plan->GetChildren()) {
      //     children.emplace_back(OptimizeColumnPruningForAnyType(child, all_column_need_from_children));
      //   }
      // } else {
      //   // 分裂column_indexes_map
      //   const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*plan);
      //   uint32_t pivot = nlj_plan.GetLeftPlan()->OutputSchema().GetColumnCount();
      //   for(auto iter = all_column_need_from_children.begin() ; iter != all_column_need_from_children.end() ; ) {
      //     if(iter->first >= pivot) {
      //       all_column_need_from_children_for_join_right.insert({iter->first - pivot, iter->second});
      //       iter = all_column_need_from_children.erase(iter);
      //     } else {
      //       ++iter;
      //     }
      //   }
      //   children.emplace_back(OptimizeColumnPruningForAnyType(nlj_plan.GetLeftPlan(),
      //   all_column_need_from_children));
      //   children.emplace_back(OptimizeColumnPruningForAnyType(nlj_plan.GetRightPlan(),
      //   all_column_need_from_children_for_join_right));
      // }
      // optimize_plan = plan->CloneWithChildren(std::move(children));

      // // 只有其孩子列裁剪改变了output schema,父亲才需要重写表达式.
      // // FIXME(gukele): 目前只处理只有一个孩子的情况
      // if (!optimize_plan->GetChildren().empty()) {
      //   switch (optimize_plan->GetChildAt(0)->GetType()) {
      //     case PlanType::Aggregation:
      //     case PlanType::Filter:
      //     case PlanType::Projection:
      //       if(optimize_plan->GetType() == PlanType::NestedLoopJoin) {
      //         optimize_plan = RewriteExpressionForColumnPruning(optimize_plan, all_column_need_from_children,
      //         all_column_need_from_children_for_join_right);
      //       } else {
      //         optimize_plan = RewriteExpressionForColumnPruning(optimize_plan, all_column_need_from_children);
      //       }
      //       break;
      //     case PlanType::SeqScan:
      //     case PlanType::NestedLoopJoin:
      //     case PlanType::Sort:
      //     default:
      //       // 如果第二个孩子进行了列裁剪，我们同样需要重写expr
      //       if(optimize_plan->GetChildren().size() > 1 &&
      //         (optimize_plan->GetChildAt(1)->GetType() == PlanType::Aggregation ||
      //          optimize_plan->GetChildAt(1)->GetType() == PlanType::Projection ||
      //          optimize_plan->GetChildAt(1)->GetType() == PlanType::Filter)) {
      //            if(optimize_plan->GetType() == PlanType::NestedLoopJoin) {
      //              optimize_plan = RewriteExpressionForColumnPruning(optimize_plan, all_column_need_from_children,
      //              all_column_need_from_children_for_join_right);
      //            }
      //          }
      //       break;
      //   }
      // }
      std::unordered_map<uint32_t, uint32_t> unused;
      optimize_plan = OptimizeColumnPruningForAnyType(plan, unused);
      break;
  }

  return optimize_plan;
}

auto Optimizer::OptimizeColumnPruningForFilter(const AbstractPlanNodeRef &plan,
                                               std::unordered_map<uint32_t, uint32_t> &father_column_indexes_map)
    -> AbstractPlanNodeRef {
  BUSTUB_ASSERT(plan->GetType() == PlanType::Filter && !father_column_indexes_map.empty(),
                "Plan should be Filter and father_column_indexes_map should not empty!");
  if (father_column_indexes_map.size() == plan->OutputSchema().GetColumnCount()) {
    // plan不需要裁剪
    std::unordered_map<uint32_t, uint32_t> unused;
    return OptimizeColumnPruningForAnyType(plan, unused);
  }

  const auto &filter_plan = dynamic_cast<const FilterPlanNode &>(*plan);
  // 根据父亲需要哪些列和自己需要的列，只返回这些列，那么需要修改output schema
  // 那么此时父亲的expr中的colum value expr如何变化，也就知道了
  auto column_indexes_map = father_column_indexes_map;
  GetColumnIndexes(filter_plan.GetPredicate(), column_indexes_map);

  // std::size_t i = 0;
  // FIXME(gukele): 应该是做一个类似稳定排序的稳定一样，列裁剪后保持之前的相对顺序
  // for (auto &[old_column_index, _] : column_indexes_map) {
  //   if (auto iter = father_column_indexes_map.find(old_column_index); iter != father_column_indexes_map.end()) {
  //     iter->second = i;
  //   }
  //   new_columns.emplace_back(plan->OutputSchema().GetColumn(old_column_index));
  //   ++i;
  // }

  // Filter本身是不做裁剪的！！只有其孩子做裁剪，自己schema和孩子schema一样
  std::vector<uint32_t> old_indexes;  // 为了稳定性，保持列裁剪后相对位置不改变
  old_indexes.reserve(column_indexes_map.size());
  for (auto &[old_index, _] : column_indexes_map) {
    old_indexes.emplace_back(old_index);
  }
  std::sort(old_indexes.begin(), old_indexes.end());
  std::vector<Column> new_columns;
  new_columns.reserve(column_indexes_map.size());
  for (std::size_t i = 0; i < old_indexes.size(); ++i) {
    if (auto iter = father_column_indexes_map.find(old_indexes[i]); iter != father_column_indexes_map.end()) {
      iter->second = i;
    }
    new_columns.emplace_back(plan->OutputSchema().GetColumn(old_indexes[i]));
  }

  auto new_schema = std::make_shared<Schema>(new_columns);
  BUSTUB_ASSERT(new_schema->GetColumnCount() == column_indexes_map.size(),
                "filter不进行裁剪，自己schema和孩子schema一样");

  auto new_child = OptimizeColumnPruningForAnyType(filter_plan.GetChildPlan(), column_indexes_map);

  // 孩子修改完column_indexes_map才能重写表达式
  auto new_expr = RewriteExpressionForColumnPruning(filter_plan.GetPredicate(), column_indexes_map);

  // return std::make_shared<FilterPlanNode>(std::move(new_schema), std::move(new_expr), std::move(new_child));
  return std::make_shared<FilterPlanNode>(std::move(new_schema), std::move(new_expr), std::move(new_child));
}

auto Optimizer::OptimizeColumnPruningForProjection(const AbstractPlanNodeRef &plan,
                                                   std::unordered_map<uint32_t, uint32_t> &father_column_indexes_map)
    -> AbstractPlanNodeRef {
  BUSTUB_ASSERT(plan->GetType() == PlanType::Projection && !father_column_indexes_map.empty(),
                "Plan should be Projection and father_column_indexes_map should not empty!");
  const auto &projection_plan = dynamic_cast<const ProjectionPlanNode &>(*plan);

  // 父亲需要我的第几列，映射到我孩子的第几列
  std::unordered_map<uint32_t, uint32_t> column_indexes_map;
  for (const auto &[index, _] : father_column_indexes_map) {
    // BUSTUB_ASSERT(dynamic_cast<ConstantValueExpression *>(projection_plan.GetExpressions()[index].get()) == nullptr,
    //               "projection expr 出现了constant value expr");
    if (auto constant_expr = dynamic_cast<ConstantValueExpression *>(projection_plan.GetExpressions()[index].get());
        constant_expr != nullptr) {
    } else {
      GetColumnIndexes(projection_plan.GetExpressions()[index], column_indexes_map);
    }
  }

  // 修改father_column_indexes_map、裁剪schema、裁剪exprs
  std::vector<Column> new_columns;
  new_columns.reserve(father_column_indexes_map.size());
  std::vector<AbstractExpressionRef> new_exprs;
  new_exprs.reserve(father_column_indexes_map.size());
  // std::size_t i = 0;
  // FIXME(gukele): 应该是做一个类似稳定排序的稳定一样，列裁剪后保持之前的相对顺序,不想改成红黑树map
  // for (auto &[old_column_index, new_column_index] : father_column_indexes_map) {
  //   new_column_index = i++;
  //   new_columns.emplace_back(projection_plan.OutputSchema().GetColumn(old_column_index));
  // }
  std::vector<uint32_t> old_indexes;  // 为了稳定性，保持列裁剪后相对位置不改变
  old_indexes.reserve(column_indexes_map.size());
  for (auto &[old_index, _] : father_column_indexes_map) {
    old_indexes.emplace_back(old_index);
  }
  std::sort(old_indexes.begin(), old_indexes.end());
  for (std::size_t i = 0; i < old_indexes.size(); ++i) {
    father_column_indexes_map[old_indexes[i]] = i;
    new_columns.emplace_back(projection_plan.OutputSchema().GetColumn(old_indexes[i]));
    new_exprs.emplace_back(projection_plan.GetExpressions()[old_indexes[i]]);
  }

  auto new_schema = std::make_shared<Schema>(new_columns);

  auto new_child = OptimizeColumnPruningForAnyType(projection_plan.GetChildPlan(), column_indexes_map);

  // 孩子修改完column_indexes_map才能重写表达式
  for (auto &expr : new_exprs) {
    expr = (RewriteExpressionForColumnPruning(expr, column_indexes_map));
  }

  return std::make_shared<ProjectionPlanNode>(std::move(new_schema), std::move(new_exprs), std::move(new_child));
}

auto Optimizer::OptimizeColumnPruningForAggregation(const AbstractPlanNodeRef &plan,
                                                    std::unordered_map<uint32_t, uint32_t> &father_column_indexes_map)
    -> AbstractPlanNodeRef {
  BUSTUB_ASSERT(plan->GetType() == PlanType::Aggregation && !father_column_indexes_map.empty(),
                "Plan should be Aggregation and father_column_indexes_map should not empty!");
  const auto &agg_plan = dynamic_cast<const AggregationPlanNode &>(*plan);
  // auto column_indexes_map = father_column_indexes_map;

  // 添加父亲所需聚合函数，同时找出父亲所需要的group by key
  // TODO(gukele): 只保留父亲需要的group by key
  std::unordered_map<uint32_t, uint32_t> column_indexes_map;
  // std::unordered_map<uint32_t, uint32_t> group_bys_of_father_need;
  const auto &aggregations = agg_plan.GetAggregates();
  const auto &group_bys = agg_plan.GetGroupBys();
  // NOTE(gukele): bug?明明是复制的map的key,但是index却是const uint32_t
  // const int a = 10;
  // auto b = a;  // a是const int, b是int
  for (auto [index, _] : father_column_indexes_map) {
    // BUSTUB_ASSERT(dynamic_cast<ConstantValueExpression *>(projection_plan.GetExpressions()[index].get()) == nullptr,
    // "projection expr 出现了constant value expr"); output schema 是 (group bys + aggregations)
    if (index >= group_bys.size()) {  // 不需要现在计算key,因为key是本plan node一定需要，不管父节点是否需要
      BUSTUB_ASSERT(aggregations.size() > index - group_bys.size(), "应该大于");
      GetColumnIndexes(aggregations[index - group_bys.size()], column_indexes_map);
    } else {
      // GetColumnIndexes(group_bys[index], group_bys_of_father_need);
    }
  }

  // 聚合函数去重
  // std::unordered_map<uint32_t, uint32_t> aggregation_map;
  const auto &agg_types = agg_plan.GetAggregateTypes();
  // FIXME(gukele): unordered_map
  // 相同AggregationType、使用相同列的aggregation放在一起，方便去重
  std::map<std::pair<AggregationType, uint32_t>, std::size_t> unique_agg;
  std::vector<std::size_t> constant_agg;
  for (std::size_t i = 0; i < aggregations.size(); ++i) {
    if (auto column_value_expr = dynamic_cast<ColumnValueExpression *>(aggregations[i].get());
        column_value_expr != nullptr) {
      std::pair<AggregationType, uint32_t> key = {agg_types[i], column_value_expr->GetColIdx()};
      if (unique_agg.find(key) == unique_agg.end()) {
        unique_agg[key] = i;
      }
    } else if (auto constant_value_expr = dynamic_cast<ConstantValueExpression *>(aggregations[i].get());
               constant_value_expr != nullptr) {
      // 聚合函数中出现常数的情况 count(常数) == count(*) == count(1)
      constant_agg.emplace_back(i);
    } else if (auto arithmetic_expr = dynamic_cast<ArithmeticExpression *>(aggregations[i].get());
               arithmetic_expr != nullptr) {
      // TODO(gukele): 目前只处理聚合函数中出现一个列或者count(*、常数)的情况,
      // 如果出现sum(#0.0 + #0.3)类似的情况先不处理，不进行裁剪了
      std::unordered_map<uint32_t, uint32_t> unused;
      return OptimizeColumnPruningForAnyType(plan, unused);
    } else {
      throw ExecutionException("聚合函数中出现 非column value expr、非constant value expr、非arithmetic expr");
    }
  }

  // group by key
  for (const auto &expr : group_bys) {
    // if(auto column_value_expr = dynamic_cast<ColumnValueExpression *>(expr.get()); column_value_expr != nullptr) {
    //   // BUSTUB_ASSERT(column_value_expr != nullptr, "Aggregate expr should be ColumnValueExpr");
    //   column_indexes_map[column_value_expr->GetColIdx()] = column_value_expr->GetColIdx();
    // } else {
    //   throw ExecutionException("Group by non-column value expr");
    // }
    GetColumnIndexes(expr, column_indexes_map);
  }

  // 重写agg_types、重写schema(group bys + aggregations)
  std::vector<AggregationType> new_agg_types;
  new_agg_types.reserve(unique_agg.size() + constant_agg.size());
  const auto &columns = agg_plan.OutputSchema().GetColumns();
  // group bys不变
  std::vector<Column> new_columns(columns.begin(), columns.begin() + group_bys.size());
  BUSTUB_ASSERT(new_columns.size() == group_bys.size(), "should same!");
  new_columns.reserve(group_bys.size() + unique_agg.size() + constant_agg.size());
  std::vector<AbstractExpressionRef> new_aggregations;
  new_aggregations.reserve(unique_agg.size() + constant_agg.size());
  std::unordered_map<uint32_t, std::size_t> new_indexes;
  std::size_t new_index = 0;

  // for (const auto &[_, unique_old_index] : unique_agg) {
  //   new_indexes[unique_old_index] = new_index++;
  //   new_agg_types.emplace_back(agg_types[unique_old_index]);
  //   new_columns.emplace_back(columns[unique_old_index + group_bys.size()]);
  // }
  // for (const auto &constant_old_index : constant_agg) {
  //   new_indexes[constant_old_index] = new_index++;
  //   new_agg_types.emplace_back(agg_types[constant_old_index]);
  //   new_columns.emplace_back(columns[constant_old_index + group_bys.size()]);
  // }

  // FIXME(gukele): 应该是做一个类似稳定排序的稳定一样，去重后保持之前的相对顺序
  std::vector<std::size_t> old_indexes;
  old_indexes.reserve(unique_agg.size() + constant_agg.size());
  for (const auto &[_, unique_old_index] : unique_agg) {
    old_indexes.emplace_back(unique_old_index);
  }
  for (const auto &constant_old_index : constant_agg) {
    old_indexes.emplace_back(constant_old_index);
  }
  std::sort(old_indexes.begin(), old_indexes.end());

  for (const auto &old_index : old_indexes) {
    new_indexes[old_index] = group_bys.size() + new_index++;
    new_agg_types.emplace_back(agg_types[old_index]);
    new_columns.emplace_back(columns[old_index + group_bys.size()]);
    new_aggregations.emplace_back(aggregations[old_index]);
  }

  auto new_schema = std::make_shared<Schema>(new_columns);

  BUSTUB_ASSERT(new_schema->GetColumns().size() == group_bys.size() + unique_agg.size() + constant_agg.size(),
                "size same!");

  // 修改father_column_indexes_map
  for (auto &[old_index, new_index] : father_column_indexes_map) {
    if (old_index >= group_bys.size()) {  // 聚合函数产生的列才需要变
      auto agg_index = old_index - group_bys.size();
      if (auto column_value_expr = dynamic_cast<ColumnValueExpression *>(aggregations[agg_index].get());
          column_value_expr != nullptr) {
        if (column_value_expr != nullptr) {
          // BUSTUB_ASSERT(column_value_expr != nullptr, "Aggregate expr should be ColumnValueExpr");
          std::pair<AggregationType, uint32_t> key = {agg_types[agg_index], column_value_expr->GetColIdx()};
          new_index = new_indexes[unique_agg[key]];
        } else {
          // throw ExecutionException("222聚合函数中出现非column value expr");
          new_index = new_indexes[old_index];
        }
      }
    }
  }

  auto new_child = OptimizeColumnPruningForAnyType(agg_plan.GetChildPlan(), column_indexes_map);

  // 孩子修改完column_indexes_map才能重写表达式
  // 重写group by
  std::vector<AbstractExpressionRef> new_group_bys;
  new_group_bys.reserve(group_bys.size());
  for (const auto &group_by : group_bys) {
    new_group_bys.emplace_back(RewriteExpressionForColumnPruning(group_by, column_indexes_map));
  }
  // 重写aggregations
  for (auto &agg : new_aggregations) {
    agg = RewriteExpressionForColumnPruning(agg, column_indexes_map);
  }

  return std::make_shared<AggregationPlanNode>(std::move(new_schema), std::move(new_child), std::move(new_group_bys),
                                               std::move(new_aggregations), std::move(new_agg_types));
}

auto Optimizer::RewriteExpressionForColumnPruning(
    const AbstractPlanNodeRef &plan, const std::unordered_map<uint32_t, uint32_t> &indexes_map,
    const std::unordered_map<uint32_t, uint32_t> &indexes_map_for_join_right) -> AbstractPlanNodeRef {
  switch (plan->GetType()) {
    case PlanType::Aggregation: {
      const auto &agg_plan = dynamic_cast<const AggregationPlanNode &>(*plan);
      const auto &group_bys = agg_plan.GetGroupBys();
      const auto &aggregations = agg_plan.GetAggregates();
      std::vector<AbstractExpressionRef> new_group_bys;
      // 重写group_bys
      new_group_bys.reserve(group_bys.size());
      for (const auto &group_by : group_bys) {
        new_group_bys.emplace_back(RewriteExpressionForColumnPruning(group_by, indexes_map));
      }
      // 重写aggregations
      std::vector<AbstractExpressionRef> new_aggregations;
      new_aggregations.reserve(aggregations.size());
      for (const auto &agg : aggregations) {
        new_aggregations.emplace_back(RewriteExpressionForColumnPruning(agg, indexes_map));
      }
      return std::make_shared<AggregationPlanNode>(agg_plan.output_schema_, agg_plan.GetChildPlan(),
                                                   std::move(new_group_bys), std::move(new_aggregations),
                                                   agg_plan.GetAggregateTypes());
    } break;
    case PlanType::Filter: {
      const auto &filter_plan = dynamic_cast<const FilterPlanNode &>(*plan);
      auto new_expr = RewriteExpressionForColumnPruning(filter_plan.GetPredicate(), indexes_map);
      return std::make_shared<FilterPlanNode>(filter_plan.output_schema_, std::move(new_expr),
                                              filter_plan.GetChildPlan());
    } break;
    case PlanType::Projection: {
      const auto &projection_plan = dynamic_cast<const ProjectionPlanNode &>(*plan);
      std::vector<AbstractExpressionRef> new_exprs;
      for (const auto &expr : projection_plan.GetExpressions()) {
        new_exprs.emplace_back(RewriteExpressionForColumnPruning(expr, indexes_map));
      }
      return std::make_shared<ProjectionPlanNode>(projection_plan.output_schema_, std::move(new_exprs),
                                                  projection_plan.GetChildPlan());
    } break;
    case PlanType::Sort: {
      const auto &sort_plan = dynamic_cast<const SortPlanNode &>(*plan);
      const auto &order_bys = sort_plan.GetOrderBy();
      std::vector<std::pair<OrderByType, AbstractExpressionRef>> new_order_bys;
      new_order_bys.reserve(order_bys.size());
      for (const auto &[order_by_type, order_by] : order_bys) {
        new_order_bys.emplace_back(order_by_type, RewriteExpressionForColumnPruning(order_by, indexes_map));
      }
      return std::make_shared<SortPlanNode>(sort_plan.output_schema_, sort_plan.GetChildPlan(),
                                            std::move(new_order_bys));
    } break;
    case PlanType::NestedLoopJoin: {
      const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*plan);
      auto new_predicate =
          RewriteExpressionForColumnPruningJoin(nlj_plan.Predicate(), indexes_map, indexes_map_for_join_right);
      return std::make_shared<NestedLoopJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                      nlj_plan.GetRightPlan(), std::move(new_predicate),
                                                      nlj_plan.GetJoinType());
    } break;
    case PlanType::Insert:
    case PlanType::Delete:
    case PlanType::Update:
    case PlanType::SeqScan:
    case PlanType::MockScan:
      // 没有exprs的plan
      return plan;
      break;
    default:
      throw NotImplementedException(std::string("Rewriting expressions for ") +
                                    std::to_string(static_cast<int64_t>(plan->GetType())) +
                                    " type plan not implemented!");
      break;
  }
}

auto Optimizer::RewriteExpressionForColumnPruning(const AbstractExpressionRef &expr,
                                                  const std::unordered_map<uint32_t, uint32_t> &indexes_map)
    -> AbstractExpressionRef {
  std::vector<AbstractExpressionRef> children;
  for (const auto &child : expr->GetChildren()) {
    children.emplace_back(RewriteExpressionForColumnPruning(child, indexes_map));
  }
  AbstractExpressionRef rewrite_expr;
  if (auto column_value_expr = dynamic_cast<ColumnValueExpression *>(expr.get()); column_value_expr != nullptr) {
    BUSTUB_ASSERT(indexes_map.find(column_value_expr->GetColIdx()) != indexes_map.end(),
                  "Impossible, indexes should have all column indexes of expr");
    if (auto iter = indexes_map.find(column_value_expr->GetColIdx()); iter->second == column_value_expr->GetColIdx()) {
      rewrite_expr = expr->CloneWithChildren(std::move(children));
    } else {
      rewrite_expr = std::make_shared<ColumnValueExpression>(column_value_expr->GetTupleIdx(),
                                                             indexes_map.find(column_value_expr->GetColIdx())->second,
                                                             column_value_expr->GetReturnType());
    }
  } else {
    rewrite_expr = expr->CloneWithChildren(std::move(children));
  }
  return rewrite_expr;
}

auto Optimizer::RewriteExpressionForColumnPruningJoin(const AbstractExpressionRef &expr,
                                                      const std::unordered_map<uint32_t, uint32_t> &left_indexes_map,
                                                      const std::unordered_map<uint32_t, uint32_t> &right_indexes_map)
    -> AbstractExpressionRef {
  std::vector<AbstractExpressionRef> children;
  for (const auto &child : expr->GetChildren()) {
    children.emplace_back(RewriteExpressionForColumnPruningJoin(child, left_indexes_map, right_indexes_map));
  }
  AbstractExpressionRef rewrite_expr;
  if (auto column_value_expr = dynamic_cast<ColumnValueExpression *>(expr.get()); column_value_expr != nullptr) {
    const std::unordered_map<uint32_t, uint32_t> *indexes_map = nullptr;
    if (column_value_expr->GetTupleIdx() == 0) {
      indexes_map = &left_indexes_map;
    } else {
      indexes_map = &right_indexes_map;
    }
    BUSTUB_ASSERT(indexes_map->find(column_value_expr->GetColIdx()) != indexes_map->end(),
                  "Impossible, indexes should have all column indexes of expr");
    rewrite_expr = std::make_shared<ColumnValueExpression>(column_value_expr->GetTupleIdx(),
                                                           indexes_map->find(column_value_expr->GetColIdx())->second,
                                                           column_value_expr->GetReturnType());
  } else {
    rewrite_expr = expr->CloneWithChildren(std::move(children));
  }
  return rewrite_expr;
}

void Optimizer::GetAllColumnsNeededFromChildren(const AbstractPlanNodeRef &plan,
                                                std::unordered_map<uint32_t, uint32_t> &column_indexes_map) const {
  switch (plan->GetType()) {
    case PlanType::Aggregation: {
      const auto &agg_plan = dynamic_cast<const AggregationPlanNode &>(*plan);
      for (const auto &agg : agg_plan.GetAggregates()) {
        GetColumnIndexes(agg, column_indexes_map);
      }
      for (const auto &group_by : agg_plan.GetGroupBys()) {
        GetColumnIndexes(group_by, column_indexes_map);
      }
    } break;
    case PlanType::Projection: {
      // 作为根时，自己的每一列都需要。同时自己每一列映射到需要孩子的第几列
      const auto &projection_plan = dynamic_cast<const ProjectionPlanNode &>(*plan);
      for (const auto &expr : projection_plan.GetExpressions()) {
        GetColumnIndexes(expr, column_indexes_map);
      }
    } break;
    case PlanType::Filter:
    // {
    //   const auto &filter_plan = dynamic_cast<const FilterPlanNode &>(*plan);
    //   for (std::size_t i = 0; i < filter_plan.OutputSchema().GetColumnCount(); ++i) {
    //     column_indexes_map.insert({i, i});
    //   }
    // } break;
    case PlanType::Sort:
      // {
      //   const auto &sort_plan = dynamic_cast<const SortPlanNode &>(*plan);
      //   const auto &order_bys = sort_plan.GetOrderBy();
      //   for(const auto &[_, order_by] : order_bys) {
      //     GetColumnIndexes(order_by, column_indexes_map);
      //   }
      // }
      // break;

      // NOTE(gukele): 作为根时自己有多少列就返回多少列，不根据父亲所需进行裁剪
      BUSTUB_ASSERT(plan->GetChildren().size() == 1, "Must has exactly one child!");
      for (std::size_t i = 0; i < plan->OutputSchema().GetColumnCount(); ++i) {
        column_indexes_map.insert({i, i});
      }
    case PlanType::Insert:
    case PlanType::Delete:
    case PlanType::Update:
      // 孩子给多少列，就需要多少列，不让孩子进行裁剪
      for (std::size_t i = 0; i < plan->GetChildAt(0)->OutputSchema().GetColumnCount(); ++i) {
        column_indexes_map.insert({i, i});
      }
      break;
    case PlanType::MockScan:
      BUSTUB_ENSURE(true, "mock scan 只是为了测试!");
      break;
    case PlanType::SeqScan:
      // 这些类型暂时没有完成列裁剪，并且他们也没有孩子
      // std::cout << static_cast<int>(plan->GetType()) << " type plan 不需要裁剪！" << std::endl;
      BUSTUB_ENSURE(true, "以后再完成seq scan裁剪!");
      break;
    case PlanType::NestedLoopJoin: {
      // 暂时还没完成列裁剪的
      BUSTUB_ASSERT(plan->GetChildren().size() == 2, "Must has exactly two children!");
      uint32_t index = 0;
      for (const auto &child : plan->GetChildren()) {
        for (std::size_t i = 0; i < child->OutputSchema().GetColumnCount(); ++i) {
          column_indexes_map.insert({index, index});
          ++index;
        }
      }
    } break;
    default:
      // 暂时还没完成列裁剪的，我们需要孩子的所有列，output schema应该与孩子的相同
      BUSTUB_ASSERT(plan->GetChildren().size() <= 1, "At most one child!");
      if (!plan->GetChildren().empty()) {
        for (std::size_t i = 0; i < plan->GetChildAt(0)->OutputSchema().GetColumnCount(); ++i) {
          column_indexes_map.insert({i, i});
        }
      }
      // std::cout << static_cast<int>(plan->GetType()) << " type plan 还未实现裁剪！" << std::endl;
      // throw NotImplementedException(std::string("Getting column indexes for ") +
      // std::to_string(static_cast<int64_t>(plan->GetType())) +
      //                          " type plans not implemented!");
      break;
  }
}

void Optimizer::GetColumnIndexes(const AbstractExpressionRef &expr,
                                 std::unordered_map<uint32_t, uint32_t> &column_indexes_map) const {
  if (auto column_value_expr = dynamic_cast<ColumnValueExpression *>(expr.get()); column_value_expr != nullptr) {
    column_indexes_map[column_value_expr->GetColIdx()] = column_value_expr->GetColIdx();
  }
  for (auto &child : expr->GetChildren()) {
    GetColumnIndexes(child, column_indexes_map);
  }
}

auto Optimizer::OptimizeFilterNLJPredicatePushDown(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // 应该是自顶向下
  AbstractPlanNodeRef optimized_plan = plan;

  if (optimized_plan->GetType() == PlanType::Filter) {
    auto &filter_plan = dynamic_cast<const FilterPlanNode &>(*optimized_plan);
    if (filter_plan.GetChildPlan()->GetType() == PlanType::NestedLoopJoin) {
      auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*filter_plan.GetChildPlan());
      auto rewrite_expr =
          RewriteExpressionForJoin(filter_plan.GetPredicate(), nlj_plan.GetLeftPlan()->OutputSchema().GetColumnCount(),
                                   nlj_plan.GetRightPlan()->OutputSchema().GetColumnCount());
      // FIXME(gukele): 目前只处理只有and logic的情况
      if (!HasOrLogic(rewrite_expr)) {
        auto [left_filter_expr, right_filter_expr, join_expr] =
            RewriteExpressionForFilterPushDown(rewrite_expr);  // 返回左右孩子Plan的expression和自身的

        auto new_left_children_plan = nlj_plan.GetLeftPlan();
        if (left_filter_expr) {
          // 加一层filter
          new_left_children_plan = std::make_shared<FilterPlanNode>(nlj_plan.GetLeftPlan()->output_schema_,
                                                                    left_filter_expr, nlj_plan.GetLeftPlan());
        }
        auto new_right_children_plan = nlj_plan.GetRightPlan();
        if (right_filter_expr) {
          // 加一层filter
          new_right_children_plan = std::make_shared<FilterPlanNode>(nlj_plan.GetRightPlan()->output_schema_,
                                                                     right_filter_expr, nlj_plan.GetRightPlan());
        }

        // std::cout << "left filter : \n" << left_filter->ToString() << "\n" << std::endl;
        // std::cout << "right filter : \n" << right_filter->ToString() << "\n" << std::endl;
        // std::cout << "NLJ : \n" << OptimizeFilterNLJPushDown(left_filter)->ToString() << "\n" << std::endl;
        // std::cout << "NLJ : \n" << OptimizeFilterNLJPushDown(right_filter)->ToString() << "\n" << std::endl;

        // OptimizeFilterNLJPushDown(left_filter);
        // OptimizeFilterNLJPushDown(right_filter);

        // optimized_plan = std::make_shared<NestedLoopJoinPlanNode>(filter_plan.output_schema_,
        // OptimizeFilterNLJPushDown(left_filter), OptimizeFilterNLJPushDown(right_filter), join_expr,
        // nlj_plan.GetJoinType());
        if (IsPredicateTrue(nlj_plan.Predicate())) {  // filter 谓词中有join predicate
          optimized_plan =
              std::make_shared<NestedLoopJoinPlanNode>(filter_plan.output_schema_, new_left_children_plan,
                                                       new_right_children_plan, join_expr, nlj_plan.GetJoinType());
        } else {  // filter 谓词中没有join predicate
          BUSTUB_ASSERT(!join_expr, "Expression should not have join predicate!");
          optimized_plan = std::make_shared<NestedLoopJoinPlanNode>(filter_plan.output_schema_, new_left_children_plan,
                                                                    new_right_children_plan, nlj_plan.Predicate(),
                                                                    nlj_plan.GetJoinType());
        }
        // std::cout << "NLJ : \n" << optimized_plan->ToString() << "\n" << std::endl;
      }
    }
  }

  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : optimized_plan->GetChildren()) {
    children.emplace_back(OptimizeFilterNLJPredicatePushDown(child));
  }
  optimized_plan = optimized_plan->CloneWithChildren(std::move(children));

  return optimized_plan;
}
// CREATE TABLE t4(x int, y int); CREATE TABLE t5(x int, y int); CREATE TABLE t6(x int, y int);
// EXPLAIN SELECT * FROM t4, t5, t6 WHERE(t4.x = t5.x) AND (t5.y = t6.y) AND (t4.y >= 1000000) AND (t4.y < 1500000) AND
// (t6.x >= 100000) AND (t6.x < 150000);

auto Optimizer::HasOrLogic(const AbstractExpressionRef &expr) -> bool {
  if (auto logic_expr = dynamic_cast<LogicExpression *>(expr.get()); logic_expr != nullptr) {
    if (logic_expr->logic_type_ == LogicType::Or) {
      return true;
    }
  }

  for (auto &children_expr : expr->GetChildren()) {
    if (HasOrLogic(children_expr)) {
      return true;
    }
  }
  auto &children_exprs = expr->GetChildren();
  return static_cast<bool>(std::any_of(children_exprs.cbegin(), children_exprs.cend(), HasOrLogic));
}

void Optimizer::GetAllAndLogicChildren(const AbstractExpressionRef &expr,
                                       std::vector<AbstractExpressionRef> &result_exprs) {
  BUSTUB_ASSERT(expr->GetReturnType() == TypeId::BOOLEAN, "Expression should not have non-bool expression");
  if (auto logic_expr = dynamic_cast<LogicExpression *>(expr.get()); logic_expr != nullptr) {
    auto &children_exprs = logic_expr->GetChildren();
    BUSTUB_ASSERT(children_exprs.size() == 2, "Logic expression should have exactly 2 children.");
    GetAllAndLogicChildren(logic_expr->GetChildAt(0), result_exprs);
    GetAllAndLogicChildren(logic_expr->GetChildAt(1), result_exprs);
    return;
  }
  if (auto comparison_expr = dynamic_cast<ComparisonExpression *>(expr.get()); comparison_expr != nullptr) {
    result_exprs.push_back(expr);
    return;
  }
  throw Exception(ExceptionType::EXECUTION, "Expression should not have non-bool expression");
}

auto Optimizer::IsColumnCompareConstant(const AbstractExpressionRef &expr)
    -> std::optional<std::pair<AbstractExpressionRef, AbstractExpressionRef>> {
  if (auto comparison_expr = dynamic_cast<ComparisonExpression *>(expr.get()); comparison_expr != nullptr) {
    auto &children_exprs = comparison_expr->GetChildren();
    BUSTUB_ASSERT(children_exprs.size() == 2, "Comparison expression should have exactly 2 children.");
    auto left_column_value_expr = dynamic_cast<ColumnValueExpression *>(children_exprs[0].get());
    auto right_constant_value_expr = dynamic_cast<ConstantValueExpression *>(children_exprs[1].get());
    // AbstractExpressionRef column_value_expr_ref [[maybe_unused]];
    // AbstractExpressionRef constant_value_expr_ref [[maybe_unused]];
    // ColumnValueExpression *column_value_expr = nullptr;
    // ConstantValueExpression *constant_value_expr [[maybe_unused]] = nullptr;
    if (left_column_value_expr != nullptr && right_constant_value_expr != nullptr) {
      // column_value_expr_ref = children_exprs[0];
      // constant_value_expr_ref = children_exprs[1];
      // column_value_expr = left_column_value_expr;
      // constant_value_expr = right_constant_value_expr;
      return std::pair<AbstractExpressionRef, AbstractExpressionRef>{children_exprs[0], children_exprs[1]};
    }
    auto left_constant_value_expr = dynamic_cast<ConstantValueExpression *>(children_exprs[0].get());
    auto right_column_value_expr = dynamic_cast<ColumnValueExpression *>(children_exprs[1].get());
    if (left_constant_value_expr != nullptr && right_column_value_expr != nullptr) {
      // column_value_expr_ref = children_exprs[1];
      // constant_value_expr_ref = children_exprs[0];
      // column_value_expr = right_column_value_expr;
      // constant_value_expr = left_constant_value_expr;
      return std::pair<AbstractExpressionRef, AbstractExpressionRef>{children_exprs[1], children_exprs[0]};
    }
  }
  return std::nullopt;
}

auto Optimizer::IsColumnCompareColumn(const AbstractExpressionRef &expr) -> bool {
  if (auto comparison_expr = dynamic_cast<ComparisonExpression *>(expr.get()); comparison_expr != nullptr) {
    auto &children_exprs = comparison_expr->GetChildren();
    BUSTUB_ASSERT(children_exprs.size() == 2, "Comparison expression should have exactly 2 children.");
    auto left_column_value_expr = dynamic_cast<ColumnValueExpression *>(children_exprs[0].get());
    auto right_column_value_expr = dynamic_cast<ColumnValueExpression *>(children_exprs[1].get());
    if (left_column_value_expr != nullptr && right_column_value_expr != nullptr) {
      return true;
    }
  }
  return false;
}

auto Optimizer::IsJoinPredicate(const AbstractExpressionRef &expr)
    -> std::optional<std::pair<AbstractExpressionRef, AbstractExpressionRef>> {
  if (auto comparison_expr = dynamic_cast<ComparisonExpression *>(expr.get()); comparison_expr != nullptr) {
    auto &children_exprs = comparison_expr->GetChildren();
    BUSTUB_ASSERT(children_exprs.size() == 2, "Comparison expression should have exactly 2 children.");
    auto left_column_value_expr = dynamic_cast<ColumnValueExpression *>(children_exprs[0].get());
    auto right_column_value_expr = dynamic_cast<ColumnValueExpression *>(children_exprs[1].get());
    if (left_column_value_expr != nullptr && right_column_value_expr != nullptr &&
        left_column_value_expr->GetTupleIdx() != right_column_value_expr->GetTupleIdx()) {
      if (left_column_value_expr->GetTupleIdx() == 0) {
        return std::pair<AbstractExpressionRef, AbstractExpressionRef>{children_exprs[0], children_exprs[1]};
      }
      return std::pair<AbstractExpressionRef, AbstractExpressionRef>{children_exprs[1], children_exprs[0]};
    }
  }
  return std::nullopt;
}

auto Optimizer::RewriteExpressionForFilterPushDown(const AbstractExpressionRef &expr)
    -> std::array<AbstractExpressionRef, 3> {
  std::vector<AbstractExpressionRef> children_exprs;
  GetAllAndLogicChildren(expr, children_exprs);
  std::array<AbstractExpressionRef, 3> result;
  for (auto &children_expr : children_exprs) {
    BUSTUB_ASSERT(children_expr->GetReturnType() == TypeId::BOOLEAN, "Logic expression's children should be bool type");
    if (IsColumnCompareColumn(children_expr)) {
      if (IsJoinPredicate(children_expr)) {
        result[2] =
            result[2] ? std::make_shared<LogicExpression>(result[2], children_expr, LogicType::And) : children_expr;
      } else {  // 孩子的join predicate 例如(t4.x = t5.x) -> (#0.0 = #0.3)
        auto comparison_expr = dynamic_cast<ComparisonExpression *>(children_expr.get());
        auto left_column_value_exr = dynamic_cast<ColumnValueExpression *>(comparison_expr->GetChildAt(0).get());
        if (left_column_value_exr->GetTupleIdx() == 0) {
          result[0] =
              result[0] ? std::make_shared<LogicExpression>(result[0], children_expr, LogicType::And) : children_expr;
        } else {
          auto new_left_column_value_expr = std::make_shared<ColumnValueExpression>(
              0, left_column_value_exr->GetColIdx(), left_column_value_exr->GetReturnType());
          auto right_column_value_exr = dynamic_cast<ColumnValueExpression *>(comparison_expr->GetChildAt(1).get());
          BUSTUB_ASSERT(left_column_value_exr->GetTupleIdx() == 1 && right_column_value_exr->GetTupleIdx() == 1,
                        "NLJ 右孩子的column value expression's tuple_index should equal 1");
          auto new_right_column_value_expr = std::make_shared<ColumnValueExpression>(
              0, right_column_value_exr->GetColIdx(), right_column_value_exr->GetReturnType());
          auto new_comparison_expr = std::make_shared<ComparisonExpression>(
              new_left_column_value_expr, new_right_column_value_expr, comparison_expr->comp_type_);
          result[1] =
              result[1] ? std::make_shared<LogicExpression>(result[1], children_expr, LogicType::And) : children_expr;
        }
      }
    } else if (auto optional_exprs = IsColumnCompareConstant(children_expr)) {
      auto column_value_expr = dynamic_cast<ColumnValueExpression *>(optional_exprs->first.get());
      if (column_value_expr->GetTupleIdx() == 0) {
        result[0] =
            result[0] ? std::make_shared<LogicExpression>(result[0], children_expr, LogicType::And) : children_expr;
      } else if (column_value_expr->GetTupleIdx() == 1) {
        auto new_column_value_expr = std::make_shared<ColumnValueExpression>(0, column_value_expr->GetColIdx(),
                                                                             column_value_expr->GetReturnType());
        auto new_comparison_expr =
            std::make_shared<ComparisonExpression>(new_column_value_expr, optional_exprs->second,
                                                   dynamic_cast<ComparisonExpression &>(*children_expr).comp_type_);
        // ???
        // result[1] = result[1] ? std::make_shared<LogicExpression>(result[1], new_comparison_expr, LogicType::And) :
        // new_comparison_expr;
        AbstractExpressionRef new_xx = new_comparison_expr;
        result[1] =
            result[1] ? std::make_shared<LogicExpression>(result[1], new_comparison_expr, LogicType::And) : new_xx;
      }
    } else {
      throw Exception(ExceptionType::EXECUTION, "Already constant fold, should not be any other situation");
    }
  }
  return result;
}

auto Optimizer::OptimizeRemoveProjectionEmptyValues(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeRemoveProjectionEmptyValues(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::Projection) {
    const auto &projection_plan = dynamic_cast<const ProjectionPlanNode &>(*optimized_plan);
    if (projection_plan.GetChildPlan()->GetType() == PlanType::Values) {
      const auto &values_plan = dynamic_cast<const ValuesPlanNode &>(*projection_plan.GetChildPlan());

      if (values_plan.GetValues().empty()) {
        return projection_plan.GetChildPlan();
      }
    }
  }

  return optimized_plan;
}

auto Optimizer::OptimizeRemoveNLJEmptyValues(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeRemoveNLJEmptyValues(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    if (nlj_plan.GetJoinType() == JoinType::LEFT) {
      if (nlj_plan.GetRightPlan()->GetType() == PlanType::Values) {
        const auto &right_plan = dynamic_cast<const ValuesPlanNode &>(*nlj_plan.GetRightPlan());
        if (right_plan.GetValues().empty()) {
          return nlj_plan.GetLeftPlan();
        }
      }
    } else if (nlj_plan.GetJoinType() == JoinType::INNER) {
      if (nlj_plan.GetLeftPlan()->GetType() == PlanType::Values) {
        const auto &left_plan = dynamic_cast<const ValuesPlanNode &>(*nlj_plan.GetLeftPlan());
        if (left_plan.GetValues().empty()) {
          return nlj_plan.GetLeftPlan();
        }
      }
      if (nlj_plan.GetRightPlan()->GetType() == PlanType::Values) {
        const auto &right_plan = dynamic_cast<const ValuesPlanNode &>(*nlj_plan.GetRightPlan());
        if (right_plan.GetValues().empty()) {
          return nlj_plan.GetRightPlan();
        }
      }
    }
  }

  return optimized_plan;
}

/*====================================others' optimization========================*/

// auto Optimizer::OptimizeReorderJoinUseIndex(const AbstractPlanNodeRef &plan)
//     -> AbstractPlanNodeRef {
//   std::vector<AbstractPlanNodeRef> children;
//   for (const auto &child : plan->GetChildren()) {
//     children.emplace_back(OptimizeReorderJoinUseIndex(child));
//   }
//   auto optimized_plan = plan->CloneWithChildren(std::move(children));
//   if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
//     const auto &nlj_plan =
//         dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
//     BUSTUB_ASSERT(nlj_plan.children_.size() == 2,
//                   "NLJ should have exactly 2 children.");

//     // ensure the left child is nlp
//     // the right child is seqscan or mockscan
//     if (nlj_plan.GetLeftPlan()->GetType() != PlanType::NestedLoopJoin ||
//         (nlj_plan.GetRightPlan()->GetType() != PlanType::SeqScan &&
//          nlj_plan.GetRightPlan()->GetType() != PlanType::MockScan)) {
//       return optimized_plan;
//     }

//     const auto &left_nlj_plan =
//         dynamic_cast<const NestedLoopJoinPlanNode &>(*nlj_plan.GetLeftPlan());

//     if (left_nlj_plan.GetLeftPlan()->GetType() == PlanType::NestedLoopJoin ||
//         left_nlj_plan.GetRightPlan()->GetType() == PlanType::NestedLoopJoin) {
//       return optimized_plan;
//     }

//     if (const auto *expr = dynamic_cast<const ComparisonExpression *>(
//             &left_nlj_plan.Predicate());
//         expr != nullptr) {
//       if (expr->comp_type_ == ComparisonType::Equal) {
//         if (const auto *left_expr = dynamic_cast<const ColumnValueExpression *>(
//                 expr->children_[0].get());
//             left_expr != nullptr) {
//           if (const auto *right_expr =
//                   dynamic_cast<const ColumnValueExpression *>(
//                       expr->children_[1].get());
//               right_expr != nullptr) {
//             if (left_nlj_plan.GetLeftPlan()->GetType() == PlanType::SeqScan) {
//               const auto &left_seq_scan = dynamic_cast<const SeqScanPlanNode &>(
//                   *left_nlj_plan.GetLeftPlan());
//               if (auto index = MatchIndex(left_seq_scan.table_name_,
//                                           left_expr->GetColIdx());
//                   index != std::nullopt) {
//                 auto *outer_expr = dynamic_cast<const ComparisonExpression *>(
//                     &nlj_plan.Predicate());
//                 auto left_outer_expr =
//                     dynamic_cast<const ColumnValueExpression *>(
//                         outer_expr->children_[0].get());
//                 auto right_outer_expr =
//                     dynamic_cast<const ColumnValueExpression *>(
//                         outer_expr->children_[1].get());
//                 BUSTUB_ASSERT(expr->comp_type_ == ComparisonType::Equal,
//                               "comparison type must be equal");
//                 BUSTUB_ASSERT(outer_expr->comp_type_ == ComparisonType::Equal,
//                               "comparison type must be equal");

//                 auto inner_pred = std::make_shared<ComparisonExpression>(
//                     std::make_shared<ColumnValueExpression>(
//                         0,
//                         left_outer_expr->GetColIdx() -
//                             left_nlj_plan.GetLeftPlan()
//                                 ->output_schema_->GetColumnCount(),
//                         left_outer_expr->GetReturnType()),
//                     std::make_shared<ColumnValueExpression>(
//                         1, right_outer_expr->GetColIdx(),
//                         right_outer_expr->GetReturnType()),
//                     ComparisonType::Equal);
//                 auto outer_pred = std::make_shared<ComparisonExpression>(
//                     std::make_shared<ColumnValueExpression>(
//                         0, right_expr->GetColIdx(),
//                         right_expr->GetReturnType()),
//                     std::make_shared<ColumnValueExpression>(
//                         1, left_expr->GetColIdx(), left_expr->GetReturnType()),
//                     ComparisonType::Equal);

//                 auto right_column_1 =
//                     left_nlj_plan.GetRightPlan()->output_schema_->GetColumns();
//                 auto right_column_2 =
//                     nlj_plan.GetRightPlan()->output_schema_->GetColumns();
//                 std::vector<Column> columns;
//                 columns.reserve(right_column_1.size() + right_column_2.size());

//                 for (const auto &col : right_column_1) {
//                   columns.push_back(col);
//                 }
//                 for (const auto &col : right_column_2) {
//                   columns.push_back(col);
//                 }

//                 std::vector<Column> outer_columns(columns);
//                 for (const auto &col : left_nlj_plan.GetLeftPlan()
//                                            ->output_schema_->GetColumns()) {
//                   outer_columns.push_back(col);
//                 }

//                 return std::make_shared<NestedLoopJoinPlanNode>(
//                     std::make_shared<Schema>(outer_columns),
//                     std::make_shared<NestedLoopJoinPlanNode>(
//                         std::make_shared<Schema>(columns),
//                         left_nlj_plan.GetRightPlan(), nlj_plan.GetRightPlan(),
//                         inner_pred, JoinType::INNER),
//                     left_nlj_plan.GetLeftPlan(), outer_pred, JoinType::INNER);
//               }
//             }
//           }
//         }
//       }
//     }
//   }

//   return optimized_plan;
// }

// auto Optimizer::OptimizePredicatePushDown(const AbstractPlanNodeRef &plan)
//     -> AbstractPlanNodeRef {
//   std::vector<AbstractPlanNodeRef> children;
//   for (const auto &child : plan->GetChildren()) {
//     children.emplace_back(OptimizePredicatePushDown(child));
//   }
//   auto optimized_plan = plan->CloneWithChildren(std::move(children));

//   if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
//     const auto &nlj_plan =
//         dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);

//     if (nlj_plan.GetLeftPlan()->GetType() != PlanType::NestedLoopJoin) {
//       return optimized_plan;
//     }
//     const auto &left_nlj_plan =
//         dynamic_cast<const NestedLoopJoinPlanNode &>(*nlj_plan.GetLeftPlan());

//     if (nlj_plan.GetRightPlan()->GetType() != PlanType::MockScan ||
//         left_nlj_plan.GetLeftPlan()->GetType() != PlanType::MockScan ||
//         left_nlj_plan.GetRightPlan()->GetType() != PlanType::MockScan) {
//       return optimized_plan;
//     }

//     std::vector<AbstractExpressionRef> join_preds;
//     std::vector<AbstractExpressionRef> filter_preds;
//     if (const auto *expr =
//             dynamic_cast<const LogicExpression *>(&nlj_plan.Predicate());
//         expr != nullptr) {
//       while (const auto *inner_expr = dynamic_cast<const LogicExpression *>(
//                  expr->children_[0].get())) {
//         if (const auto *pred = dynamic_cast<const ColumnValueExpression *>(
//                 expr->children_[1]->children_[1].get());
//             pred != nullptr) {
//           join_preds.push_back(expr->children_[1]);
//         } else {
//           filter_preds.push_back(expr->children_[1]);
//         }
//         expr = dynamic_cast<const LogicExpression *>(expr->children_[0].get());
//       }
//       if (const auto *pred = dynamic_cast<const ColumnValueExpression *>(
//               expr->children_[1]->children_[1].get());
//           pred != nullptr) {
//         join_preds.push_back(expr->children_[1]);
//       } else {
//         filter_preds.push_back(expr->children_[1]);
//       }
//       if (const auto *pred = dynamic_cast<const ColumnValueExpression *>(
//               expr->children_[0]->children_[1].get());
//           pred != nullptr) {
//         join_preds.push_back(expr->children_[0]);
//       } else {
//         filter_preds.push_back(expr->children_[0]);
//       }

//       std::vector<AbstractExpressionRef> first_filter;
//       std::vector<AbstractExpressionRef> third_filter;

//       for (const auto &pred : filter_preds) {
//         const auto *outer =
//             dynamic_cast<const ComparisonExpression *>(pred.get());
//         const auto *inner = dynamic_cast<const ColumnValueExpression *>(
//             pred->children_[0].get());
//         if (inner->GetTupleIdx() == 0) {
//           first_filter.push_back(pred);
//         } else {
//           third_filter.push_back(std::make_shared<ComparisonExpression>(
//               std::make_shared<ColumnValueExpression>(0, inner->GetColIdx(),
//                                                       inner->GetReturnType()),
//               pred->children_[1], outer->comp_type_));
//         }
//       }
//       BUSTUB_ASSERT(first_filter.size() == 2, "only in leader board test!");
//       BUSTUB_ASSERT(third_filter.size() == 2, "only in leader board test!");

//       auto first_pred = std::make_shared<LogicExpression>(
//           first_filter[0], first_filter[1], LogicType::And);
//       auto third_pred = std::make_shared<LogicExpression>(
//           third_filter[0], third_filter[1], LogicType::And);

//       auto first_filter_scan = std::make_shared<FilterPlanNode>(
//           left_nlj_plan.children_[0]->output_schema_, first_pred,
//           left_nlj_plan.children_[0]);
//       auto third_filter_scan = std::make_shared<FilterPlanNode>(
//           nlj_plan.GetRightPlan()->output_schema_, third_pred,
//           nlj_plan.GetRightPlan());
//       auto left_node = std::make_shared<NestedLoopJoinPlanNode>(
//           left_nlj_plan.output_schema_, first_filter_scan,
//           left_nlj_plan.GetRightPlan(), left_nlj_plan.predicate_,
//           left_nlj_plan.GetJoinType());
//       return std::make_shared<NestedLoopJoinPlanNode>(
//           nlj_plan.output_schema_, left_node, third_filter_scan, join_preds[0],
//           nlj_plan.GetJoinType());
//     }
//   }

//   return optimized_plan;
// }

// auto Optimizer::IsPredicateFalse(const AbstractExpression &expr) -> bool {
//   if (const auto *compare_expr =
//           dynamic_cast<const ComparisonExpression *>(&expr);
//       compare_expr != nullptr) {
//     if (const auto *left_expr = dynamic_cast<const ConstantValueExpression *>(
//             compare_expr->children_[0].get());
//         left_expr != nullptr) {
//       if (const auto *right_expr =
//               dynamic_cast<const ConstantValueExpression *>(
//                   compare_expr->children_[1].get());
//           right_expr != nullptr) {
//         if (compare_expr->comp_type_ == ComparisonType::Equal) {
//           if (left_expr->val_.CastAs(TypeId::INTEGER).GetAs<int>() !=
//               right_expr->val_.CastAs(TypeId::INTEGER).GetAs<int>()) {
//             return true;
//           }
//         }
//       }
//     }
//   }
//   return false;
// }

// auto Optimizer::OptimizeFalseFilter(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
//   std::vector<AbstractPlanNodeRef> children;
//   for (const auto &child : plan->GetChildren()) {
//     children.emplace_back(OptimizeFalseFilter(child));
//   }

//   auto optimized_plan = plan->CloneWithChildren(std::move(children));

//   if (optimized_plan->GetType() == PlanType::Filter) {
//     const auto &filter_plan =
//         dynamic_cast<const FilterPlanNode &>(*optimized_plan);

//     if (IsPredicateFalse(*filter_plan.GetPredicate())) {
//       return std::make_shared<ValuesPlanNode>(
//           filter_plan.children_[0]->output_schema_,
//           std::vector<std::vector<AbstractExpressionRef>>{});
//     }
//   }
//   return optimized_plan;
// }

// auto Optimizer::OptimizeRemoveColumn(const AbstractPlanNodeRef &plan)
//     -> AbstractPlanNodeRef {
//   std::vector<AbstractPlanNodeRef> children;
//   for (const auto &child : plan->GetChildren()) {
//     children.emplace_back(OptimizeRemoveJoin(child));
//   }

//   auto optimized_plan = plan->CloneWithChildren(std::move(children));
//   if (optimized_plan->GetType() == PlanType::Projection) {
//     const auto outer_proj =
//         dynamic_cast<const ProjectionPlanNode &>(*optimized_plan);

//     if (outer_proj.GetChildPlan()->GetType() == PlanType::Projection) {
//       const auto inner_proj =
//           dynamic_cast<const ProjectionPlanNode &>(*outer_proj.GetChildPlan());

//       if (inner_proj.GetChildPlan()->GetType() == PlanType::Aggregation) {
//         const auto agg_plan = dynamic_cast<const AggregationPlanNode &>(
//             *inner_proj.GetChildPlan());
//         std::vector<AbstractExpressionRef> cols;
//         for (size_t i = 0; i < outer_proj.GetExpressions().size(); ++i) {
//           if (const auto *pred = dynamic_cast<const ColumnValueExpression *>(
//                   inner_proj.GetExpressions()[i].get());
//               pred != nullptr) {
//             cols.push_back(inner_proj.GetExpressions()[i]);
//           } else {
//             // hacking
//             cols.push_back(
//                 inner_proj.GetExpressions()[i]->children_[0]->children_[0]);
//             cols.push_back(
//                 inner_proj.GetExpressions()[i]->children_[0]->children_[1]);
//             cols.push_back(inner_proj.GetExpressions()[i]->children_[1]);
//           }
//         }

//         std::vector<Column> inner_schema;
//         std::vector<AbstractExpressionRef> inner_proj_expr;
//         for (const auto &i : outer_proj.GetExpressions()) {
//           const auto *col =
//               dynamic_cast<const ColumnValueExpression *>(i.get());
//           inner_proj_expr.push_back(
//               inner_proj.GetExpressions()[col->GetColIdx()]);
//           inner_schema.push_back(
//               inner_proj.OutputSchema().GetColumns()[col->GetColIdx()]);
//         }

//         inner_proj_expr.pop_back();
//         inner_proj_expr.push_back(std::make_shared<ArithmeticExpression>(
//             std::make_shared<ArithmeticExpression>(
//                 std::make_shared<ColumnValueExpression>(0, 1, TypeId::INTEGER),
//                 std::make_shared<ColumnValueExpression>(0, 1, TypeId::INTEGER),
//                 ArithmeticType::Plus),
//             std::make_shared<ColumnValueExpression>(0, 2, TypeId::INTEGER),
//             ArithmeticType::Plus));

//         std::vector<AbstractExpressionRef> aggregates;
//         std::vector<AggregationType> agg_types;
//         std::vector<Column> agg_schema;

//         for (size_t i = 0; i < agg_plan.GetGroupBys().size(); ++i) {
//           agg_schema.push_back(agg_plan.OutputSchema().GetColumns()[i]);
//         }

//         aggregates.push_back(agg_plan.GetAggregates()[0]);
//         agg_types.push_back(agg_plan.GetAggregateTypes()[0]);
//         agg_schema.push_back(agg_plan.OutputSchema()
//                                  .GetColumns()[agg_plan.GetGroupBys().size()]);

//         aggregates.push_back(agg_plan.GetAggregates()[3]);
//         agg_types.push_back(agg_plan.GetAggregateTypes()[3]);
//         agg_schema.push_back(
//             agg_plan.OutputSchema()
//                 .GetColumns()[agg_plan.GetGroupBys().size() + 3]);

//         return std::make_shared<ProjectionPlanNode>(
//             std::make_shared<Schema>(inner_schema), inner_proj_expr,
//             std::make_shared<AggregationPlanNode>(
//                 std::make_shared<Schema>(agg_schema), agg_plan.GetChildAt(0),
//                 agg_plan.GetGroupBys(), aggregates, agg_types));
//       }
//     }
//   }
//   return optimized_plan;
// }

// auto Optimizer::OptimizeMergeFilterIndexScan(const AbstractPlanNodeRef &plan)
//     -> AbstractPlanNodeRef {
//   std::vector<AbstractPlanNodeRef> children;
//   for (const auto &child : plan->GetChildren()) {
//     children.emplace_back(OptimizeMergeFilterIndexScan(child));
//   }

//   auto optimized_plan = plan->CloneWithChildren(std::move(children));

//   if (optimized_plan->GetType() == PlanType::Filter) {
//     const auto &filter_plan =
//         dynamic_cast<const FilterPlanNode &>(*optimized_plan);
//     BUSTUB_ASSERT(optimized_plan->children_.size() == 1,
//                   "must have exactly one children");
//     const auto &child_plan = *optimized_plan->children_[0];
//     if (child_plan.GetType() == PlanType::SeqScan) {
//       const auto &seq_scan_plan =
//           dynamic_cast<const SeqScanPlanNode &>(child_plan);
//       const auto *table_info = catalog_.GetTable(seq_scan_plan.GetTableOid());
//       const auto indices = catalog_.GetTableIndexes(table_info->name_);
//       if (const auto *expr = dynamic_cast<const ComparisonExpression *>(
//               filter_plan.GetPredicate().get());
//           expr != nullptr) {
//         if (expr->comp_type_ == ComparisonType::Equal) {
//           if (const auto *left_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[0].get());
//                left_expr != nullptr) {
//             if (const auto *right_expr = dynamic_cast<const ConstantValueExpression *>(expr->children_[1].get());
//                 right_expr != nullptr) {
//               for (const auto *index : indices) {
//                 const auto &columns = index->key_schema_.GetColumns();
//                 if (columns.size() == 1 &&
//                     columns[0].GetName() == table_info->schema_.GetColumn(left_expr->GetColIdx()).GetName()) {
//                   return std::make_shared<IndexScanPlanNode>(
//                       optimized_plan->output_schema_, index->index_oid_,
//                       filter_plan.GetPredicate());
//                 }
//               }
//             }
//           }
//         }
//       }
//     }
//   }
//   return optimized_plan;
// }

}  // namespace bustub
