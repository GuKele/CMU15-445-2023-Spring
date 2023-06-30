#include <algorithm>
#include <array>
#include <cfloat>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <tuple>
#include <vector>
#include "binder/table_ref/bound_join_ref.h"
#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
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
#include "execution/plans/filter_plan.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "execution/plans/seq_scan_plan.h"
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
  p = OptimizeConstantFold(p);
  p = OptimizeEliminateTrueFilter(p);
  p = OptimizeEliminateFalseFilter(p);
  p = OptimizeMergeProjection(p);  // project与下层输出完全一样，最多只是换了列的名字时，去除该project
  p = OptimizeEliminateProjection(p);
  p = OptimizeFilterNLJPushDown(p);  // 应该在Filter + NLJ改变之前调用，例如MergeFilterNLJ、NLJAsHashJoin
  p = OptimizeMergeFilterNLJ(p);
  p = OptimizeMergeFilterScan(p);  // 谓词filter下推 Filter + Scan -> Scan + Filter
  p = OptimizeRemoveNLJ(p);
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

auto Optimizer::OptimizeEliminateProjection(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeEliminateProjection(child));
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
        new_exprs.emplace_back(RewriteExpressionForEliminateProjection(expr, child_project_exprs));
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

auto Optimizer::RewriteExpressionForEliminateProjection(const AbstractExpressionRef &expr,
                                                        const std::vector<AbstractExpressionRef> &children_exprs)
    -> AbstractExpressionRef {
  std::vector<AbstractExpressionRef> children;
  for (const auto &child : expr->GetChildren()) {
    children.emplace_back(RewriteExpressionForEliminateProjection(child, children_exprs));
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
              throw Exception(ExceptionType::EXECUTION, "decimal type not implemented");
              {
                auto value = constant_value.GetAs<double>();
                auto max_double = std::nextafter(value, DBL_MAX);
                next = value == max_double ? constant_value : Value(type_id, max_double);
                // last = ?;
              }
              break;
            case TypeId::VARCHAR:
            case TypeId::BOOLEAN:
              throw Exception(ExceptionType::EXECUTION, "other type not implemented");
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
      BUSTUB_ASSERT(true, "Or logic not implemented");
    }
  }
  return std::nullopt;
}

auto Optimizer::OptimizeColumnPruning(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  throw Exception(ExceptionType::EXECUTION, "Column pruning not implemented");
}

auto Optimizer::OptimizeFilterNLJPushDown(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // 应该是自顶向下
  AbstractPlanNodeRef optimized_plan = plan;

  if (plan->GetType() == PlanType::Filter) {
    auto &filter_plan = dynamic_cast<const FilterPlanNode &>(*plan);
    if (filter_plan.GetChildPlan()->GetType() == PlanType::NestedLoopJoin) {
      auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*filter_plan.GetChildPlan());
      if (IsPredicateTrue(nlj_plan.Predicate())) {
        auto rewrite_expr = RewriteExpressionForJoin(filter_plan.GetPredicate(),
                                                     nlj_plan.GetLeftPlan()->OutputSchema().GetColumnCount(),
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
          optimized_plan =
              std::make_shared<NestedLoopJoinPlanNode>(filter_plan.output_schema_, new_left_children_plan,
                                                       new_right_children_plan, join_expr, nlj_plan.GetJoinType());
          // std::cout << "NLJ : \n" << optimized_plan->ToString() << "\n" << std::endl;
        }
      }
    }
  }

  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : optimized_plan->GetChildren()) {
    children.emplace_back(OptimizeFilterNLJPushDown(child));
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

auto Optimizer::OptimizeRemoveNLJ(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeRemoveNLJ(child));
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
