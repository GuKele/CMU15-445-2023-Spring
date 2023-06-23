//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include <cstddef>
#include <cstdint>
#include <optional>
#include <ostream>
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "execution/executor_context.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/logic_expression.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  // throw NotImplementedException("NestedLoopJoinExecutor is not implemented");
  left_executor_->Init();
  cur_left_tuple_ = std::nullopt;

  // std::cout << plan_->GetLeftPlan()->ToString() << std::endl;
  // std::cout << plan_->GetRightPlan()->ToString() << std::endl;
  // std::cout << plan_->Predicate()->ToString() << std::endl;
  // //
  // 我觉得应该是ComparisonExpression(#0.2=#1.1)/LogicExpression((#0.2=#1.1)and(#1.2=#0.1)),但是typeid似乎支持的不太好
  // std::cout << (typeid(plan_->Predicate().get()) == typeid(LogicExpression)) << std::endl;
  // std::cout << typeid(plan_->Predicate().get()).name() << std::endl;

  // auto child_exprs = plan_->Predicate()->GetChildren();
  // for(const auto &expr : child_exprs) {
  //   std::cout << expr->ToString() << std::endl;
  //   // std::cout << typeid(expr.get()).name() << std::endl;
  //   auto c_exprs = expr->GetChildren();
  //   for(const auto &c_expr : c_exprs) {
  //     std::cout << c_expr->ToString() << std::endl;
  //   }
  // }
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // the author do not want sutdent remain right-tuples, because that implementation is BNLJ rather than NLJ

  // while (right_tuples_index_ < right_tuples_.size() || (right_tuples_index_ == right_tuples_.size() &&
  // left_executor_->Next(&cur_left_tuple_, rid))) {
  //   if(right_tuples_index_ == right_tuples_.size()) {
  //     right_tuples_index_ = 0;

  //     if (plan_->GetJoinType() == JoinType::LEFT) {
  //       std::vector<Value> values{};
  //       for(uint32_t i = 0 ; i < left_executor_->GetOutputSchema().GetColumnCount() ; ++i) {
  //         values.push_back(cur_left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
  //       }
  //       for(uint32_t i = 0 ; i < right_executor_->GetOutputSchema().GetColumnCount() ; ++i) {
  //         values.push_back(ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(i).GetType()));
  //       }
  //       *tuple = Tuple{std::move(values), &GetOutputSchema()};
  //       return true;
  //     }
  //   }

  //   while (right_tuples_index_ < right_tuples_.size()) {
  //     if (Matched(cur_left_tuple_, right_tuples_[right_tuples_index_++])) {
  //       std::vector<Value> values{};
  //       // values.insert(values.end(), cur_left_tuple_.GetValue(const Schema *schema, uint32_t column_idx))
  //       for(uint32_t i = 0 ; i < left_executor_->GetOutputSchema().GetColumnCount() ; ++i) {
  //         values.push_back(cur_left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
  //       }
  //       for(uint32_t i = 0 ; i < right_executor_->GetOutputSchema().GetColumnCount() ; ++i) {
  //         values.push_back(right_tuples_[right_tuples_index_ - 1].GetValue(&right_executor_->GetOutputSchema(), i));
  //       }
  //       *tuple = Tuple{std::move(values), &GetOutputSchema()};
  //       return true;
  //     }
  //   }
  // }

  Tuple left_tuple{};
  bool is_new_left_tuple = false;
  while (cur_left_tuple_ || (is_new_left_tuple = left_executor_->Next(&left_tuple, rid))) {
    if (is_new_left_tuple) {
      right_executor_->Init();
      cur_left_tuple_ = left_tuple;
      is_new_left_tuple = false;
      have_matched_ = false;
    }

    Tuple right_tuple{};
    while (right_executor_->Next(&right_tuple, rid)) {
      if (Matched(*cur_left_tuple_, right_tuple)) {
        std::vector<Value> values{};
        for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); ++i) {
          values.push_back(cur_left_tuple_->GetValue(&left_executor_->GetOutputSchema(), i));
        }
        for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); ++i) {
          values.push_back(right_tuple.GetValue(&right_executor_->GetOutputSchema(), i));
        }
        *tuple = Tuple{std::move(values), &GetOutputSchema()};
        have_matched_ = true;
        return true;
      }
    }

    if (!have_matched_ && plan_->GetJoinType() == JoinType::LEFT) {
      std::vector<Value> values{};
      for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); ++i) {
        values.push_back(cur_left_tuple_->GetValue(&left_executor_->GetOutputSchema(), i));
      }
      for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); ++i) {
        values.push_back(ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(i).GetType()));
      }
      *tuple = Tuple{std::move(values), &GetOutputSchema()};
      cur_left_tuple_.reset();
      return true;
    }

    cur_left_tuple_.reset();
  }

  return false;
}

auto NestedLoopJoinExecutor::Matched(const Tuple &left_tuple, const Tuple &right_tuple) -> bool {
  auto val = plan_->Predicate()->EvaluateJoin(&left_tuple, left_executor_->GetOutputSchema(), &right_tuple,
                                              right_executor_->GetOutputSchema());
  return !val.IsNull() && val.GetAs<bool>();
}

}  // namespace bustub
