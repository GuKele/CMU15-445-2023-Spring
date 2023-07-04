//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include <cassert>
#include <cstddef>
#include <unordered_map>
#include <vector>
#include "binder/table_ref/bound_join_ref.h"
#include "common/macros.h"
#include "common/util/hash_util.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_child)),
      right_executor_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  // throw NotImplementedException("HashJoinExecutor is not implemented");
  left_executor_->Init();
  right_executor_->Init();
  right_tuples_map_.clear();
  output_tuples_.clear();
  cursor_ = 0;

  // std::cout << "hash join!!!!" << std::endl;
  // std::cout << plan_->ToString() << std::endl;
  // std::cout << std::endl;
  // std::cout << plan_->GetLeftPlan()->ToString() << std::endl;
  // std::cout << plan_->GetRightPlan()->ToString() << std::endl;

  Tuple tuple{};
  RID rid{};
  while (right_executor_->Next(&tuple, &rid)) {
    std::vector<Value> values{};
    for (const auto &expr : plan_->RightJoinKeyExpressions()) {
      values.emplace_back(expr->Evaluate(&tuple, right_executor_->GetOutputSchema()));
    }
    auto hash_value = HashValues(values);
    right_tuples_map_.insert({hash_value, tuple});
  }

  while (left_executor_->Next(&tuple, &rid)) {
    std::vector<Value> left_values{};
    for (const auto &expr : plan_->LeftJoinKeyExpressions()) {
      left_values.emplace_back(expr->Evaluate(&tuple, left_executor_->GetOutputSchema()));
    }
    auto hash_value = HashValues(left_values);
    auto iter = right_tuples_map_.find(hash_value);
    auto count = right_tuples_map_.count(hash_value);

    // FIXME(gukele): count > 0时，hash相同不意味着一定满足equal条件
    if (count == 0 && plan_->GetJoinType() == JoinType::LEFT) {
      std::vector<Value> values;
      for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); ++i) {
        values.push_back(tuple.GetValue(&left_executor_->GetOutputSchema(), i));
      }
      for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); ++i) {
        values.push_back(ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(i).GetType()));
      }
      output_tuples_.emplace_back(values, &GetOutputSchema());
    }

    while (count > 0) {
      auto &right_tuple = iter->second;
      std::vector<Value> right_values{};
      for (const auto &expr : plan_->RightJoinKeyExpressions()) {
        right_values.emplace_back(expr->Evaluate(&right_tuple, right_executor_->GetOutputSchema()));
      }
      if (Equal(left_values, right_values)) {
        std::vector<Value> values;
        for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); ++i) {
          values.push_back(tuple.GetValue(&left_executor_->GetOutputSchema(), i));
        }
        for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); ++i) {
          values.push_back(right_tuple.GetValue(&right_executor_->GetOutputSchema(), i));
        }
        output_tuples_.emplace_back(values, &GetOutputSchema());
      }
      ++iter;
      --count;
    }
  }
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (cursor_ < output_tuples_.size()) {
    *tuple = output_tuples_[cursor_++];
    return true;
  }
  return false;
}

auto HashJoinExecutor::HashValues(const std::vector<Value> &value_vec) -> hash_t {
  // std::size_t length = 0;
  // the Value::GetLength is not implemented
  // for(const auto &value : value_vec) {
  //   length += value.GetLength();
  // }
  // return HashUtil::HashBytes(reinterpret_cast<const char *>(value_vec.data()), length);

  BUSTUB_ASSERT(!value_vec.empty(), "value vector must non-empty");
  auto result = HashUtil::HashValue(&value_vec.front());
  for (size_t i = 1; i < value_vec.size(); ++i) {
    auto temp = HashUtil::HashValue(&value_vec[i]);
    result = HashUtil::CombineHashes(result, temp);
  }
  return result;
}

auto HashJoinExecutor::Equal(const std::vector<Value> &lhs, const std::vector<Value> &rhs) -> bool {
  assert(lhs.size() == rhs.size());
  for (size_t i = 0; i < lhs.size(); ++i) {
    if (lhs[i].CompareEquals(rhs[i]) != CmpBool::CmpTrue) {
      return false;
    }
  }
  return true;
}

}  // namespace bustub
