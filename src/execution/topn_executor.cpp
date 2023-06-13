#include "execution/executors/topn_executor.h"
#include <algorithm>
#include <queue>
#include <vector>
#include "storage/table/tuple.h"
#include "type/type_id.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  // throw NotImplementedException("TopNExecutor is not implemented");
  child_executor_->Init();
  ordered_output_tuples_.clear();
  index_ = 0;

  auto size = plan_->GetN();
  const auto &order_bys = plan_->GetOrderBy();

  // 同sort_executor的comparator
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

  // auto re_comparator = [&comparator](const Tuple &lhs, const Tuple &rhs) { return !comparator(lhs, rhs); };
  // std::partial_sort
  std::priority_queue<Tuple, std::vector<Tuple>, decltype(comparator)> heap(comparator);
  Tuple tuple{};
  RID rid{};
  while (child_executor_->Next(&tuple, &rid)) {
    heap.push(tuple);
    if (heap.size() == size + 1) {
      heap.pop();
    }
  }

  while (!heap.empty()) {
    ordered_output_tuples_.emplace_back(heap.top());
    heap.pop();
  }
  std::reverse(ordered_output_tuples_.begin(), ordered_output_tuples_.end());
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (index_ < ordered_output_tuples_.size()) {
    *tuple = ordered_output_tuples_[index_++];
    return true;
  }
  return false;
}

auto TopNExecutor::GetNumInHeap() -> size_t {
  // throw NotImplementedException("TopNExecutor is not implemented");
  return ordered_output_tuples_.size();
};

}  // namespace bustub
