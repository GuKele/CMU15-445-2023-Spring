//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_plan.h
//
// Identification: src/include/execution/plans/index_scan_plan.h
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "common/macros.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/abstract_plan.h"
#include "storage/index/index.h"
#include "storage/table/tuple.h"
#include "type/value.h"

namespace bustub {
/**
 * IndexScanPlanNode identifies a table that should be scanned with an optional predicate.
 */
class IndexScanPlanNode : public AbstractPlanNode {
 public:
  /**
   * Creates a new index scan plan node.
   * @param output The output format of this scan plan node
   * @param table_oid The identifier of table to be scanned
   */
  IndexScanPlanNode(SchemaRef output, index_oid_t index_oid, std::string index_name = "", int index_num = -1,
                    AbstractExpressionRef filter_predicate = {},
                    std::vector<std::pair<std::vector<Value>, std::vector<Value>>> ranges = {})
      : AbstractPlanNode(std::move(output), {}),
        index_oid_(index_oid),
        index_name_(std::move(index_name)),
        index_num_(index_num),
        filter_predicate_(std::move(filter_predicate)),
        ranges_(std::move(ranges)) {}

  // IndexScanPlanNode(SchemaRef output, index_oid_t index_oid, AbstractExpressionRef filter_predicate = {})
  //     : AbstractPlanNode(std::move(output), {}),
  //       index_oid_(index_oid),
  //       filter_predicate_(std::move(filter_predicate)) {}

  auto GetType() const -> PlanType override { return PlanType::IndexScan; }

  /** @return the identifier of the table that should be scanned */
  auto GetIndexOid() const -> index_oid_t { return index_oid_; }

  BUSTUB_PLAN_NODE_CLONE_WITH_CHILDREN(IndexScanPlanNode);

  /** The table whose tuples should be scanned. */
  index_oid_t index_oid_;

  // Add anything you want here for index lookup
  std::string index_name_;
  int index_num_ = -1;  // 表示使用索引中的前几列
  AbstractExpressionRef filter_predicate_;
  // 表示所有的索引区间[begin, end]，遵循最左前缀原则
  // std::vector<std::pair<Tuple, Tuple>> ranges_;
  std::vector<std::pair<std::vector<Value>, std::vector<Value>>> ranges_;

 protected:
  auto PlanNodeToString() const -> std::string override {
    if (filter_predicate_) {
      std::cout << "range begin: ";
      for (auto &value : ranges_.front().first) {
        std::cout << value.ToString() << " ";
      }
      std::cout << '\n' << "range end: ";
      for (auto &value : ranges_.front().second) {
        std::cout << value.ToString() << " ";
      }
      std::cout << "\n";

      return fmt::format("IndexScan {{ index={{index_oid={}, index_name={}, index_num={}}}, predicate={} }}",
                         index_oid_, index_name_, index_num_, filter_predicate_);
    }
    return fmt::format("IndexScan {{ index_oid={} }}", index_oid_);
  }
};

}  // namespace bustub
