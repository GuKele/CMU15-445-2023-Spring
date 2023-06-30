#pragma once

#include <array>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "concurrency/transaction.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "storage/table/tuple.h"
#include "type/value.h"

namespace bustub {

/**
 * The optimizer takes an `AbstractPlanNode` and outputs an optimized `AbstractPlanNode`.
 */
class Optimizer {
 public:
  explicit Optimizer(const Catalog &catalog, bool force_starter_rule)
      : catalog_(catalog), force_starter_rule_(force_starter_rule) {}

  auto Optimize(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  auto OptimizeCustom(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

 private:
  /**
   * @brief merge projections that do identical project.
   * Identical projection might be produced when there's `SELECT *`, aggregation, or when we need to rename the columns
   * in the planner. We merge these projections so as to make execution faster.
   * Identical projection：project除了列名字(schema)完全与下层输出相同。此时应该去掉该层，并且使用project的列名字
   *
   * explain select * from my_table;
   * === PLANNER ===
   * Projection { exprs=[#0.0, #0.1] } | (my_table.colA:INTEGER, my_table.colB:INTEGER)
   *   MockScan { table=my_table } | (my_table.colA:INTEGER, my_table.colB:INTEGER)
   * === OPTIMIZER ===
   * MockScan { table=my_table } | (my_table.colA:INTEGER, my_table.colB:INTEGER)
   *
   */
  auto OptimizeMergeProjection(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  /**
   * @brief merge filter condition into nested loop join.
   * In planner, we plan cross join + filter with cross product (done with nested loop join) and a filter plan node. We
   * can merge the filter condition into nested loop join to achieve better efficiency.
   */
  auto OptimizeMergeFilterNLJ(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  /**
   * @brief optimize nested loop join into hash join.
   * In the starter code, we will check NLJs with exactly one equal condition. You can further support optimizing joins
   * with multiple eq conditions.
   */
  auto OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  /**
   * @brief optimize nested loop join into index join.
   */
  auto OptimizeNLJAsIndexJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  /**
   * @brief eliminate always true filter
   */
  auto OptimizeEliminateTrueFilter(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  /**
   * @brief merge filter into filter_predicate of seq scan plan node
   */
  auto OptimizeMergeFilterScan(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  /**
   * @brief rewrite expression to be used in nested loop joins. e.g., if we have `SELECT * FROM a, b WHERE a.x = b.y`,
   * we will have `#0.x = #0.y` in the filter plan node. We will need to figure out where does `0.x` and `0.y` belong
   * in NLJ (left table or right table?), and rewrite it as `#0.x = #1.y`.
   *
   * @param expr the filter expression
   * @param left_column_cnt number of columns in the left size of the NLJ
   * @param right_column_cnt number of columns in the left size of the NLJ
   */
  auto RewriteExpressionForJoin(const AbstractExpressionRef &expr, size_t left_column_cnt, size_t right_column_cnt)
      -> AbstractExpressionRef;

  /** @brief check if the predicate is true::boolean */
  auto IsPredicateTrue(const AbstractExpressionRef &expr) -> bool;

  /**
   * @brief optimize order by as index scan if there's an index on a table
   */
  auto OptimizeOrderByAsIndexScan(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  /** @brief check if the index can be matched */
  auto MatchIndex(const std::string &table_name, uint32_t index_key_idx)
      -> std::optional<std::tuple<index_oid_t, std::string>>;

  /**
   * @brief optimize sort + limit as top N
   */
  auto OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  /**
   * @brief get the estimated cardinality for a table based on the table name. Useful when join reordering. BusTub
   * doesn't support statistics for now, so it's the only way for you to get the table size :(
   *
   * @param table_name
   * @return std::optional<size_t>
   */
  auto EstimatedCardinality(const std::string &table_name) -> std::optional<size_t>;

  // TODO(gukele): More optimizer!

  /**
   * @brief 连续两层projection,第一层projection的ColumnValueExpression时，优化掉一层projection
   * 用第一层exprs对应的第二层的exprs，并且使用第一层的列名字(schema)
   *
   * explain select newA + 3 as newB from (select colA + 1 as newA from __mock_table_1 where colA > 10);
   * === PLANNER ===
   * Projection { exprs=[(#0.0+3)] } | (newb:INTEGER)
   *   Projection { exprs=[#0.0] } | (__subquery#0.newa:INTEGER)
   *     Projection { exprs=[(#0.0+1)] } | (newa:INTEGER)
   *       Filter { predicate=(#0.0>10) } | (__mock_table_1.colA:INTEGER, __mock_table_1.colB:INTEGER)
   *         MockScan { table=__mock_table_1 } | (__mock_table_1.colA:INTEGER, __mock_table_1.colB:INTEGER)
   * === OPTIMIZER ===
   * Projection { exprs=[((#0.0+1)+3)] } | (newb:INTEGER)
   *   Filter { predicate=(#0.0>10) } | (__mock_table_1.colA:INTEGER, __mock_table_1.colB:INTEGER)
   *     MockScan { table=__mock_table_1 } | (__mock_table_1.colA:INTEGER, __mock_table_1.colB:INTEGER)
   */
  auto OptimizeEliminateProjection(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  /**
   * @brief 连续两层project,将第一层project中的每个ColumnValueExpression都替换成第二层对应的expr
   */
  auto RewriteExpressionForEliminateProjection(const AbstractExpressionRef &expr,
                                               const std::vector<AbstractExpressionRef> &children_exprs)
      -> AbstractExpressionRef;

  /**
   * @brief 将FilterPlan和ValuesPlan中的常量表达式折叠
   *
   * @param plan
   * @return AbstractPlanNodeRef
   */
  auto OptimizeConstantFold(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  /*
   * 常量折叠：
   *   两个常量运算换成常量：
   *     原表达式	    优化后表达式
   *     1 + 4         5
   *   常量逻辑比较换成常量true/false： 一侧为常量bool的逻辑运算、两侧常量运算
   *     原表达式	            优化后表达式
   *     x >= x                true
   *     y < y                 false
   *     1 = 1	               true
   *     x = 1 and false	   false
   *     true or y >= 9	       true
   *     1 + 5 = 8	           false
       // TODO(gukele):
   *   替换常量：
   *     原表达式	              优化后表达式
   *     x = y and y = 3       x = 3 and y = 3
   *     x >= y and y = 3      x > 3 and y = 3
         *     ((x + 3) + 4)         x + 7
   */
  auto RewriteExpressionForConstantFold(const AbstractExpressionRef &expr) -> AbstractExpressionRef;

  /**
   * @brief Filter {(exprs=false)} + xxPlan -> empty ValuesPlan
   *
   */
  auto OptimizeEliminateFalseFilter(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  auto IsPredicateFalse(const AbstractExpressionRef &expr) -> bool;

  /**
   * @brief // TODO(gukele): 将表达式
   *
   * @param expr
   * @return std::optional<std::vector<std::pair<Tuple, Tuple>>>
   */
  auto ExpressionToRanges(const AbstractExpressionRef &expr) -> std::optional<std::vector<std::pair<Tuple, Tuple>>>;

  /**
   * @brief 如果Seq Scan中有谓词，并且谓词中有索引相关，优化为Index Scan
   * 目前只简单的处理最多两个comparison并且and的情况，而且简单的选择一个列使用索引
   * // TODO(gukele): 看一下开源库是如何处理索引提取
   * 下面的方法似乎也只是处理全是and的逻辑
   * Index Key
   *    用于确定 SQL 查询在索引中的连续范围（起始点 + 终止点）的查询条件，被称之为Index Key；
   *    由于一个范围，至少包含一个起始条件与一个终止条件，因此 Index Key 也被拆分为 Index First Key 和 Index Last Key，
   *    分别用于定位索引查找的起始点和终止点
   * Index First Key
   *    用于确定索引查询范围的起始点；提取规则：从索引的第一个键值开始，检查其在 where 条件中是否存在，若存在并且条件是
   * =、>=， 则将对应的条件加入Index First Key之中，继续读取索引的下一个键值，使用同样的提取规则；若存在并且条件是 >，
   *    则将对应的条件加入 Index First Key 中，同时终止 Index First Key 的提取；若不存在，同样终止 Index First Key
   * 的提取.
   *
   *    select * from tbl_test where b >= 2 and b < 7 and c > 0 and d != 2 and e != 'a'，
   *    应用这个提取规则，提取出来的 Index First Key 为 b >= 2, c > 0
   * Index Last Key
   *    用于确定索引查询范围的终止点，与 Index First Key 正好相反；提取规则：从索引的第一个键值开始，
   *    检查其在 where 条件中是否存在，若存在并且条件是 =、<=，则将对应条件加入到 Index Last Key 中，
   *    继续提取索引的下一个键值，使用同样的提取规则；若存在并且条件是 < ，则将条件加入到 Index Last Key 中，
   *    同时终止提取；若不存在，同样终止Index Last Key的提取.
   *
   *    select * from tbl_test where b >= 2 and b < 7 and c > 0 and d != 2 and e != 'a'，
   *    应用这个提取规则，提取出来的 Index Last Key为 b < 7
   * @return AbstractPlanNodeRef
   */
  auto OptimizeSeqScanAsIndexScan(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  /**
   *
   * @return std::optional<std::tuple<index_oid_t, uint32_t>>  [index_oid_t, key attribute]即索引oid和索引的第几列
   */

  /**
   * @brief 检查一个comparison expression是否是一个可能能使用索引的表达式
   *（#0.1 > #0.2）、(#0.1 != 5)等都不可以使用索引
   * (#0.1 > 10)并且存在索引包含#0.1才有可能(最左前缀原则)可以使用索引
   *
   * @param expr The comparison expression to be checked
   * @param indexes The indexes that expr maybe used
   * @return std::optional<std::tuple<IndexInfo, uint32_t, Value, ComparisonType>>
   *         [index_info, key attribute索引的第几列，constant value, comparison type]
   */
  auto CanComparisonUseIndexLookup(const AbstractExpressionRef &expr, const std::vector<IndexInfo *> &indexes)
      -> std::optional<std::tuple<IndexInfo *, uint32_t, Value, ComparisonType>>;

  auto CanUseIndexLookup(const AbstractExpressionRef &expr, const std::vector<IndexInfo *> &indexes)
      -> std::optional<std::tuple<IndexInfo *, std::vector<uint32_t>, std::vector<Value>, std::vector<ComparisonType>>>;

  /**
   * // TODO(gukele): 列裁剪
   * @brief 列裁剪的算法实现是自顶向下地把算子过一遍。某个节点需要用到的列，等于它自己需要用到的列，
   * 加上它的父节点需要用到的列。可以发现，由上往下的节点，需要用到的列将越来越多。
   */
  auto OptimizeColumnPruning(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  /**
   * @brief 应该在Filter + NLJ改变之前调用，例如MergeFilterNLJ、NLJAsHashJoin。
   * 把Filter + NLJ表达式中属于NLJ左右子plan的谓词下推成左右子plan的一个filter,并且把join的谓词合并到NLJ
   * 例如(#0.1 > 10)这种推成下层的一个filter，而(#0.1 > #1.2)的join谓词合并到NLJ
   *
     CREATE TABLE t4(x int, y int); CREATE TABLE t5(x int, y int); CREATE TABLE t6(x int, y int);
   *
     EXPLAIN SELECT * FROM t4, t5, t6 WHERE(t4.x = t5.x) AND (t5.y = t6.y) AND (t4.y >= 1000000)
     AND (t4.y < 1500000) AND (t6.x >= 100000) AND (t6.x < 150000);
   *
   * @param plan
   * @return AbstractPlanNodeRef
   */
  auto OptimizeFilterNLJPushDown(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  static auto HasOrLogic(const AbstractExpressionRef &expr) -> bool;

  /**
   * @brief Get the All And Logic Children object
   *
   * @param expr
   * @param[out] result_exprs
   */
  void GetAllAndLogicChildren(const AbstractExpressionRef &expr, std::vector<AbstractExpressionRef> &result_exprs);

  /**
   * @brief 如果是一个ComparisonExpression(ColumnValueExpression, ConstantValueExpression)，
   * 则解析出ColumnValueExpression, ConstantValueExpression
   *
   * @param expr
   * @return std::optional<std::pair<AbstractExpressionRef, AbstractExpressionRef>> [ColumnValueExpression,
   * ConstantValueExpression]
   */
  auto IsColumnCompareConstant(const AbstractExpressionRef &expr)
      -> std::optional<std::pair<AbstractExpressionRef, AbstractExpressionRef>>;

  auto IsColumnCompareColumn(const AbstractExpressionRef &expr) -> bool;

  auto IsJoinPredicate(const AbstractExpressionRef &expr)
      -> std::optional<std::pair<AbstractExpressionRef, AbstractExpressionRef>>;

  /**
   * @brief
   *
   * @param expr
   * @return std::array<AbstractExpressionRef, 3> [left_filter_expr, right_filter_expr, join_expr]
   */
  auto RewriteExpressionForFilterPushDown(const AbstractExpressionRef &expr) -> std::array<AbstractExpressionRef, 3>;

  /**
   * @brief 如果NLJ其中一个children plan为空ConstantValuesPlan,并且不是left join,就移除这个join
   *
   * @param plan
   * @return AbstractPlanNodeRef
   */
  auto OptimizeRemoveNLJ(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  /** Catalog will be used during the planning process. USERS SHOULD ENSURE IT OUTLIVES
   * OPTIMIZER, otherwise it's a dangling reference.
   */
  const Catalog &catalog_;

  const bool force_starter_rule_;
};

}  // namespace bustub
