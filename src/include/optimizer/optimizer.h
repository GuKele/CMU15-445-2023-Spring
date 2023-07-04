#pragma once

#include <array>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
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

  // TODO(gukele): 感觉将各个优化的接口放在AbstractPlanNode中是更合理的做法，每个plan node override各个优化函数，
  // 在实现中还会调用孩子plan的对应的优化函数。当优化时调用跟节点的所有优化函数（如果该plan不需要优化也要调用孩子plan对应的优化）。

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
  auto OptimizeEliminateContinuousProjection(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  /**
   * @brief 连续两层project,将第一层project中的每个ColumnValueExpression都替换成第二层对应的expr
   */
  auto RewriteExpressionForEliminateContinuousProjection(const AbstractExpressionRef &expr,
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
   * @brief // TODO(gukele): 将表达式计算提出来作为一个函数
   *
   * @param expr
   * @return std::optional<std::vector<std::pair<Tuple, Tuple>>>
   */
  auto ExpressionToRanges(const AbstractExpressionRef &expr) -> std::optional<std::vector<std::pair<Tuple, Tuple>>>;

  /**
   * @brief 如果Seq Scan中有谓词，并且谓词中有索引相关，优化为Index Scan
   * 目前只简单的处理最多两个comparison并且and的情况，而且简单的选择一个列使用索引
   * // TODO(gukele): 看一下开源库是如何处理索引提取
   * // TODO(gukele): 重构索引scan。
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
   * // TODO(gukele): 完善列裁剪
   * @brief 列裁剪的算法实现是自顶向下地把算子过一遍。某个节点需要用到的列，等于它自己需要用到的列union上层所需。
   * 可以发现，由上往下的节点，需要用到的列将越来越多。可能会产生新projection
   *   filter是自己上层需要什么，加上自己过滤条件中需要的列（放在filter前就不用处理seq scan可能存在的谓词了）
   *   aggregation 那么就是上层需要什么，加上aggregation中group by的列和聚合函数中的列(去掉重复的聚合函数)
   *   seq scan 就是上层需要什么，就真的返回什么，不返回完整的行，不修改官方提供的seq
   * scan接口无法实现，(所以先优化成多加一层projection) order by是上层所需，加上自己排序所需的列
   *   NLJ是上层所需，加上join连接中所需的列
   * @param plan
   * @param[in] father_column_indexes_map 上层所需的[列下标, 相同的列下标]，若为空说明plan是根节点。
   * @param[out] father_column_indexes_map   上层所需的[列下标，children plan列裁剪后对应的新列下标]
   */
  auto OptimizeColumnPruning(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  // auto OptimizeColumnPruningImpl(const AbstractPlanNodeRef &plan, std::unordered_map<uint32_t, uint32_t>
  // &father_column_indexes_map) -> AbstractPlanNodeRef;

  /**
   * @brief 根据父亲所需列以及自己需要的列进行裁剪，去除不需要的列。当为plan为根、不需要裁剪(insert、delete、update)、
   * 暂未实现的列裁剪，这三种情况时，father_column_indexes_map为空，然后plan保留所有列。
   *
   *   filter是自己上层需要什么，加上自己过滤条件中需要的列（放在filter前就不用处理seq scan可能存在的谓词了）
   *   aggregation 那么就是上层需要什么，加上aggregation中group by的列和聚合函数中的列(去掉重复的聚合函数)
   *   seq scan 就是上层需要什么，就真的返回什么，不返回完整的行，不修改官方提供的seq
   * scan接口无法实现，(所以先优化成多加一层projection) order by是上层所需，加上自己排序所需的列
   *   NLJ是上层所需，加上join连接中所需的列
   *
   * @param plan
   * @param father_column_indexes_map 为空时表示plan为根或者不需要裁剪(insert、delete、update)或者
   * 暂未实现的列裁剪。plan不进行裁剪并且保留所有列。
   * @return AbstractPlanNodeRef
   */
  auto OptimizeColumnPruningForAnyType(const AbstractPlanNodeRef &plan,
                                       std::unordered_map<uint32_t, uint32_t> &father_column_indexes_map)
      -> AbstractPlanNodeRef;

  /**
   * @brief 需要保证plan为filter,并且father_column_indexes_map不为空，不会单独使用，
   * 只会在OptimizeColumnPruningForAnyType中调用
   *
   * @param plan
   * @param father_column_indexes_map
   * @return AbstractPlanNodeRef
   */
  auto OptimizeColumnPruningForFilter(const AbstractPlanNodeRef &plan,
                                      std::unordered_map<uint32_t, uint32_t> &father_column_indexes_map)
      -> AbstractPlanNodeRef;

  auto OptimizeColumnPruningForProjection(const AbstractPlanNodeRef &plan,
                                          std::unordered_map<uint32_t, uint32_t> &father_column_indexes_map)
      -> AbstractPlanNodeRef;

  /**
   * @brief 裁剪重复的aggregation,并且只返回父亲所需的aggregation,以及全部的group_bys
   * // TODO(gukele): 只返回父亲所需的key。
   *
   * @param plan
   * @param[out] father_column_indexes_map
   * @return AbstractPlanNodeRef
   */
  auto OptimizeColumnPruningForAggregation(const AbstractPlanNodeRef &plan,
                                           std::unordered_map<uint32_t, uint32_t> &father_column_indexes_map)
      -> AbstractPlanNodeRef;

  /**
   * @brief 将plan中的ColumnValueExpression根据indexes_map重写colum index
   *
   * @param plan
   * @param indexes_map map of <old column idx, new column idx>
   * @return AbstractPlanNodeRef
   */
  auto RewriteExpressionForColumnPruning(const AbstractPlanNodeRef &plan,
                                         const std::unordered_map<uint32_t, uint32_t> &indexes_map,
                                         const std::unordered_map<uint32_t, uint32_t> &indexes_map_for_join_right = {})
      -> AbstractPlanNodeRef;

  auto RewriteExpressionForColumnPruning(const AbstractExpressionRef &expr,
                                         const std::unordered_map<uint32_t, uint32_t> &indexes_map)
      -> AbstractExpressionRef;

  auto RewriteExpressionForColumnPruningJoin(const AbstractExpressionRef &expr,
                                             const std::unordered_map<uint32_t, uint32_t> &left_indexes_map,
                                             const std::unordered_map<uint32_t, uint32_t> &right_indexes_map)
      -> AbstractExpressionRef;

  /**
   * @brief Get the Column Indexes object of plan，当plan不进行裁剪时，使用该函数获取plan所有列中
   * 所需孩子plan中的所有列index
   * 例如filter作为根，就不能裁剪，output schema 应该等于孩子的output schema
   *
   * @param plan The plan that does not require column pruning
   * @param[out] column_indexes_map
   */
  void GetAllColumnsNeededFromChildren(const AbstractPlanNodeRef &plan,
                                       std::unordered_map<uint32_t, uint32_t> &column_indexes_map) const;

  /**
   * @brief Get the Column Indexes of expr
   *
   * @param expr
   * @param[out] column_indexes
   */
  void GetColumnIndexes(const AbstractExpressionRef &expr,
                        std::unordered_map<uint32_t, uint32_t> &column_indexes_map) const;

  /**
   * @brief 当使用where进行表连接时，会出现filter + NLJ，并且filter谓词包含join谓词和non-join谓词;而使用join
   * on时join谓词会直接在nlj中。 总之我们不管将filter中是否出现了join、non-join谓词， 我们将join predicate merge into
   * NLJ(if predicate true),将non-join predicate push down成为children plans' filter,
   * 然后自顶向下递归继续OptimizeFilterNLJPredicatePushDown
   *
   *   可能会产生新的Filter,应该在(filter merger)Filter + NLJ改变之前调用，例如MergeFilterNLJ、NLJAsHashJoin。
   *   把Filter + NLJ表达式中属于NLJ左右子plan的谓词下推成左右子plan的一个filter,并且把join的谓词合并到NLJ
   *   例如(#0.1 > 10)这种推成下层的一个filter，而(#0.1 > #1.2)的join谓词合并到NLJ
   *
   *   CREATE TABLE t4(x int, y int); CREATE TABLE t5(x int, y int); CREATE TABLE t6(x int, y int);
   *   EXPLAIN SELECT * FROM t4, t5, t6 WHERE(t4.x = t5.x) AND (t5.y = t6.y) AND (t4.y >= 1000000)
   * AND (t4.y < 1500000) AND (t6.x >= 100000) AND (t6.x < 150000);
   *
   * @param plan
   * @return AbstractPlanNodeRef
   */
  auto OptimizeFilterNLJPredicatePushDown(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

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
   * @brief projection + empty values -> empty values
   *
   * @param plan
   * @return AbstractPlanNodeRef
   */
  auto OptimizeRemoveProjectionEmptyValues(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  /**
   * @brief 如果NLJ其中一个children plan为空ValuesPlan,并且不是left join,就移除这个join；
   * 如果是left join,右孩子plan是空ValuesPlan，那么直接返回左孩子plan
   *
   * @param plan
   * @return AbstractPlanNodeRef
   */
  auto OptimizeRemoveNLJEmptyValues(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  /**
   * @brief projection的孩子plan是空ValuesPlan，移除这层projection
   *
   * @param plan
   * @return AbstractPlanNodeRef
   */
  auto OptimizeRemoveProjection(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;
  auto OptimizeRemoveFilter(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  // TODO(gukele): 干脆上边几个合在一起，就是孩子是空ValuesPlan，我们自底向上处理。注意根plan的话特殊处理
  auto OptimizeMergeEmptyValuesPlan(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  /** Catalog will be used during the planning process. USERS SHOULD ENSURE IT OUTLIVES
   * OPTIMIZER, otherwise it's a dangling reference.
   */
  const Catalog &catalog_;

  const bool force_starter_rule_;
};

}  // namespace bustub
