#include "execution/plans/abstract_plan.h"
#include "optimizer/optimizer.h"

// Note for 2023 Spring: You can add all optimizer rule implementations and apply the rules as you want in this file.
// Note that for some test cases, we force using starter rules, so that the configuration here won't take effects.
// Starter rule can be forcibly enabled by `set force_optimizer_starter_rule=yes`.

namespace bustub {

auto Optimizer::OptimizeCustom(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  auto p = plan;
  p = OptimizeMergeProjection(p);
  p = OptimizeMergeFilterNLJ(p);
  p = OptimizeOrderByAsIndexScan(p);
  p = OptimizeMergeFilterScan(p);  // 谓词filter下推 Filter + Scan -> Scan + Filter
  p = OptimizeNLJAsHashJoin(p);    // NLJ -> Hash Join p =
  p = OptimizeSortLimitAsTopN(p);  // limit + sort -> Top-N
  // TODO(gukele): more optimizer!!!!

  // NLJ -> Index Join

  /*
   * p3 Leader board Task (Optional):  https://www.zhihu.com/people/Dlee-01/posts
   *   Query 2: Too Many Joins!
   *     Filter + Hash/Index Join -> Hash/Index Join + Filte*
   *   Query 3: The Mad Data Scientist
   *     Projection优化 常量折叠、列裁剪
   *     遇到连续的两个 Projection，合并为 1 个，只取上层 Projection 所需列。
   *     遇到 Projection + Aggregation，改写 aggregates，截取 Projection 中需要的项目，其余直接抛弃。
   *     同样地，我个人认为 Column Pruning 也是自顶向下地改写比较方便。具体实现是收集 Projection 里的所有
   *     column，然后改写下层节点，仅保留上层需要 project 的 column。这里的 column 不一定是表中的 column，而是一种广义的
   *     column，例如 SELECT t1.x + t1.y FROM t1 中的 t1.x + t1.y。
   */

  return p;
}

}  // namespace bustub
