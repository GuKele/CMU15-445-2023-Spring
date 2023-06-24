//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <stack>
#include <string>
#include <unordered_set>
#include <vector>
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/executors/topn_executor.h"
#include "storage/table/tuple.h"

namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  // 1.check txn state
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  if (txn->GetState() == TransactionState::COMMITTED) {
    // TODO(gukele): 如果加锁时已经commit了，直接返回true？
    throw Exception(ExceptionType::EXECUTION, "already commited?");
    // return true;
  }

  try {
    CanTxnTakeLock(txn, lock_mode);
  } catch (const TransactionAbortException &e) {
    throw(e);
  }

  // 2.get lock request queue corresponding to the table
  std::shared_ptr<LockRequestQueue> table_lock_request_queue;
  {
    std::lock_guard<std::mutex> guard(table_lock_map_latch_);
    if (auto table_lock_request_queue_iterator = table_lock_map_.find(oid);
        table_lock_request_queue_iterator != table_lock_map_.end()) {
      table_lock_request_queue = table_lock_request_queue_iterator->second;
    } else {  // 没有则创建
      table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
      table_lock_request_queue = table_lock_map_[oid];
    }
    table_lock_request_queue->latch_.lock();
  }

  {
    // TODO(gukele): 整个逻辑是这样的，首先一个for循环遍历整个table_lock_request_queue，

    // 3.检查是否为锁升级，如果是则进行锁升级
    std::unique_lock<std::mutex> lock(table_lock_request_queue->latch_, std::adopt_lock);
    std::optional<bool> op_bool;
    try {
      op_bool = UpgradeLockTable(txn, lock_mode, table_lock_request_queue.get(), lock);
    } catch (const TransactionAbortException &e) {
      throw(e);
    }
    if (op_bool.has_value()) {
      return *op_bool;
    }

    // 4.非锁升级,普通加锁事件
    table_lock_request_queue->request_queue_.emplace_back(
        std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid));
    auto iter = --(table_lock_request_queue->request_queue_.end());
    auto &lock_request = table_lock_request_queue->request_queue_.back();

    while (!CanGrantLock(table_lock_request_queue.get(), lock_request.get())) {
      table_lock_request_queue->cv_.wait(lock);

      // 查看事务当前的状态，如果是abort就notify，唤醒其它等待队列上阻塞的线程
      // 为什么事务阻塞在请求锁的过程中状态会有可能被设置为aborted？因为发生了死锁，被死锁检测进程给强行设置的
      if (txn->GetState() == TransactionState::ABORTED) {
        table_lock_request_queue->upgrading_ = INVALID_TXN_ID;
        table_lock_request_queue->request_queue_.erase(iter);
        // condition_variable.notify之前应该释放锁，否则被唤醒的线程无法马上拿到锁又会被阻塞
        lock.unlock();
        table_lock_request_queue->cv_.notify_all();
        return false;
      }
    }
    lock_request->granted_ = true;
    UpdateTxnTableLockSet(txn, lock_request.get(), true);
  }

  if (lock_mode != LockMode::EXCLUSIVE) {
    table_lock_request_queue->cv_.notify_all();
  }

  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  // 1.尝试拿到table_lock_request_queue
  std::shared_ptr<LockRequestQueue> table_lock_request_queue;
  {
    std::lock_guard<std::mutex> guard(table_lock_map_latch_);
    if (auto table_lock_request_queue_iterator = table_lock_map_.find(oid);
        table_lock_request_queue_iterator != table_lock_map_.end()) {
      table_lock_request_queue = table_lock_request_queue_iterator->second;
      table_lock_request_queue->latch_.lock();
    } else {  // 没有则说明直接不持有该锁
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    }
  }

  // 2.若能拿到queue，则检查是否持有该锁(txn对一个表至多持有一种表锁)
  std::unique_lock<std::mutex> lock(table_lock_request_queue->latch_, std::adopt_lock);
  bool have_table_lock = false;
  auto iter = table_lock_request_queue->request_queue_.begin();
  for (; iter != table_lock_request_queue->request_queue_.end(); ++iter) {
    auto &request = *iter;
    if (request->txn_id_ == txn->GetTransactionId()) {
      BUSTUB_ASSERT(request->granted_ == true, "we unlock table lock have not been granted");
      have_table_lock = true;
      break;
    }
  }

  if (!have_table_lock) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  // 3.检查对应表的行锁是否都已经释放了
  auto request = *iter;
  if (!CheckNotHoldAppropriateLockOnRow(txn, oid, request->lock_mode_)) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }

  // 4.释放锁，更新state
  table_lock_request_queue->request_queue_.erase(iter);
  lock.unlock();
  // TODO(gukele): Finally, unlocking a resource should also grant any new lock requests for the resource (if possible).
  // ？？？
  table_lock_request_queue->cv_.notify_all();

  UpdateTxnState(txn, request->lock_mode_);
  UpdateTxnTableLockSet(txn, request.get(), false);
  return true;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  // 1.check txn state
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  if (txn->GetState() == TransactionState::COMMITTED) {
    // TODO(gukele): 如果加锁时已经commit了，直接返回true？
    throw Exception(ExceptionType::EXECUTION, "already commited?");
    // return true;
  }

  // 行加锁不允许意向锁
  if (lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::INTENTION_EXCLUSIVE ||
      lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }

  // 行锁需要先拿到对应的表级意向锁
  if (!CheckAppropriateLockOnTable(txn, oid, lock_mode)) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
  }

  try {
    CanTxnTakeLock(txn, lock_mode);
  } catch (const TransactionAbortException &e) {
    throw(e);
  }

  // 2.get lock request queue corresponding to the row
  std::shared_ptr<LockRequestQueue> row_lock_request_queue;
  {
    std::lock_guard<std::mutex> guard(row_lock_map_latch_);
    if (auto row_lock_request_queue_iterator = row_lock_map_.find(rid);
        row_lock_request_queue_iterator != row_lock_map_.end()) {
      row_lock_request_queue = row_lock_request_queue_iterator->second;
    } else {  // 没有则创建
      row_lock_map_[rid] = std::make_shared<LockRequestQueue>();
      row_lock_request_queue = row_lock_map_[rid];
    }
    row_lock_request_queue->latch_.lock();
  }

  {
    // 3.检查是否为锁升级，如果是则进行锁升级
    std::unique_lock<std::mutex> lock(row_lock_request_queue->latch_, std::adopt_lock);
    std::optional<bool> op_bool;
    try {
      op_bool = UpgradeLockRow(txn, lock_mode, row_lock_request_queue.get(), lock);
    } catch (const TransactionAbortException &e) {
      throw(e);
    }
    if (op_bool.has_value()) {
      return *op_bool;
    }

    // 4.非锁升级,普通加锁事件
    row_lock_request_queue->request_queue_.emplace_back(
        std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid));
    auto iter = --(row_lock_request_queue->request_queue_.end());
    auto &lock_request = row_lock_request_queue->request_queue_.back();

    while (!CanGrantLock(row_lock_request_queue.get(), lock_request.get())) {
      row_lock_request_queue->cv_.wait(lock);

      // 查看事务当前的状态，如果是abort就notify，唤醒其它等待队列上阻塞的线程
      // 为什么事务阻塞在请求锁的过程中状态会有可能被设置为aborted？因为发生了死锁，被死锁检测进程给强行设置的
      if (txn->GetState() == TransactionState::ABORTED) {
        row_lock_request_queue->upgrading_ = INVALID_TXN_ID;
        row_lock_request_queue->request_queue_.erase(iter);
        lock.unlock();
        row_lock_request_queue->cv_.notify_all();
        return false;
      }
    }
    lock_request->granted_ = true;
    UpdateTxnRowLockSet(txn, lock_request.get(), true);
  }

  if (lock_mode != LockMode::EXCLUSIVE) {
    row_lock_request_queue->cv_.notify_all();
  }

  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force) -> bool {
  // 1.尝试拿到table_lock_request_queue
  std::shared_ptr<LockRequestQueue> row_lock_request_queue;
  {
    std::lock_guard<std::mutex> guard(row_lock_map_latch_);
    if (auto row_lock_request_queue_iterator = row_lock_map_.find(rid);
        row_lock_request_queue_iterator != row_lock_map_.end()) {
      row_lock_request_queue = row_lock_request_queue_iterator->second;
      row_lock_request_queue->latch_.lock();
    } else {  // 没有说明压根不持有该锁
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    }
  }

  // 2.若能拿到queue,则检查是否持有该锁(txn对一个行至多持有一种行锁)
  std::unique_lock<std::mutex> lock(row_lock_request_queue->latch_, std::adopt_lock);
  bool have_row_lock = false;
  auto iter = row_lock_request_queue->request_queue_.begin();
  for (; iter != row_lock_request_queue->request_queue_.end(); ++iter) {
    auto &request = *iter;
    if (request->txn_id_ == txn->GetTransactionId()) {
      BUSTUB_ASSERT(request->granted_ == true, "we unlock row lock have not been granted");
      have_row_lock = true;
      break;
    }
  }

  if (!have_row_lock) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  // 3.释放锁，更新state
  auto request = *iter;
  row_lock_request_queue->request_queue_.erase(iter);
  lock.unlock();
  // TODO(gukele): Finally, unlocking a resource should also grant any new lock requests for the resource (if possible).
  // ？？？
  row_lock_request_queue->cv_.notify_all();
  // 综合一下上述表述，当 force 被置为 true 时，解锁行将不会改变事务状态（如从GROWING 切换到 SHRINKING）。
  // 这将用于 Task 3 SeqScan 算子的实现中，可以及时释放不满足谓词的元组。
  // 或者死锁检测线程来释放txn所有的锁，并且aborted而不会勿该成shrinking
  if (!force) {
    UpdateTxnState(txn, request->lock_mode_);
  }
  UpdateTxnRowLockSet(txn, request.get(), false);
  return true;
}

void LockManager::UnlockAll() {
  // You probably want to unlock all table and txn locks here.
  // txn_manager_->BlockAllTransactions();
  std::lock_guard<std::shared_mutex> lock(txn_manager_->txn_map_mutex_);
  for (const auto &[txn_id, txn] : txn_manager_->txn_map_) {
    txn->LockTxn();
    // TODO(gukele) 在LockManager析构中使用使用，所以没有修改LockRequestQueue
    txn->GetSharedRowLockSet()->clear();
    txn->GetExclusiveRowLockSet()->clear();
    txn->GetIntentionSharedTableLockSet()->clear();
    txn->GetIntentionExclusiveTableLockSet()->clear();
    txn->GetSharedIntentionExclusiveTableLockSet()->clear();
    txn->UnlockTxn();
  }
}

void LockManager::ReleaseLocks(Transaction *txn) {
  /** Drop all row locks */
  txn->LockTxn();
  std::unordered_map<table_oid_t, std::unordered_set<RID>> row_lock_set;
  for (const auto &s_row_lock_set : *txn->GetSharedRowLockSet()) {
    for (auto rid : s_row_lock_set.second) {
      row_lock_set[s_row_lock_set.first].emplace(rid);
    }
  }
  for (const auto &x_row_lock_set : *txn->GetExclusiveRowLockSet()) {
    for (auto rid : x_row_lock_set.second) {
      row_lock_set[x_row_lock_set.first].emplace(rid);
    }
  }

  /** Drop all table locks */
  std::unordered_set<table_oid_t> table_lock_set;
  for (auto oid : *txn->GetSharedTableLockSet()) {
    table_lock_set.emplace(oid);
  }
  for (table_oid_t oid : *(txn->GetIntentionSharedTableLockSet())) {
    table_lock_set.emplace(oid);
  }
  for (auto oid : *txn->GetExclusiveTableLockSet()) {
    table_lock_set.emplace(oid);
  }
  for (auto oid : *txn->GetIntentionExclusiveTableLockSet()) {
    table_lock_set.emplace(oid);
  }
  for (auto oid : *txn->GetSharedIntentionExclusiveTableLockSet()) {
    table_lock_set.emplace(oid);
  }
  txn->UnlockTxn();

  for (const auto &locked_table_row_set : row_lock_set) {
    table_oid_t oid = locked_table_row_set.first;
    for (auto rid : locked_table_row_set.second) {
      UnlockRow(txn, oid, rid);
    }
  }

  for (auto oid : table_lock_set) {
    UnlockTable(txn, oid);
  }
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  std::lock_guard<std::mutex> guard(waits_for_latch_);
  waits_for_[t1].insert(t2);
  // TODO(gukele): just for test
  unsafe_nodes_.insert(t1);
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  std::lock_guard<std::mutex> guard(waits_for_latch_);
  if (auto iter = waits_for_.find(t1); iter != waits_for_.end()) {
    iter->second.erase(t2);
    // TODO(gukele): just for test
    if (iter->second.empty()) {
      unsafe_nodes_.erase(t1);
    }
  }
}

auto LockManager::HasCycle(txn_id_t *abort_txn_id) -> bool {
  // TODO(gukele): cmu的测试会只使用AddEdge、RemoveEdge、HasCycle去测试，所以AddEdge、RemoveEdge也要修改
  // TODO(gukele): 使用一个全局的unvisited/visited，并且不保留环的路径，似乎无法找到全部的环，
  // 例如2->3->4->2, 3->5->3, 2->6->2这样一个图，存在三个环，第一次HasCycle后发现了环，并且删除了边4,
  // 下次调用HasCycle时unvisited中只有5和6了，找不到第二个和第三个环,所以我们需要从上次发现环的路径继续开始
  std::lock_guard<std::mutex> guard(waits_for_latch_);

  // 再例如2->5->3->4->2  2->6->2  2->7->2  4->8->4 如果找到了第一个环，删除了点5，
  // 如果从删除的点开始继续寻找，那么4->8->4的环就丢了

  // 所以我们应该用最笨得方法，每次HasCycle都是重新DFS整个图寻找是否有环？

  // 优化1，当DFS一个极大连通子图(连通分量)都没有发现环的时候，那么整个极大连通子图的点下次就没有必要DFS了

  // 优化2

  while (!unsafe_nodes_.empty()) {
    // TODO(gukele): sort for test
    std::vector<txn_id_t> unsafe_nodes(unsafe_nodes_.begin(), unsafe_nodes_.end());
    std::sort(unsafe_nodes.begin(), unsafe_nodes.end());
    for (const auto &source_txn : unsafe_nodes) {
      // for(const auto &source_txn : unsafe_nodes_) {
      std::unordered_set<txn_id_t> on_path{};
      std::unordered_set<txn_id_t> visited{};
      std::unordered_set<txn_id_t> connected_component{};  // 连通分量(极大连通子图)
      if (DFSFindCycle(source_txn, on_path, visited, connected_component)) {
        *abort_txn_id = *on_path.begin();
        for (const auto &txn_id : on_path) {
          *abort_txn_id = std::max(*abort_txn_id, txn_id);
        }
        // RemoveVertex(*abort_txn_id);
        return true;
      }
      // source_txn所在的极大连通子图已经不存在环了
      for (const auto &safe_node : connected_component) {
        unsafe_nodes_.erase(safe_node);
      }
    }
  }
  return false;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::lock_guard<std::mutex> guard(waits_for_latch_);
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  for (const auto &[v1, v2_s] : waits_for_) {
    for (const auto &v2 : v2_s) {
      edges.emplace_back(v1, v2);
    }
  }
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
      BuildDeadlockDetectionGraph();
      txn_id_t abort_txn_id = INVALID_TXN_ID;
      while (HasCycle(&abort_txn_id)) {
        auto txn = txn_manager_->GetTransaction(abort_txn_id);
        // 1.拿到的锁释放放掉，先释放行锁
        // TODO(gukele): 根据测试的意思，只是单纯的set aborted,加锁失败返回false后再由TxnManager去释放锁
        // 我认为直接在这里释放锁就可以了,如果只是单纯的set aborted,那么你要通知自己，让自己加锁失败
        // txn_manager_->Abort(txn);

        // 找到abort_txn当前死锁的加锁请求，notify
        txn->SetState(TransactionState::ABORTED);
        {
          std::lock_guard<std::mutex> table_guard(table_lock_map_latch_);
          if (auto iter = waits_for_table_.find(abort_txn_id); iter != waits_for_table_.end()) {
            table_lock_map_[iter->second]->cv_.notify_all();
          }
        }
        {
          std::lock_guard<std::mutex> row_guard(row_lock_map_latch_);
          if (auto iter = waits_for_row_.find(abort_txn_id); iter != waits_for_row_.end()) {
            row_lock_map_[iter->second]->cv_.notify_all();
          }
        }

        // 2.更新wait_for_
        RemoveVertex(abort_txn_id);
      }
      std::lock_guard<std::mutex> guard(waits_for_latch_);
      waits_for_.clear();
      unsafe_nodes_.clear();
      waits_for_table_.clear();
      waits_for_row_.clear();
    }
  }
}

auto LockManager::UpgradeLockTable(Transaction *txn, LockMode lock_mode, LockRequestQueue *table_lock_request_queue,
                                   std::unique_lock<std::mutex> &lock) -> std::optional<bool> {
  /*
   * LOCK UPGRADE:
   *    Calling Lock() on a resource that is already locked should have the
   * following behaviour:
   *    - If requested lock mode is the same as that of the lock presently held,
   *      Lock() should return true since it already has the lock.
   *    - If requested lock mode is different, Lock() should upgrade the lock
   * held by the transaction.
   *
   *    A lock request being upgraded should be prioritised over other waiting
   * lock requests on the same resource.
   *
   *    While upgrading, only the following transitions should be allowed:
   *        IS -> [S, X, IX, SIX]
   *        S -> [X, SIX]
   *        IX -> [X, SIX]
   *        SIX -> [X]
   *    Any other upgrade is considered incompatible, and such an attempt should
   * set the TransactionState as ABORTED and throw a TransactionAbortException
   * (INCOMPATIBLE_UPGRADE)
   *
   *    Furthermore, only one transaction should be allowed to upgrade its lock
   * on a given resource. Multiple concurrent lock upgrades on the same resource
   * should set the TransactionState as ABORTED and throw a
   * TransactionAbortException (UPGRADE_CONFLICT).
   */
  for (auto iter = table_lock_request_queue->request_queue_.begin();
       iter != table_lock_request_queue->request_queue_.end(); ++iter) {
    auto request = *iter;
    // 发现锁升级事件
    if (request->txn_id_ == txn->GetTransactionId()) {
      BUSTUB_ASSERT(request->granted_ == true, "The previous lock request for the transaction should be granted");

      // 1. If requested lock mode is the same as that of the lock presently
      // held, Lock() should return true since it already has the lock.
      if (request->lock_mode_ == lock_mode) {
        return true;
      }

      // 2. 当前资源上如果另一个事务正在尝试升级会造成UPGRADE_CONFLICT冲突
      // NOTE(gukele) figure out why conflict
      /*
        T1 lock table in SHARED mode
        T2 lock table in SHARED mode
        T1 try to upgrade the lock to EXCLUSIVE mode, but conflict with T2,
        so T1 will Remove [T1, SHARED] from queue,Insert [T1, EXCLUSIVE, granted=false] into queue
        T2 upgrade to EXCLUSIVE but wait T1 E lock request
        so T2 will Remove [T2, SHARED] from queue,Insert [T2, EXCLUSIVE, granted=false] into queue
        then T1 granted E lock,


        如果允许同时2个upgrade会出现下面的情况
        T1 lock table in SHARED mode
        T2 lock table in SHARED mode
        T1 try to upgrade the lock to EXCLUSIVE mode, but conflict with T2,
        so T1 will Remove [T1, SHARED] from queue,Insert [T1, EXCLUSIVE, granted=false] into queue
        T2 upgrade to EXCLUSIVE without conflict 这样table就被SHARED+EXCLUSIVE lock了。
      */

      /*
       * Furthermore, only one transaction should be allowed to upgrade its
       * lock on a given resource. Multiple concurrent lock upgrades on the
       * same resource should set the TransactionState as ABORTED and throw a
       * TransactionAbortException (UPGRADE_CONFLICT).
       */
      if (table_lock_request_queue->upgrading_ != INVALID_TXN_ID) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      }

      // 3. 错误的lock upgrade
      if (!CanLockUpgrade(lock_mode, request->lock_mode_)) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }

      // 4. lock upgrade
      // table_lock_request_queue->upgrading_ = txn->GetTransactionId();
      auto upgraded_lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, request->oid_);
      // 删除旧的
      iter = table_lock_request_queue->request_queue_.erase(iter);
      UpdateTxnTableLockSet(txn, request.get(), false);
      // 插入新的
      // TODO(gukele): bug 是因为我们新锁插入了原来的位置，而无论你是锁升级还是普通加锁，我们都要FIFO，所以应该插入最后
      // 如果插入插入原来的位置，那么CanGrantLock默认认为自己请求之后的都是non-granted，无法检测当前自己request之后granted的
      // iter = table_lock_request_queue->request_queue_.insert(iter, upgraded_lock_request);
      iter = table_lock_request_queue->request_queue_.insert(table_lock_request_queue->request_queue_.end(),
                                                             upgraded_lock_request);

      // TODO(gukele): 放在这里更有利于并发？相当于自己释放了自己之前持有的一个锁
      table_lock_request_queue->upgrading_ = txn->GetTransactionId();

      // 会导致多次释放锁！
      // std::unique_lock<std::mutex> lock(table_lock_request_queue->latch_, std::adopt_lock);

      while (!CanGrantLock(table_lock_request_queue, upgraded_lock_request.get())) {
        table_lock_request_queue->cv_.wait(lock);

        // 查看事务当前的状态，如果是abort就notify，唤醒其它等待队列上阻塞的线程
        // 为什么事务阻塞在请求锁的过程中状态会有可能被设置为aborted？因为发生了死锁，被死锁检测进程给强行设置的
        if (txn->GetState() == TransactionState::ABORTED) {
          table_lock_request_queue->upgrading_ = INVALID_TXN_ID;
          table_lock_request_queue->request_queue_.erase(iter);
          lock.unlock();
          table_lock_request_queue->cv_.notify_all();
          return false;
        }
      }
      table_lock_request_queue->upgrading_ = INVALID_TXN_ID;
      upgraded_lock_request->granted_ = true;
      UpdateTxnTableLockSet(txn, upgraded_lock_request.get(), true);
      return true;
    }
  }
  return std::nullopt;
}

auto LockManager::UpgradeLockRow(Transaction *txn, LockMode lock_mode, LockRequestQueue *row_lock_request_queue,
                                 std::unique_lock<std::mutex> &lock) -> std::optional<bool> {
  for (auto iter = row_lock_request_queue->request_queue_.begin(); iter != row_lock_request_queue->request_queue_.end();
       ++iter) {
    auto request = *iter;
    if (request->txn_id_ == txn->GetTransactionId()) {
      BUSTUB_ASSERT(request->granted_ == true, "The previous lock request for the transaction should be granted");

      // 1.
      if (request->lock_mode_ == lock_mode) {
        return true;
      }

      // 2.
      if (row_lock_request_queue->upgrading_ != INVALID_TXN_ID) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      }

      // 3.
      // 行锁升级只有S -> X一种情况
      if (!CanLockUpgrade(lock_mode, request->lock_mode_)) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }

      // 4. lock upgrade
      auto upgraded_lock_request =
          std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, request->oid_, request->rid_);
      // 删除旧的
      iter = row_lock_request_queue->request_queue_.erase(iter);
      UpdateTxnRowLockSet(txn, request.get(), false);
      // 插入新的
      iter = row_lock_request_queue->request_queue_.insert(row_lock_request_queue->request_queue_.end(),
                                                           upgraded_lock_request);

      row_lock_request_queue->upgrading_ = txn->GetTransactionId();

      while (!CanGrantLock(row_lock_request_queue, upgraded_lock_request.get())) {
        row_lock_request_queue->cv_.wait(lock);

        // 查看事务当前的状态，如果是abort就notify，唤醒其它等待队列上阻塞的线程
        // 为什么事务阻塞在请求锁的过程中状态会有可能被设置为aborted？因为发生了死锁，被死锁检测进程给强行设置的
        if (txn->GetState() == TransactionState::ABORTED) {
          row_lock_request_queue->upgrading_ = INVALID_TXN_ID;
          row_lock_request_queue->request_queue_.erase(iter);
          lock.unlock();
          row_lock_request_queue->cv_.notify_all();
          return false;
        }
      }
      row_lock_request_queue->upgrading_ = INVALID_TXN_ID;
      upgraded_lock_request->granted_ = true;
      UpdateTxnRowLockSet(txn, upgraded_lock_request.get(), true);
      return true;
    }
  }
  return std::nullopt;
}

auto LockManager::AreLocksCompatible(LockMode l1, LockMode l2) -> bool {
  switch (l1) {
    case LockMode::SHARED:
      if (l2 == LockMode::EXCLUSIVE || l2 == LockMode::INTENTION_EXCLUSIVE ||
          l2 == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        return false;
      }
      break;
    case LockMode::EXCLUSIVE:
      return false;
      break;
    case LockMode::INTENTION_SHARED:
      if (l2 == LockMode::EXCLUSIVE) {
        return false;
      }
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      if (l2 == LockMode::SHARED || l2 == LockMode::SHARED_INTENTION_EXCLUSIVE || l2 == LockMode::EXCLUSIVE) {
        return false;
      }
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      if (l2 == LockMode::EXCLUSIVE || l2 == LockMode::SHARED || l2 == LockMode::INTENTION_EXCLUSIVE ||
          l2 == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        return false;
      }
      break;
    default:
      throw Exception(ExceptionType::EXECUTION, "impossible! not have other lock mode");
  }
  return true;
}

auto LockManager::CanTxnTakeLock(Transaction *txn, LockMode lock_mode) -> bool {
  // shrinking not allow X/IX
  if (txn->GetState() == TransactionState::SHRINKING) {
    if (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }

  /*
   * READ_UNCOMMITTED:
   *     The transaction is required to take only IX, X locks.
   *     X, IX locks are allowed in the GROWING state.
   *     S, IS, SIX locks are never allowed
   */
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
        lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
    if (txn->GetState() == TransactionState::SHRINKING) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }

  /*
   * READ_COMMITTED:
   *     The transaction is required to take all locks.
   *     All locks are allowed in the GROWING state
   *     Only IS, S locks are allowed in the SHRINKING state
   */
  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    if (txn->GetState() == TransactionState::SHRINKING) {
      if (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE ||
          lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
    }
  }

  /*
   * REPEATABLE_READ:
   *     The transaction is required to take all locks.
   *     All locks are allowed in the GROWING state
   *     No locks are allowed in the SHRINKING state
   */
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    if (txn->GetState() == TransactionState::SHRINKING) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  return true;
}

auto LockManager::CanGrantLock(LockRequestQueue *lock_request_queue, LockRequest *lock_request) -> bool {
  for (const auto &request : lock_request_queue->request_queue_) {
    if (request->granted_) {
      if (!AreLocksCompatible(request->lock_mode_, lock_request->lock_mode_)) {
        return false;
      }
    } else if (request.get() != lock_request) {  // FIFO 前边还有没有granted的
      return false;
    } else {
      // } else if (lock_request_queue->upgrading_ == INVALID_TXN_ID ||
      //            lock_request_queue->upgrading_ == lock_request->txn_id_) {
      // TODO(gukele): 并且没有锁升级的,或者是自己锁升级,其实不需要，因为那个锁升级插进去的是non-granted
      return true;
    }
  }
  throw Exception(ExceptionType::EXECUTION, "impossible!");
}

// auto LockManager::CanGrantRowLock(LockRequestQueue *row_lock_request_queue, LockRequest *lock_request) -> bool {

// }

void LockManager::GrantNewLocksIfPossible(LockRequestQueue *lock_request_queue) {
  throw Exception(ExceptionType::EXECUTION, "not implemented");
}

/*
 * While upgrading, only the following transitions should be allowed:
 *     IS -> [S, X, IX, SIX]
 *     S -> [X, SIX]
 *     IX -> [X, SIX]
 *     SIX -> [X]
 * Any other upgrade is considered incompatible, and such an attempt should set
 * the TransactionState as ABORTED and throw a TransactionAbortException
 * (INCOMPATIBLE_UPGRADE)
 */
auto LockManager::CanLockUpgrade(LockMode curr_lock_mode, LockMode requested_lock_mode) -> bool {
  if (!(requested_lock_mode == LockMode::INTENTION_SHARED &&
        (curr_lock_mode == LockMode::SHARED || curr_lock_mode == LockMode::EXCLUSIVE ||
         curr_lock_mode == LockMode::INTENTION_EXCLUSIVE || curr_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) &&
      !(requested_lock_mode == LockMode::SHARED &&
        (curr_lock_mode == LockMode::EXCLUSIVE || curr_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) &&
      !(requested_lock_mode == LockMode::INTENTION_EXCLUSIVE &&
        (curr_lock_mode == LockMode::EXCLUSIVE || curr_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) &&
      !(requested_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE && curr_lock_mode == LockMode::EXCLUSIVE)) {
    void();
    return false;
  }
  return true;
}

auto LockManager::CheckAppropriateLockOnTable(Transaction *txn, const table_oid_t &oid, LockMode row_lock_mode)
    -> bool {
  txn->LockTxn();
  bool result = false;
  switch (row_lock_mode) {
    case LockMode::SHARED: {
      auto i_s_table_locked = txn->IsTableIntentionSharedLocked(oid);
      auto s_table_locked = txn->IsTableSharedLocked(oid);
      auto i_x_table_locked = txn->IsTableIntentionExclusiveLocked(oid);
      auto s_i_x_table_locked = txn->IsTableSharedIntentionExclusiveLocked(oid);
      auto x_table_locked = txn->IsTableExclusiveLocked(oid);
      result = i_s_table_locked || s_table_locked || i_x_table_locked || s_i_x_table_locked || x_table_locked;
    } break;
    case LockMode::EXCLUSIVE: {
      auto i_x_table_locked = txn->IsTableIntentionExclusiveLocked(oid);
      auto s_i_x_table_locked = txn->IsTableSharedIntentionExclusiveLocked(oid);
      auto x_table_locked = txn->IsTableExclusiveLocked(oid);
      result = i_x_table_locked || s_i_x_table_locked || x_table_locked;
    } break;
    default:
      txn->UnlockTxn();
      throw Exception(ExceptionType::EXECUTION, "impossible! error row_lock_mode!");
      break;
  }
  txn->UnlockTxn();
  return result;
}

auto LockManager::CheckNotHoldAppropriateLockOnRow(Transaction *txn, const table_oid_t &oid, LockMode table_lock_mode)
    -> bool {
  txn->LockTxn();
  // bool result = false;
  // switch (table_lock_mode)
  // {
  //   case LockMode::INTENTION_SHARED:
  //     {
  //       auto s_lock_set =  *txn->GetSharedRowLockSet();
  //       result = s_lock_set.find(oid) == s_lock_set.end() ||
  //       s_lock_set[oid].empty();
  //     }
  //     break;
  //   case LockMode::INTENTION_EXCLUSIVE:
  //   case LockMode::SHARED_INTENTION_EXCLUSIVE:
  //     {
  //       auto x_lock_set =  *txn->GetExclusiveRowLockSet();
  //       result = x_lock_set.find(oid) == x_lock_set.end() ||
  //       x_lock_set[oid].empty();
  //     }
  //     break;
  //   default:
  //     // TODO(gukele): 持有S/X表锁，应该就不会有对应表的行锁吧，但是如果是表级锁IS->S升级，那么可能会持有行锁？
  //     {
  //       auto s_lock_set =  *txn->GetSharedRowLockSet();
  //       auto x_lock_set =  *txn->GetExclusiveRowLockSet();
  //       BUSTUB_ASSERT((s_lock_set.find(oid) == s_lock_set.end() ||
  //                     s_lock_set[oid].empty()) &&
  //                     (x_lock_set.find(oid) == x_lock_set.end() ||
  //                     x_lock_set[oid].empty()), "持有S/X表锁,
  //                     但是仍然含有行锁");
  //       txn->UnlockTxn();
  //     }
  //     break;
  // }

  // TODO(gukele): 可能因为锁升级，所以无法根据现在表锁判断是否持有某些行锁，例如IS表锁升级成IX表锁，此时可能持有S行锁
  // 例如 IX表锁升级成X表锁，此时可能持有X行锁
  // 所以UnlockTable时检查不持有对应表的任何行锁
  auto s_lock_set = *txn->GetSharedRowLockSet();
  auto x_lock_set = *txn->GetExclusiveRowLockSet();
  bool result = (s_lock_set.find(oid) == s_lock_set.end() || s_lock_set[oid].empty()) &&
                (x_lock_set.find(oid) == x_lock_set.end() || x_lock_set[oid].empty());
  txn->UnlockTxn();
  return result;
}

auto LockManager::DFSFindCycle(txn_id_t source_txn, std::unordered_set<txn_id_t> &on_path,
                               std::unordered_set<txn_id_t> &visited, std::unordered_set<txn_id_t> &connected_component)
    -> bool {
  // dfs找到环？ 如果邻居中存在节点是DFS路径上的节点，那么就说明出现了环
  visited.insert(source_txn);
  on_path.insert(source_txn);
  connected_component.insert(source_txn);
  if (auto iter = waits_for_.find(source_txn); iter != waits_for_.end()) {
    // TODO(gukele): sort for test
    std::vector<txn_id_t> next_nodes(iter->second.begin(), iter->second.end());
    std::sort(next_nodes.begin(), next_nodes.end());
    for (const auto &next_node : next_nodes) {
      // for (const auto &next_node : iter->second) {
      if (visited.count(next_node) == 0) {
        if (DFSFindCycle(next_node, on_path, visited, connected_component)) {
          return true;
        }
      } else if (on_path.count(next_node) == 1) {
        return true;
      }
      // 2->5->3->4->2  6->3->6
    }
  }
  std::string str;
  for (const auto &txn_id : on_path) {
    str += " " + std::to_string(txn_id);
  }
  on_path.erase(source_txn);
  return false;
}

auto LockManager::DFSFindCycle(txn_id_t source_txn, std::unordered_set<txn_id_t> &on_path,
                               std::unordered_set<txn_id_t> &unvisited, txn_id_t *abort_txn_id) -> bool {
  // dfs找到环？ 如果邻居中存在节点是DFS路径上的节点，那么就说明出现了环
  if (auto iter = waits_for_.find(source_txn); iter != waits_for_.end()) {
    for (const auto &v2 : iter->second) {
      if (unvisited.count(v2) == 1) {
        unvisited.erase(v2);
        on_path.insert(v2);
        if (DFSFindCycle(v2, on_path, unvisited, abort_txn_id)) {
          return true;
        }
        on_path.erase(v2);
      } else if (on_path.count(v2) == 1) {
        *abort_txn_id = *on_path.begin();
        for (const auto &txn_id : on_path) {
          *abort_txn_id = std::max(*abort_txn_id, txn_id);
        }
        return true;
      }
    }
  }
  return false;
}

void LockManager::RemoveVertex(txn_id_t txn_id) {
  std::lock_guard<std::mutex> guard(waits_for_latch_);
  unsafe_nodes_.erase(txn_id);
  waits_for_.erase(txn_id);
  // 其实为了检测死锁，没必要把指向aborted_txn_id的边也去除
  for (auto &[_, v2_s] : waits_for_) {
    v2_s.erase(txn_id);
  }
}

void LockManager::UpdateTxnTableLockSet(Transaction *txn, LockRequest *table_lock_request, bool is_insert) {
  std::unordered_set<table_oid_t> *table_lock_set;
  txn->LockTxn();
  switch (table_lock_request->lock_mode_) {
    case LockMode::SHARED:
      table_lock_set = txn->GetSharedTableLockSet().get();
      break;
    case LockMode::EXCLUSIVE:
      table_lock_set = txn->GetExclusiveTableLockSet().get();
      break;
    case LockMode::INTENTION_SHARED:
      table_lock_set = txn->GetIntentionSharedTableLockSet().get();
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      table_lock_set = txn->GetIntentionExclusiveTableLockSet().get();
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      table_lock_set = txn->GetSharedIntentionExclusiveTableLockSet().get();
      break;
    default:
      txn->UnlockTxn();
      throw Exception(ExceptionType::EXECUTION, "Error lock mode");
      break;
  }

  if (is_insert) {
    // BUSTUB_ASSERT(table_lock_set->count(request->oid_) == 0, "Already have");
    table_lock_set->insert(table_lock_request->oid_);
  } else {
    // BUSTUB_ASSERT(table_lock_set->count(request->oid_) == 1, "Do not exist");
    table_lock_set->erase(table_lock_request->oid_);
  }
  txn->UnlockTxn();
}

void LockManager::UpdateTxnRowLockSet(Transaction *txn, LockRequest *row_lock_request, bool is_insert) {
  std::unordered_map<table_oid_t, std::unordered_set<RID>> *row_lock_set;
  txn->LockTxn();
  switch (row_lock_request->lock_mode_) {
    case LockMode::SHARED:
      row_lock_set = txn->GetSharedRowLockSet().get();
      break;
    case LockMode::EXCLUSIVE:
      row_lock_set = txn->GetExclusiveRowLockSet().get();
      break;
    default:
      txn->UnlockTxn();
      throw Exception(ExceptionType::EXECUTION, "Error row lock mode");
      break;
  }

  if (is_insert) {
    (*row_lock_set)[row_lock_request->oid_].insert(row_lock_request->rid_);
  } else {
    // row_lock_set[row_lock_request->oid_].erase(row_lock_request->rid_);
    if (const auto &set_iter = row_lock_set->find(row_lock_request->oid_); set_iter != row_lock_set->end()) {
      set_iter->second.erase(row_lock_request->rid_);
    }
  }
  txn->UnlockTxn();
}

auto LockManager::UpdateTxnState(Transaction *txn, LockMode unlock_mode) -> bool {
  /*
   *    Only unlocking S or X locks changes transaction state.
   *
   *    REPEATABLE_READ:
   *        Unlocking S/X locks should set the transaction state to SHRINKING
   *
   *    READ_COMMITTED:
   *        Unlocking X locks should set the transaction state to SHRINKING.
   *        Unlocking S locks does not affect transaction state.
   *
   *   READ_UNCOMMITTED:
   *        Unlocking X locks should set the transaction state to SHRINKING.
   *        S locks are not permitted under READ_UNCOMMITTED.
   *            The behaviour upon unlocking an S lock under this isolation level is undefined.
   */
  switch (txn->GetIsolationLevel()) {
    case IsolationLevel::REPEATABLE_READ:
      if (unlock_mode == LockMode::SHARED || unlock_mode == LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::SHRINKING);
        return true;
      }
      break;
    case IsolationLevel::READ_COMMITTED:
    case IsolationLevel::READ_UNCOMMITTED:
      if (unlock_mode == LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::SHRINKING);
        return true;
      }
      break;
    default:
      throw Exception(ExceptionType::EXECUTION, "impossible! Error isolation level");
  }
  return false;
}

void LockManager::AddWaitsForTable(txn_id_t txn_id, table_oid_t oid) {
  std::lock_guard<std::mutex> guard(waits_for_latch_);
  waits_for_table_[txn_id] = oid;
}
void LockManager::AddWaitsForRow(txn_id_t txn_id, RID rid) {
  std::lock_guard<std::mutex> guard(waits_for_latch_);
  waits_for_row_[txn_id] = rid;
}

void LockManager::BuildDeadlockDetectionGraph() {
  // 找到所有granted,然后non-granted看是否与granted冲突，冲突则需要等待
  waits_for_.clear();
  unsafe_nodes_.clear();
  waits_for_table_.clear();
  waits_for_row_.clear();
  std::unique_lock<std::mutex> table_lock(table_lock_map_latch_);
  std::unique_lock<std::mutex> row_lock(row_lock_map_latch_);
  // 1.表级建图
  for (const auto &[_, table_lock_request_queue_ptr] : table_lock_map_) {
    auto &request_queue = table_lock_request_queue_ptr->request_queue_;
    auto non_granted = request_queue.begin();
    for (; non_granted != request_queue.end(); ++non_granted) {
      if (!(*non_granted)->granted_) {
        break;
      }
    }
    auto end = non_granted;
    for (; non_granted != request_queue.end(); ++non_granted) {
      for (auto granted = request_queue.begin(); granted != end; ++granted) {
        if (!AreLocksCompatible((*non_granted)->lock_mode_, (*granted)->lock_mode_)) {
          AddEdge((*non_granted)->txn_id_, (*granted)->txn_id_);
          AddWaitsForTable((*non_granted)->txn_id_, (*non_granted)->oid_);
        }
      }
    }
  }
  table_lock.unlock();
  // 2.行级建图
  for (const auto &[_, row_lock_request_queue_ptr] : row_lock_map_) {
    auto &request_queue = row_lock_request_queue_ptr->request_queue_;
    auto non_granted = request_queue.begin();
    for (; non_granted != request_queue.end(); ++non_granted) {
      if (!(*non_granted)->granted_) {
        break;
      }
    }
    auto end = non_granted;
    for (; non_granted != request_queue.end(); ++non_granted) {
      for (auto granted = request_queue.begin(); granted != end; ++granted) {
        if (!AreLocksCompatible((*non_granted)->lock_mode_, (*granted)->lock_mode_)) {
          AddEdge((*non_granted)->txn_id_, (*granted)->txn_id_);
          AddWaitsForRow((*non_granted)->txn_id_, (*non_granted)->rid_);
        }
      }
    }
  }
  row_lock.unlock();
  // 3.构建unvisited、unsafe_nodes_
  std::lock_guard<std::mutex> guard(waits_for_latch_);
  unsafe_nodes_.reserve(waits_for_.size());
  for (const auto &[txn_id, _] : waits_for_) {
    unsafe_nodes_.insert(txn_id);
  }
}

}  // namespace bustub
