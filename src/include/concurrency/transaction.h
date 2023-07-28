//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction.h
//
// Identification: src/include/concurrency/transaction.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <fmt/format.h>
#include <atomic>
#include <deque>
#include <memory>
#include <string>
#include <thread>  // NOLINT
#include <unordered_map>
#include <unordered_set>

#include "common/config.h"
#include "common/logger.h"
#include "storage/page/page.h"
#include "storage/table/tuple.h"

namespace bustub {

// NOTE(gukele)

/**
 * lock 与 latch的区别
 * latch：针对多线程来说的，保护内存数据结构
 * lock：针对事务来说，实际上最终读写tuple都是需要页级别的latch
 */

/**
 * 只有释放X、S锁才可能进入shrinking阶段，X锁的释放一定会转入shrinking。
 *
 * 读未提交：
 *    释放X锁才会转入shrinking。在shrinking阶段不可以加任何锁；在growing阶段可以加X、IX锁。
 *    读未提交不允许加S、IS、SIX锁。
 *    实际上就是普通两阶段锁，存在脏读、级联abort问题。
 *
 * 读提交：
 *    只有释放X锁才会转入shrinking。在shrinking阶段只有IS、S可以加锁；在growing阶段可以加任何锁。
 *    相当于S锁不受两阶段锁影响，那么实际上就是严格两阶段锁，仍然存在不可重复读问题。
 *
 * 可重复读：
 *    释放X/S锁都会转入shrinking。在shrinking阶段不可以加任何锁；在growing阶段可以加任何锁。
 *    实际上就是强严格两阶段锁。
 */

/**
 * MVCC（并发版本控制）
 *     在MVCC中，有了当前读的概念，此时读提交和可重复读都不需要加读锁了，因为undo log和一致性视图可以找到历史版本。
 *     普通读不需要加锁，只有特殊的读，例如SELECT … LOCK IN SHARE MODE/FOR UPDATE的读会加读锁/写锁
 *
 */

/**
 * Two-phase Locking 两阶段锁协议
 * 1、增长阶段（growing phase）：事务可以获得锁，但不能释放锁。
 * 2、缩减阶段（shrinking phase）：事务可以释放锁，但不能获得新锁。
 * 可以证明，若并发执行的所有事务均遵守两段锁协议(充分条件)，则对这些事务的任何并发调度策略都是可串行化的。
 *
 * Strict Two-phase Locking 严格两阶段锁: commit后才释放所有的排他锁
 * 两阶段可能会出现脏读(读未提交)、级联abort(第一个事物abort,而第二个事物在第一个事物最终没有commit的基础上脏读，同样需要跟着abort)。
 * 简单粗暴的解决级联abort的方法就是commit后再释放该事物所有的排他锁
 *
 * Strong Strict Two-phase Locking 强两阶段锁: commit后才释放所有锁
 * 严格两阶段锁可能会出现不可重复读(例如读提交下，shrinking可以拿读锁，那么就可能出现不可重复读)
 *
 * Transaction states for 2PL:
 *
 *     _________________________
 *    |                         v
 * GROWING -> SHRINKING -> COMMITTED   ABORTED
 *    |__________|________________________^
 *
 * Transaction states for Non-2PL:
 *     __________
 *    |          v
 * GROWING  -> COMMITTED     ABORTED
 *    |_________________________^
 *
 **/
// abort/rollback
enum class TransactionState { GROWING, SHRINKING, COMMITTED, ABORTED };

/**
 * ACID
 * Atomicity: all or nothing (undo log)
 * Consistency: it looks correct to me
 * Isolation: READ_UNCOMMITTED, REPEATABLE_READ, READ_COMMITTED, Serial串行化
 * Durability: survive failure
 */

/**
 *
 *
 */

/**
 * Transaction isolation level.
 */
enum class IsolationLevel { READ_UNCOMMITTED, REPEATABLE_READ, READ_COMMITTED };

/**
 * Type of write operation.
 */
enum class WType { INSERT = 0, DELETE, UPDATE };

class TableHeap;
class Catalog;
using table_oid_t = uint32_t;
using index_oid_t = uint32_t;

/**
 * WriteRecord tracks information related to a write.
 */
class TableWriteRecord {
 public:
  // NOLINTNEXTLINE
  TableWriteRecord(table_oid_t tid, RID rid, TableHeap *table_heap) : tid_(tid), rid_(rid), table_heap_(table_heap) {}

  table_oid_t tid_;
  RID rid_;
  TableHeap *table_heap_;

  // 无法利用TableWriteRecord来恢复原地修改啊！！就一个rid,根本不知道原地修改之前的tuple是什么啊
  // NOTE(gukele): 所以用IndexWriteRecord wtype_=update的时候表示这是tuple原地修改而非index修改
  // Recording write type might be useful if you want to implement in-place update for leaderboard optimization.
  // You don't need it for the basic implementation.
  WType wtype_;
};

/**
 * WriteRecord tracks information related to a write.
 */
class IndexWriteRecord {
 public:
  // NOLINTNEXTLINE
  IndexWriteRecord(RID rid, table_oid_t table_oid, WType wtype, const Tuple &tuple, index_oid_t index_oid,
                   Catalog *catalog)
      : rid_(rid), table_oid_(table_oid), wtype_(wtype), tuple_(tuple), index_oid_(index_oid), catalog_(catalog) {}

  /**
   * Note(spring2023): I don't know what are these for. If you are implementing leaderboard optimizations, you can
   * figure out how to use this structure to store what you need.
   */

  // TODO(gukele): IndexWriteRecord结构设计似乎有些不合理

  /** The rid is the value stored in the index. */
  RID rid_;
  /** Table oid. */
  table_oid_t table_oid_;
  /** Write type. */
  WType wtype_;
  /** The tuple is used to construct an index key. */
  // 目前我改成直接表示key tuple，但是TableWriteRecord无法回复原地修改的tuple,所以还是表示tuple吧
  Tuple tuple_;
  /** The old tuple is only used for the update operation. */
  // 我直接改成了old key tuple？？？
  // 还是说我保存成old_tuple,然后当WType是update时，也表示我是原地更改了table,不太行，如果一个索引都没插入成功呢
  // 但是TableWriteRecord无法回复原地修改的tuple,所以还是表示tuple吧
  Tuple old_tuple_;
  /** Each table has an index list, this is the identifier of an index into the list. */
  index_oid_t index_oid_;
  /** The catalog contains metadata required to locate index. */
  Catalog *catalog_;
};

/**
 * Reason to a transaction abortion
 */
enum class AbortReason {
  LOCK_ON_SHRINKING,
  UPGRADE_CONFLICT,
  LOCK_SHARED_ON_READ_UNCOMMITTED,
  TABLE_LOCK_NOT_PRESENT,
  ATTEMPTED_INTENTION_LOCK_ON_ROW,
  TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS,
  INCOMPATIBLE_UPGRADE,
  ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD
};

/**
 * TransactionAbortException is thrown when state of a transaction is changed to ABORTED
 */
class TransactionAbortException : public std::exception {
  txn_id_t txn_id_;
  AbortReason abort_reason_;

 public:
  explicit TransactionAbortException(txn_id_t txn_id, AbortReason abort_reason)
      : txn_id_(txn_id), abort_reason_(abort_reason) {
    std::cerr << GetInfo() << std::endl;
  }
  auto GetTransactionId() -> txn_id_t { return txn_id_; }
  auto GetAbortReason() -> AbortReason { return abort_reason_; }
  auto GetInfo() -> std::string {
    switch (abort_reason_) {
      case AbortReason::LOCK_ON_SHRINKING:
        return "Transaction " + std::to_string(txn_id_) +
               " aborted because it can not take locks in the shrinking state\n";
      case AbortReason::UPGRADE_CONFLICT:
        return "Transaction " + std::to_string(txn_id_) +
               " aborted because another transaction is already waiting to upgrade its lock\n";
      case AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED:
        return "Transaction " + std::to_string(txn_id_) + " aborted on lock shared on READ_UNCOMMITTED\n";
      case AbortReason::TABLE_LOCK_NOT_PRESENT:
        return "Transaction " + std::to_string(txn_id_) + " aborted because table lock not present\n";
      case AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW:
        return "Transaction " + std::to_string(txn_id_) + " aborted because intention lock attempted on row\n";
      case AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS:
        return "Transaction " + std::to_string(txn_id_) +
               " aborted because table locks dropped before dropping row locks\n";
      case AbortReason::INCOMPATIBLE_UPGRADE:
        return "Transaction " + std::to_string(txn_id_) + " aborted because attempted lock upgrade is incompatible\n";
      case AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD:
        return "Transaction " + std::to_string(txn_id_) + " aborted because attempted to unlock but no lock held \n";
    }
    // Todo: Should fail with unreachable.
    return "";
  }
};

/**
 * Transaction tracks information related to a transaction.
 */
class Transaction {
 public:
  explicit Transaction(txn_id_t txn_id, IsolationLevel isolation_level = IsolationLevel::REPEATABLE_READ)
      : isolation_level_(isolation_level),
        thread_id_(std::this_thread::get_id()),
        txn_id_(txn_id),
        prev_lsn_(INVALID_LSN),
        s_table_lock_set_{new std::unordered_set<table_oid_t>},
        x_table_lock_set_{new std::unordered_set<table_oid_t>},
        is_table_lock_set_{new std::unordered_set<table_oid_t>},
        ix_table_lock_set_{new std::unordered_set<table_oid_t>},
        six_table_lock_set_{new std::unordered_set<table_oid_t>},
        s_row_lock_set_{new std::unordered_map<table_oid_t, std::unordered_set<RID>>},
        x_row_lock_set_{new std::unordered_map<table_oid_t, std::unordered_set<RID>>} {
    // Initialize the sets that will be tracked.
    table_write_set_ = std::make_shared<std::deque<TableWriteRecord>>();
    index_write_set_ = std::make_shared<std::deque<IndexWriteRecord>>();
    page_set_ = std::make_shared<std::deque<bustub::Page *>>();
    deleted_page_set_ = std::make_shared<std::unordered_set<page_id_t>>();
  }

  ~Transaction() = default;

  DISALLOW_COPY(Transaction);

  /** @return the id of the thread running the transaction */
  inline auto GetThreadId() const -> std::thread::id { return thread_id_; }

  /** @return the id of this transaction */
  inline auto GetTransactionId() const -> txn_id_t { return txn_id_; }

  /** @return the isolation level of this transaction */
  inline auto GetIsolationLevel() const -> IsolationLevel { return isolation_level_; }

  /** @return the list of table write records of this transaction */
  inline auto GetWriteSet() -> std::shared_ptr<std::deque<TableWriteRecord>> { return table_write_set_; }

  /** @return the list of index write records of this transaction */
  inline auto GetIndexWriteSet() -> std::shared_ptr<std::deque<IndexWriteRecord>> { return index_write_set_; }

  /** @return the page set */
  inline auto GetPageSet() -> std::shared_ptr<std::deque<Page *>> { return page_set_; }

  /**
   * Adds a tuple write record into the table write set.
   * @param write_record write record to be added
   */
  inline void AppendTableWriteRecord(const TableWriteRecord &write_record) {
    table_write_set_->push_back(write_record);
  }

  /**
   * Adds an index write record into the index write set.
   * @param write_record write record to be added
   */
  inline void AppendIndexWriteRecord(const IndexWriteRecord &index_write_record) {
    index_write_set_->push_back(index_write_record);
  }

  /**
   * Adds a page into the page set.
   * @param page page to be added
   */
  inline void AddIntoPageSet(Page *page) { page_set_->push_back(page); }

  /** @return the deleted page set */
  inline auto GetDeletedPageSet() -> std::shared_ptr<std::unordered_set<page_id_t>> { return deleted_page_set_; }

  /**
   * Adds a page to the deleted page set.
   * @param page_id id of the page to be marked as deleted
   */
  inline void AddIntoDeletedPageSet(page_id_t page_id) { deleted_page_set_->insert(page_id); }

  // TODO(gukele): 应该提供Add/Delete带锁修改lock_set_的接口,而不是像下面一样提供Getter，还需要自己加锁

  /** @return the set of rows under a shared lock */
  inline auto GetSharedRowLockSet() -> std::shared_ptr<std::unordered_map<table_oid_t, std::unordered_set<RID>>> {
    return s_row_lock_set_;
  }

  /** @return the set of rows in under an exclusive lock */
  inline auto GetExclusiveRowLockSet() -> std::shared_ptr<std::unordered_map<table_oid_t, std::unordered_set<RID>>> {
    return x_row_lock_set_;
  }

  /** @return the set of resources under a shared lock */
  inline auto GetSharedTableLockSet() -> std::shared_ptr<std::unordered_set<table_oid_t>> { return s_table_lock_set_; }
  inline auto GetExclusiveTableLockSet() -> std::shared_ptr<std::unordered_set<table_oid_t>> {
    return x_table_lock_set_;
  }
  inline auto GetIntentionSharedTableLockSet() -> std::shared_ptr<std::unordered_set<table_oid_t>> {
    return is_table_lock_set_;
  }
  inline auto GetIntentionExclusiveTableLockSet() -> std::shared_ptr<std::unordered_set<table_oid_t>> {
    return ix_table_lock_set_;
  }
  inline auto GetSharedIntentionExclusiveTableLockSet() -> std::shared_ptr<std::unordered_set<table_oid_t>> {
    return six_table_lock_set_;
  }

  /** @return true if rid (belong to table oid) is shared locked by this transaction */
  auto IsRowSharedLocked(const table_oid_t &oid, const RID &rid) -> bool {
    auto row_lock_set = s_row_lock_set_->find(oid);
    if (row_lock_set == s_row_lock_set_->end()) {
      return false;
    }
    return row_lock_set->second.find(rid) != row_lock_set->second.end();
  }

  /** @return true if rid (belong to table oid) is exclusive locked by this transaction */
  auto IsRowExclusiveLocked(const table_oid_t &oid, const RID &rid) -> bool {
    auto row_lock_set = x_row_lock_set_->find(oid);
    if (row_lock_set == x_row_lock_set_->end()) {
      return false;
    }
    return row_lock_set->second.find(rid) != row_lock_set->second.end();
  }

  auto IsTableIntentionSharedLocked(const table_oid_t &oid) -> bool {
    return is_table_lock_set_->find(oid) != is_table_lock_set_->end();
  }

  auto IsTableSharedLocked(const table_oid_t &oid) -> bool {
    return s_table_lock_set_->find(oid) != s_table_lock_set_->end();
  }

  auto IsTableIntentionExclusiveLocked(const table_oid_t &oid) -> bool {
    return ix_table_lock_set_->find(oid) != ix_table_lock_set_->end();
  }

  auto IsTableExclusiveLocked(const table_oid_t &oid) -> bool {
    return x_table_lock_set_->find(oid) != x_table_lock_set_->end();
  }

  auto IsTableSharedIntentionExclusiveLocked(const table_oid_t &oid) -> bool {
    return six_table_lock_set_->find(oid) != six_table_lock_set_->end();
  }

  /** @return the current state of the transaction */
  inline auto GetState() -> TransactionState { return state_; }

  inline auto LockTxn() -> void { latch_.lock(); }

  inline auto UnlockTxn() -> void { latch_.unlock(); }

  /**
   * Set the state of the transaction.
   * @param state new state
   */
  inline void SetState(TransactionState state) { state_ = state; }

  /** @return the previous LSN */
  inline auto GetPrevLSN() -> lsn_t { return prev_lsn_; }

  /**
   * Set the previous LSN.
   * @param prev_lsn new previous lsn
   */
  inline void SetPrevLSN(lsn_t prev_lsn) { prev_lsn_ = prev_lsn; }

 private:
  /** The current transaction state. */
  // TODO(gukele): 原子变量 或者 setter/getter中加锁
  // TransactionState state_{TransactionState::GROWING};
  std::atomic<TransactionState> state_{TransactionState::GROWING};

  // TODO(gukele): const?
  /** The isolation level of the transaction. */
  IsolationLevel isolation_level_;
  /** The thread ID, used in single-threaded transactions. */
  std::thread::id thread_id_;
  /** The ID of this transaction. */
  txn_id_t txn_id_;

  /** The undo set of table tuples. */
  std::shared_ptr<std::deque<TableWriteRecord>> table_write_set_;
  /** The undo set of indexes. */
  std::shared_ptr<std::deque<IndexWriteRecord>> index_write_set_;
  /** The LSN of the last record written by the transaction. */
  lsn_t prev_lsn_;

  // TODO(gukele): 一个事物可能对应多个线程？或者在算子执行的过程中本身就是多线程？
  // LockManager中deadlock detection线程至多读写state_
  std::mutex latch_;

  /** Concurrent index: the pages that were latched during index operation. */
  std::shared_ptr<std::deque<Page *>> page_set_;
  /** Concurrent index: the page IDs that were deleted during index operation.*/
  std::shared_ptr<std::unordered_set<page_id_t>> deleted_page_set_;

  /** LockManager: the set of table locks held by this transaction. */
  std::shared_ptr<std::unordered_set<table_oid_t>> s_table_lock_set_;
  std::shared_ptr<std::unordered_set<table_oid_t>> x_table_lock_set_;
  std::shared_ptr<std::unordered_set<table_oid_t>> is_table_lock_set_;
  std::shared_ptr<std::unordered_set<table_oid_t>> ix_table_lock_set_;
  std::shared_ptr<std::unordered_set<table_oid_t>> six_table_lock_set_;

  /** LockManager: the set of row locks held by this transaction. */
  std::shared_ptr<std::unordered_map<table_oid_t, std::unordered_set<RID>>> s_row_lock_set_;
  std::shared_ptr<std::unordered_map<table_oid_t, std::unordered_set<RID>>> x_row_lock_set_;
};

}  // namespace bustub

template <>
struct fmt::formatter<bustub::IsolationLevel> : formatter<std::string_view> {
  // parse is inherited from formatter<string_view>.
  template <typename FormatContext>
  auto format(bustub::IsolationLevel x, FormatContext &ctx) const {
    using bustub::IsolationLevel;
    string_view name = "unknown";
    switch (x) {
      case IsolationLevel::READ_UNCOMMITTED:
        name = "READ_UNCOMMITTED";
        break;
      case IsolationLevel::READ_COMMITTED:
        name = "READ_COMMITTED";
        break;
      case IsolationLevel::REPEATABLE_READ:
        name = "REPEATABLE_READ";
        break;
    }
    return formatter<string_view>::format(name, ctx);
  }
};
