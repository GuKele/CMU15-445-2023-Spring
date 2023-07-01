/**
 * lock_manager_test.cpp
 */

#include <exception>
#include <random>
#include <thread>  // NOLINT

#include "common/config.h"
#include "common/rid.h"
#include "common_checker.h"  // NOLINT
#include "concurrency/lock_manager.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

#include "gtest/gtest.h"

#define TEST_TIMEOUT_BEGIN                           \
  std::promise<bool> promisedFinished;               \
  auto futureResult = promisedFinished.get_future(); \
  std::thread([](std::promise<bool>& finished) {
#define TEST_TIMEOUT_FAIL_END(X)                                                                  \
  finished.set_value(true);                                                                       \
  }, std::ref(promisedFinished)).detach();                                                        \
  EXPECT_TRUE(futureResult.wait_for(std::chrono::milliseconds(X)) != std::future_status::timeout) \
      << "Test Failed Due to Time Out";

namespace bustub {

/*
 * This test is only a sanity check. Please do not rely on this test
 * to check the correctness.
 */

// --- Helper functions ---
void CheckGrowing(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::GROWING); }

void CheckShrinking(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::SHRINKING); }

void CheckAborted(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::ABORTED); }

void CheckCommitted(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::COMMITTED); }

void CheckTxnRowLockSize(Transaction *txn, table_oid_t oid, size_t shared_size, size_t exclusive_size) {
  bool correct = true;
  correct = correct && (*txn->GetSharedRowLockSet())[oid].size() == shared_size;
  correct = correct && (*txn->GetExclusiveRowLockSet())[oid].size() == exclusive_size;
  if (!correct) {
    fmt::print("row lock size incorrect for txn={} oid={}: expected (S={} X={}), actual (S={} X={})\n",
               txn->GetTransactionId(), oid, shared_size, exclusive_size, (*txn->GetSharedRowLockSet())[oid].size(),
               (*txn->GetExclusiveRowLockSet())[oid].size());
  }
  EXPECT_TRUE(correct);
}

int GetTxnTableLockSize(Transaction *txn, LockManager::LockMode lock_mode) {
  switch (lock_mode) {
    case LockManager::LockMode::SHARED:
      return txn->GetSharedTableLockSet()->size();
    case LockManager::LockMode::EXCLUSIVE:
      return txn->GetExclusiveTableLockSet()->size();
    case LockManager::LockMode::INTENTION_SHARED:
      return txn->GetIntentionSharedTableLockSet()->size();
    case LockManager::LockMode::INTENTION_EXCLUSIVE:
      return txn->GetIntentionExclusiveTableLockSet()->size();
    case LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE:
      return txn->GetSharedIntentionExclusiveTableLockSet()->size();
  }

  return -1;
}

void CheckTableLockSizes(Transaction *txn, size_t s_size, size_t x_size, size_t is_size, size_t ix_size,
                         size_t six_size) {
  bool correct = true;
  correct = correct && s_size == txn->GetSharedTableLockSet()->size();
  correct = correct && x_size == txn->GetExclusiveTableLockSet()->size();
  correct = correct && is_size == txn->GetIntentionSharedTableLockSet()->size();
  correct = correct && ix_size == txn->GetIntentionExclusiveTableLockSet()->size();
  correct = correct && six_size == txn->GetSharedIntentionExclusiveTableLockSet()->size();
  if (!correct) {
    fmt::print(
        "table lock size incorrect for txn={}: expected (S={} X={}, IS={}, IX={}, SIX={}), actual (S={} X={}, IS={}, "
        "IX={}, "
        "SIX={})\n",
        txn->GetTransactionId(), s_size, x_size, is_size, ix_size, six_size, txn->GetSharedTableLockSet()->size(),
        txn->GetExclusiveTableLockSet()->size(), txn->GetIntentionSharedTableLockSet()->size(),
        txn->GetIntentionExclusiveTableLockSet()->size(), txn->GetSharedIntentionExclusiveTableLockSet()->size());
  }
  EXPECT_TRUE(correct);
}

void TableLockTest1() {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};

  std::vector<table_oid_t> oids;
  std::vector<Transaction *> txns;

  /** 10 tables */
  int num_oids = 10;
  for (int i = 0; i < num_oids; i++) {
    table_oid_t oid{static_cast<uint32_t>(i)};
    oids.push_back(oid);
    txns.push_back(txn_mgr.Begin());
    EXPECT_EQ(i, txns[i]->GetTransactionId());
  }

  /** Each transaction takes an X lock on every table and then unlocks */
  auto task = [&](int txn_id) {
    bool res;
    for (const table_oid_t &oid : oids) {
      res = lock_mgr.LockTable(txns[txn_id], LockManager::LockMode::EXCLUSIVE, oid);
      EXPECT_TRUE(res);
      CheckGrowing(txns[txn_id]);
    }
    for (const table_oid_t &oid : oids) {
      res = lock_mgr.UnlockTable(txns[txn_id], oid);
      EXPECT_TRUE(res);
      CheckShrinking(txns[txn_id]);
    }
    txn_mgr.Commit(txns[txn_id]);
    CheckCommitted(txns[txn_id]);

    /** All locks should be dropped */
    CheckTableLockSizes(txns[txn_id], 0, 0, 0, 0, 0);
  };

  std::vector<std::thread> threads;
  threads.reserve(num_oids);

  for (int i = 0; i < num_oids; i++) {
    threads.emplace_back(std::thread{task, i});
  }

  for (int i = 0; i < num_oids; i++) {
    threads[i].join();
  }

  for (int i = 0; i < num_oids; i++) {
    delete txns[i];
  }
}

/** Upgrading single transaction from S -> X */
void SingleTableLockUpgradeTest1() {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};

  table_oid_t oid = 0;
  auto txn1 = txn_mgr.Begin();

  /** Take S lock */
  EXPECT_EQ(true, lock_mgr.LockTable(txn1, LockManager::LockMode::SHARED, oid));
  CheckTableLockSizes(txn1, 1, 0, 0, 0, 0);

  /** Upgrade S to X */
  EXPECT_EQ(true, lock_mgr.LockTable(txn1, LockManager::LockMode::EXCLUSIVE, oid));
  CheckTableLockSizes(txn1, 0, 1, 0, 0, 0);

  /** Clean up */
  txn_mgr.Commit(txn1);
  CheckCommitted(txn1);
  CheckTableLockSizes(txn1, 0, 0, 0, 0, 0);

  delete txn1;
}

/** Mutiple upgrading transaction from S -> X */
void MutipleTableLockUpgradeTest1() {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};

  table_oid_t oid = 0;

  int num_txns = 2;
  std::vector<Transaction *> txns;
  for (int i = 0; i < num_txns; i++) {
    txns.push_back(txn_mgr.Begin());
    EXPECT_EQ(i, txns[i]->GetTransactionId());
  }

  /** Each transaction takes an S lock on the same table and row and then
   * unlocks */
  auto task = [&](int txn_id) {
    bool res;

    res = lock_mgr.LockTable(txns[txn_id], LockManager::LockMode::SHARED, oid);
    EXPECT_TRUE(res);
    if (res) {
      std::cout << txn_id << " get S table lock" << std::endl;
    } else {
      std::cout << txn_id << " fail get S table lock" << std::endl;
    }
    CheckGrowing(txns[txn_id]);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // 只有一个能升级成功锁？所有的锁升级都加不上锁，但是会有num_txns - 1个线程会抛出锁升级冲突
    try {
      res = lock_mgr.LockTable(txns[txn_id], LockManager::LockMode::EXCLUSIVE, oid);
      if (txns[txn_id]->GetState() == TransactionState::ABORTED) {
        EXPECT_FALSE(res);
      } else {
        EXPECT_TRUE(res);
        CheckGrowing(txns[txn_id]);
        std::cout << txn_id << " upgrade X table lock" << std::endl;
      }
    } catch (const std::exception &e) {
      txn_mgr.Abort(txns[txn_id]);
      std::cout << e.what() << " " << txn_id << " 锁升级冲突 " << std::endl;
    }
  };

  std::vector<std::thread> threads;
  threads.reserve(num_txns);

  for (int i = 0; i < num_txns; i++) {
    threads.emplace_back(std::thread(task, i));
  }

  for (int i = 0; i < num_txns; i++) {
    threads[i].join();
    delete txns[i];
  }
}

void RowLockTest1() {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};

  table_oid_t oid = 0;
  RID rid{0, 0};

  int num_txns = 3;
  std::vector<Transaction *> txns;
  for (int i = 0; i < num_txns; i++) {
    txns.push_back(txn_mgr.Begin());
    EXPECT_EQ(i, txns[i]->GetTransactionId());
  }

  /** Each transaction takes an S lock on the same table and row and then unlocks */
  auto task = [&](int txn_id) {
    bool res;

    res = lock_mgr.LockTable(txns[txn_id], LockManager::LockMode::SHARED, oid);
    EXPECT_TRUE(res);
    CheckGrowing(txns[txn_id]);

    res = lock_mgr.LockRow(txns[txn_id], LockManager::LockMode::SHARED, oid, rid);
    EXPECT_TRUE(res);
    CheckGrowing(txns[txn_id]);
    /** Lock set should be updated */
    ASSERT_EQ(true, txns[txn_id]->IsRowSharedLocked(oid, rid));

    res = lock_mgr.UnlockRow(txns[txn_id], oid, rid);
    EXPECT_TRUE(res);
    CheckShrinking(txns[txn_id]);
    /** Lock set should be updated */
    ASSERT_EQ(false, txns[txn_id]->IsRowSharedLocked(oid, rid));

    res = lock_mgr.UnlockTable(txns[txn_id], oid);
    EXPECT_TRUE(res);
    CheckShrinking(txns[txn_id]);

    txn_mgr.Commit(txns[txn_id]);
    CheckCommitted(txns[txn_id]);
  };

  std::vector<std::thread> threads;
  threads.reserve(num_txns);

  for (int i = 0; i < num_txns; i++) {
    threads.emplace_back(std::thread(task, i));
  }

  for (int i = 0; i < num_txns; i++) {
    threads[i].join();
    delete txns[i];
  }
}

/** Upgrading single transaction from S -> X */
void SingleRowLockUpgradeTest1() {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};

  table_oid_t oid = 0;
  RID rid{0, 0};
  auto txn1 = txn_mgr.Begin();

  bool res = false;

  /** Take table S lock */
  EXPECT_EQ(true, lock_mgr.LockTable(txn1, LockManager::LockMode::SHARED, oid));
  CheckTableLockSizes(txn1, 1, 0, 0, 0, 0);

  /** Upgrade table S to X */
  EXPECT_EQ(true, lock_mgr.LockTable(txn1, LockManager::LockMode::EXCLUSIVE, oid));
  CheckTableLockSizes(txn1, 0, 1, 0, 0, 0);

  /** Take row S lock */
  res = lock_mgr.LockRow(txn1, LockManager::LockMode::SHARED, oid, rid);
  EXPECT_TRUE(res);
  CheckGrowing(txn1);
  /** Lock set should be updated */
  ASSERT_EQ(true, txn1->IsRowSharedLocked(oid, rid));

  /** Upgrade row S to X */
  res = lock_mgr.LockRow(txn1, LockManager::LockMode::EXCLUSIVE, oid, rid);
  EXPECT_TRUE(res);
  CheckGrowing(txn1);
  /** Lock set should be updated */
  ASSERT_EQ(true, txn1->IsRowExclusiveLocked(oid, rid));

  /** Clean up */
  txn_mgr.Commit(txn1);
  CheckCommitted(txn1);
  CheckTableLockSizes(txn1, 0, 0, 0, 0, 0);

  delete txn1;
}

/** Mutiple upgrading transaction from S -> X */
void MutipleRowLockUpgradeTest1() {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};

  table_oid_t oid = 0;

  int num_txns = 3;
  std::vector<Transaction *> txns;
  RID rid{0, 0};
  for (int i = 0; i < num_txns; i++) {
    txns.push_back(txn_mgr.Begin());
    EXPECT_EQ(i, txns[i]->GetTransactionId());
  }

  /** Each transaction takes an S lock on the same table and row and then
   * unlocks */
  auto task = [&](int txn_id) {
    bool res;

    /** Take table IX lock */
    EXPECT_EQ(true, lock_mgr.LockTable(txns[txn_id], LockManager::LockMode::INTENTION_EXCLUSIVE, oid));
    CheckTableLockSizes(txns[txn_id], 0, 0, 0, 1, 0);

    /** Take row S lock */
    res = lock_mgr.LockRow(txns[txn_id], LockManager::LockMode::SHARED, oid, rid);
    CheckGrowing(txns[txn_id]);
    /** Lock set should be updated */
    ASSERT_EQ(true, txns[txn_id]->IsRowSharedLocked(oid, rid));

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    /** Upgrade row S to X */
    // 只有一个能升级成功锁？所有的锁升级都加不上锁，但是会有num_txns - 1个线程会抛出锁升级冲突
    try {
      res = lock_mgr.LockRow(txns[txn_id], LockManager::LockMode::EXCLUSIVE, oid, rid);
      if (txns[txn_id]->GetState() == TransactionState::ABORTED) {
        EXPECT_FALSE(res);
      } else {
        EXPECT_TRUE(res);
        CheckGrowing(txns[txn_id]);
        std::cout << txn_id << " upgrade X row lock" << std::endl;
      }
    } catch (const std::exception &e) {
      txn_mgr.Abort(txns[txn_id]);
      std::cout << e.what() << " " << txn_id << " 锁升级冲突 " << std::endl;
    }
  };

  std::vector<std::thread> threads;
  threads.reserve(num_txns);

  for (int i = 0; i < num_txns; i++) {
    threads.emplace_back(std::thread(task, i));
  }

  for (int i = 0; i < num_txns; i++) {
    threads[i].join();
    delete txns[i];
  }
}

void TwoPLTest1() {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  table_oid_t oid = 0;

  RID rid0{0, 0};
  RID rid1{0, 1};

  auto *txn = txn_mgr.Begin();
  EXPECT_EQ(0, txn->GetTransactionId());

  bool res;
  res = lock_mgr.LockTable(txn, LockManager::LockMode::INTENTION_EXCLUSIVE, oid);
  EXPECT_TRUE(res);

  res = lock_mgr.UnlockTable(txn, oid);
  EXPECT_TRUE(res);
  CheckGrowing(txn);

  res = lock_mgr.LockTable(txn, LockManager::LockMode::INTENTION_EXCLUSIVE, oid);
  EXPECT_TRUE(res);

  res = lock_mgr.LockRow(txn, LockManager::LockMode::SHARED, oid, rid0);
  EXPECT_TRUE(res);

  CheckGrowing(txn);
  CheckTxnRowLockSize(txn, oid, 1, 0);

  res = lock_mgr.LockRow(txn, LockManager::LockMode::EXCLUSIVE, oid, rid1);
  EXPECT_TRUE(res);
  CheckGrowing(txn);
  CheckTxnRowLockSize(txn, oid, 1, 1);

  res = lock_mgr.UnlockRow(txn, oid, rid0);
  EXPECT_TRUE(res);
  CheckShrinking(txn);
  CheckTxnRowLockSize(txn, oid, 0, 1);

  try {
    lock_mgr.LockRow(txn, LockManager::LockMode::SHARED, oid, rid0);
  } catch (TransactionAbortException &e) {
    CheckAborted(txn);
    CheckTxnRowLockSize(txn, oid, 0, 1);
  }

  // Need to call txn_mgr's abort
  txn_mgr.Abort(txn);
  CheckAborted(txn);
  CheckTxnRowLockSize(txn, oid, 0, 0);
  CheckTableLockSizes(txn, 0, 0, 0, 0, 0);

  delete txn;
}

void AbortTest1() {
  fmt::print(stderr, "AbortTest1: multiple X should block\n");

  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};

  table_oid_t oid = 0;
  RID rid{0, 0};

  auto txn1 = txn_mgr.Begin();
  auto txn2 = txn_mgr.Begin();
  auto txn3 = txn_mgr.Begin();

  /** All takes IX lock on table */
  EXPECT_EQ(true, lock_mgr.LockTable(txn1, LockManager::LockMode::INTENTION_EXCLUSIVE, oid));
  CheckTableLockSizes(txn1, 0, 0, 0, 1, 0);
  EXPECT_EQ(true, lock_mgr.LockTable(txn2, LockManager::LockMode::INTENTION_EXCLUSIVE, oid));
  CheckTableLockSizes(txn2, 0, 0, 0, 1, 0);
  EXPECT_EQ(true, lock_mgr.LockTable(txn3, LockManager::LockMode::INTENTION_EXCLUSIVE, oid));
  CheckTableLockSizes(txn3, 0, 0, 0, 1, 0);

  /** txn1 takes X lock on row */
  EXPECT_EQ(true, lock_mgr.LockRow(txn1, LockManager::LockMode::EXCLUSIVE, oid, rid));
  CheckTxnRowLockSize(txn1, oid, 0, 1);

  /** txn2 attempts X lock on row but should be blocked */
  auto txn2_task = std::thread{[&]() { lock_mgr.LockRow(txn2, LockManager::LockMode::EXCLUSIVE, oid, rid); }};

  /** Sleep for a bit */
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  /** txn2 shouldn't have been granted the lock */
  CheckTxnRowLockSize(txn2, oid, 0, 0);

  /** txn3 attempts X lock on row but should be blocked */
  auto txn3_task = std::thread{[&]() { lock_mgr.LockRow(txn3, LockManager::LockMode::EXCLUSIVE, oid, rid); }};
  /** Sleep for a bit */
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  /** txn3 shouldn't have been granted the lock */
  CheckTxnRowLockSize(txn3, oid, 0, 0);

  /** Abort txn2 */
  txn_mgr.Abort(txn2);

  /** txn1 releases lock */
  EXPECT_EQ(true, lock_mgr.UnlockRow(txn1, oid, rid));
  CheckTxnRowLockSize(txn1, oid, 0, 0);

  txn2_task.join();
  txn3_task.join();
  /** txn2 shouldn't have any row locks */
  CheckTxnRowLockSize(txn2, oid, 0, 0);
  CheckTableLockSizes(txn2, 0, 0, 0, 0, 0);
  /** txn3 should have the row lock */
  CheckTxnRowLockSize(txn3, oid, 0, 1);

  delete txn1;
  delete txn2;
  delete txn3;
}

TEST(LockManagerTest, TableLockTest1) { TableLockTest1(); }                              // NOLINT
TEST(LockManagerTest, SingleTableLockUpgradeTest1) { SingleTableLockUpgradeTest1(); }    // NOLINT
TEST(LockManagerTest, MutipleTableLockUpgradeTest1) { MutipleTableLockUpgradeTest1(); }  // NOLINT
TEST(LockManagerTest, RowLockTest1) { RowLockTest1(); }                                  // NOLINT
TEST(LockManagerTest, SingleRowLockUpgradeTest1) { SingleRowLockUpgradeTest1(); }        // NOLINT
TEST(LockManagerTest, MutipleRowLockUpgradeTest1) { MutipleRowLockUpgradeTest1(); }      // NOLINT
TEST(LockManagerTest, TwoPLTest1) { TwoPLTest1(); }                                      // NOLINT
TEST(LockManagerTest, RowAbortTest1) { AbortTest1(); }                                   // NOLINT

// others' test

TEST(LockManagerTest, DISABLED_UpgradeTest) {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  table_oid_t oid = 0;

  auto *txn0 = txn_mgr.Begin();
  auto *txn1 = txn_mgr.Begin();
  auto *txn2 = txn_mgr.Begin();

  lock_mgr.LockTable(txn0, LockManager::LockMode::SHARED, oid);
  lock_mgr.LockTable(txn0, LockManager::LockMode::EXCLUSIVE, oid);
  lock_mgr.UnlockTable(txn0, oid);
  txn_mgr.Commit(txn0);
  txn_mgr.Begin(txn0);
  CheckTableLockSizes(txn0, 0, 0, 0, 0, 0);
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  std::thread t0([&]() {
    bool res;
    res = lock_mgr.LockTable(txn0, LockManager::LockMode::SHARED, oid);
    EXPECT_TRUE(res);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    res = lock_mgr.LockTable(txn0, LockManager::LockMode::EXCLUSIVE, oid);
    EXPECT_TRUE(res);
    lock_mgr.UnlockTable(txn0, oid);
    txn_mgr.Commit(txn1);
  });

  std::thread t1([&]() {
    bool res;
    res = lock_mgr.LockTable(txn1, LockManager::LockMode::SHARED, oid);
    EXPECT_TRUE(res);
    std::this_thread::sleep_for(std::chrono::milliseconds(60));

    res = lock_mgr.UnlockTable(txn1, oid);
    EXPECT_TRUE(res);
    CheckTableLockSizes(txn0, 0, 0, 0, 0, 0);
    CheckTableLockSizes(txn1, 0, 0, 0, 0, 0);
    txn_mgr.Commit(txn1);
  });

  std::thread t2([&]() {
    bool res;
    res = lock_mgr.LockTable(txn2, LockManager::LockMode::SHARED, oid);
    EXPECT_TRUE(res);
    std::this_thread::sleep_for(std::chrono::milliseconds(70));

    res = lock_mgr.UnlockTable(txn2, oid);
    EXPECT_TRUE(res);
    txn_mgr.Commit(txn2);
  });

  t0.join();
  t1.join();
  t2.join();
  delete txn0;
  delete txn1;
  delete txn2;
}

TEST(LockManagerTest, DISABLED_UpgradeTest1) {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  table_oid_t oid = 0;

  auto *txn0 = txn_mgr.Begin();
  auto *txn1 = txn_mgr.Begin();
  auto *txn2 = txn_mgr.Begin();

  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  std::thread t0([&]() {
    bool res;
    res = lock_mgr.LockTable(txn0, LockManager::LockMode::SHARED, oid);
    EXPECT_TRUE(res);
    res = lock_mgr.LockTable(txn0, LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE, oid);
    EXPECT_TRUE(res);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    lock_mgr.UnlockTable(txn0, oid);
    txn_mgr.Commit(txn1);
  });

  std::thread t1([&]() {
    // bool res;
    // std::this_thread::sleep_for(std::chrono::milliseconds(60));
    // res = lock_mgr.LockTable(txn1, LockManager::LockMode::SHARED, oid);
    // EXPECT_TRUE(res);

    // res = lock_mgr.UnlockTable(txn1, oid);
    // EXPECT_TRUE(res);
    // CheckTableLockSizes(txn0, 0, 0, 0, 0, 0);
    // CheckTableLockSizes(txn1, 0, 0, 0, 0, 0);
    // txn_mgr.Commit(txn1);
  });

  std::thread t2([&]() {
    // bool res;
    // res = lock_mgr.LockTable(txn2, LockManager::LockMode::SHARED, oid);
    // EXPECT_TRUE(res);
    // std::this_thread::sleep_for(std::chrono::milliseconds(70));

    // res = lock_mgr.UnlockTable(txn2, oid);
    // EXPECT_TRUE(res);
    // txn_mgr.Commit(txn2);
  });

  t0.join();
  t1.join();
  t2.join();
  delete txn0;
  delete txn1;
  delete txn2;
}

TEST(LockManagerTest, DISABLED_MixedTest) {
  TEST_TIMEOUT_BEGIN
  const int num = 10;
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  std::stringstream result;
  auto bustub = std::make_unique<bustub::BustubInstance>();
  auto writer = bustub::SimpleStreamWriter(result, true, " ");

  bustub->ExecuteSql("\\dt", writer);
  auto schema = "CREATE TABLE test_1 (x int, y int);";
  bustub->ExecuteSql(schema, writer);
  std::string query = "INSERT INTO test_1 VALUES ";
  for (size_t i = 0; i < num; i++) {
    query += fmt::format("({}, {})", i, 0);
    if (i != num - 1) {
      query += ", ";
    } else {
      query += ";";
    }
  }
  bustub->ExecuteSql(query, writer);
  schema = "CREATE TABLE test_2 (x int, y int);";
  bustub->ExecuteSql(schema, writer);
  bustub->ExecuteSql(query, writer);

  auto txn1 = bustub->txn_manager_->Begin();
  auto txn2 = bustub->txn_manager_->Begin();

  fmt::print("------\n");

  query = "delete from test_1 where x = 100;";
  bustub->ExecuteSqlTxn(query, writer, txn2);

  query = "select * from test_1;";
  bustub->ExecuteSqlTxn(query, writer, txn2);

  query = "select * from test_1;";
  bustub->ExecuteSqlTxn(query, writer, txn1);

  bustub->txn_manager_->Commit(txn1);
  fmt::print("txn1 commit\n");

  bustub->txn_manager_->Commit(txn2);
  fmt::print("txn2 commit\n");

  delete txn1;
  delete txn2;
  TEST_TIMEOUT_FAIL_END(10000)
}

}  // namespace bustub
