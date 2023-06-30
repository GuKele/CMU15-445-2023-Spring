#include <fmt/ranges.h>
#include <gtest/gtest.h>
#include <algorithm>
#include <atomic>
#include <chrono>  // NOLINT
#include <cstdio>
#include <memory>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>  //NOLINT
#include <utility>
#include <vector>

#include "common_checker.h"  // NOLINT

namespace bustub {

void CommitTest1() {
  // should scan changes of committed txn
  auto db = GetDbForCommitAbortTest("CommitTest1");
  auto txn1 = Begin(*db, IsolationLevel::READ_UNCOMMITTED);
  Insert(txn1, *db, 1);
  Commit(*db, txn1);
  auto txn2 = Begin(*db, IsolationLevel::READ_UNCOMMITTED);
  Scan(txn2, *db, {1, 233, 234});
  Commit(*db, txn2);
}

// NOLINTNEXTLINE
TEST(CommitAbortTest, CommitTestA) { CommitTest1(); }

void Test1(IsolationLevel lvl) {
  // should scan changes of committed txn
  auto db = GetDbForVisibilityTest("Test1");
  auto txn1 = Begin(*db, lvl);
  Delete(txn1, *db, 233);
  Commit(*db, txn1);
  auto txn2 = Begin(*db, lvl);
  Scan(txn2, *db, {234});
  Commit(*db, txn2);
}

// NOLINTNEXTLINE
TEST(VisibilityTest, TestA) {
  // only this one will be public :)
  Test1(IsolationLevel::READ_COMMITTED);
}

// NOLINTNEXTLINE
TEST(IsolationLevelTest, InsertTestA) {
  ExpectTwoTxn("InsertTestA.1", IsolationLevel::READ_UNCOMMITTED, IsolationLevel::READ_UNCOMMITTED, false, IS_INSERT,
               ExpectedOutcome::DirtyRead);
}

// NOLINTNEXTLINE
TEST(IsolationLevelTest, DeleteTestA) {
  ExpectTwoTxn("DeleteTestA.1", IsolationLevel::READ_COMMITTED, IsolationLevel::READ_UNCOMMITTED, false, IS_DELETE,
               ExpectedOutcome::BlockOnRead);
}

class A {
 public:
  explicit A(int num = 0) : num_{num} { std::cout << "A ctor" << std::endl; }
  A(const A &that) : num_{that.num_} { std::cout << "A copy ctor" << std::endl; }
  A(A &&that) noexcept : num_{that.num_} { std::cout << "A move ctor" << std::endl; }

 private:
  int num_;
};

class B {
 public:
  B(A a, int n) : a_{std::move(a)} { std::cout << "B ctor" << std::endl; }

  B(const B &that) { std::cout << "B copy ctor" << std::endl; }
  // B(B &&that) noexcept { std::cout << "B move ctor" << std::endl; }

 private:
  A a_;
};

B Func() {
  A a(7);
  return {a, 0};  // 想知道 这个a会不会被移动过去,事实上a是被copy ctor到B构造函数的形参中,这个a会当作左值，NRVO构造B对象
}

B Func2() {
  A a(5);
  return {std::move(a), 0};
}

void Func3(const A &a) {}

TEST(RVOTest, RVOTest1) {
  // [[maybe_unused]] auto b = Func();
  [[maybe_unused]] auto b = Func2();
  // A a;
  // Func3(std::move(a));
}

}  // namespace bustub
