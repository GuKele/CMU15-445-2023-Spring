#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"
#include "storage/page/page.h"

namespace bustub {

// 这个RAII的手法和lock_guard不太一样，获取的时候并不初始化资源，而是获取到资源后封装给他，他只负责析构时释放资源
BasicPageGuard::BasicPageGuard(BufferPoolManager *bpm, Page *page) : bpm_(bpm), page_(page) {}

BasicPageGuard::BasicPageGuard(BasicPageGuard &&other) noexcept
    : bpm_{other.bpm_}, page_{other.page_}, is_dirty_{other.is_dirty_} {
  other.ClearAllContents();
}

auto BasicPageGuard::operator=(BasicPageGuard &&other) noexcept -> BasicPageGuard & {
  // 处理自赋值的清空
  if (this != &other) {
    // 释放this的资源
    Drop();
    // 移动other的资源
    bpm_ = other.bpm_;
    page_ = other.page_;
    is_dirty_ = other.is_dirty_;
    // other置空使其失效
    other.ClearAllContents();
  }
  // 返回*this
  return *this;
}

void BasicPageGuard::Drop() {
  if (IsValid()) {
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
    ClearAllContents();
  }
}

BasicPageGuard::~BasicPageGuard() { Drop(); };  // NOLINT

// dufault应该不太对
// ReadPageGuard::ReadPageGuard(ReadPageGuard &&other) noexcept = default;
ReadPageGuard::ReadPageGuard(ReadPageGuard &&other) noexcept : guard_(std::move(other.guard_)) {
  // 清空内容后，other的析构调用Drop就不会释放锁了，我们这样避免other释放锁，然后this再加锁
  // 相当于直接吧other加的锁给了this
  // 初始化列表中BasicPageGuard的移动构造函数，让other已经失效了，其析构不会释放读锁和unpin
  // 所以这样的话，直接=default就可以，dufault是对每个non-static成员使用移动构造
}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  // 同样是4个步骤，处理自赋值，释放自己的，获取别人的，让别人失效，这里用了移动获取，所以别人的会直接失效，不需要在重复
  if (this != &that) {
    Drop();
    guard_ = std::move(that.guard_);
  }
  return *this;
};

void ReadPageGuard::Drop() {
  if (IsValid()) {
    guard_.page_->RUnlatch();
    guard_.Drop();
  }
}

ReadPageGuard::~ReadPageGuard() { Drop(); }  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept = default;

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this != &that) {
    Drop();
    guard_ = std::move(that.guard_);
  }
  return *this;
}

void WritePageGuard::Drop() {
  if (IsValid()) {
    guard_.page_->WUnlatch();
    guard_.Drop();
  }
}

WritePageGuard::~WritePageGuard() { Drop(); }  // NOLINT

}  // namespace bustub
