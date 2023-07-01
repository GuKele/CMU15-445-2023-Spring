#include <cassert>
#include <optional>
#include <sstream>
#include <string>
#include <utility>
#include <variant>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_header_page.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page_guard.h"
#include "type/value.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  auto root_page_id = GetRootPageIdForRead();
  return root_page_id == INVALID_PAGE_ID;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn) -> bool {
  // Declaration of context instance.
  Context ctx;
  (void)ctx;

  if (auto leaf_node = FindLeafForRead(key, &ctx); leaf_node != nullptr) {  // b+树不为空
    if (ValueType val; leaf_node->ValueOfKey(key, &val, comparator_)) {     // 找到了key对应的值
      result->emplace_back(val);
      return true;
    }
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafForRead(const KeyType &key, Context *context) const -> const LeafPage * {
  if (GetRootPageIdForRead(context) == INVALID_PAGE_ID) {
    return nullptr;
  }

  auto read_page_guard = bpm_->FetchPageRead(context->root_page_id_);
  context->read_set_.pop_back();
  auto node = read_page_guard.As<BPlusTreePage>();

  while (!node->IsLeafPage()) {
    auto internal_node = read_page_guard.As<InternalPage>();
    auto child_page_id = internal_node->Search(key, comparator_);
    read_page_guard = bpm_->FetchPageRead(child_page_id);
    node = read_page_guard.As<BPlusTreePage>();
  }
  context->read_set_.emplace_back(std::move(read_page_guard));
  assert(context->read_set_.size() == 1);
  return context->read_set_.back().As<LeafPage>();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeftMostLeafForRead(Context *context) const -> const LeafPage * {
  // FindLeafForRead(Min_KeyType_obj, &context);
  if (auto root_page_id = GetRootPageIdForRead(context); root_page_id != INVALID_PAGE_ID) {
    auto read_page_guard = bpm_->FetchPageRead(context->root_page_id_);
    context->read_set_.pop_back();
    auto node = read_page_guard.As<BPlusTreePage>();

    while (!node->IsLeafPage()) {
      auto internal_node = read_page_guard.As<InternalPage>();
      auto child_page_id = internal_node->FrontValue();
      read_page_guard = bpm_->FetchPageRead(child_page_id);
      node = read_page_guard.As<BPlusTreePage>();
    }
    context->read_set_.emplace_back(std::move(read_page_guard));
    assert(context->read_set_.size() == 1);
    return context->read_set_.back().As<LeafPage>();
  }
  return nullptr;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  // Declaration of context instance.
  Context ctx;
  (void)ctx;

  auto leaf_node = FindLeafForOption(key, &ctx, BPlusTreeOption::INSERT);

  /* 1.空树 */
  if (leaf_node == nullptr) {
    // 创建一个新的叶子作为root,在其中插入这个键值对
    return CreateNewTree(key, value, &ctx);
  }

  /* 2.非空树，往叶子节点中插入 */
  auto result = leaf_node->Insert(key, value, comparator_);

  if (leaf_node->IsFull()) {
    if (auto right_bro_leaf_node = SplitLeaf(leaf_node); right_bro_leaf_node != nullptr) {
      auto right_bro_leaf_page_id = leaf_node->GetNextPageId();
      auto right_bro_leaf_key = right_bro_leaf_node->KeyAt(0);
      return InsertIntoParent(right_bro_leaf_key, right_bro_leaf_page_id, &ctx);
    }
  }

  // TODO(gukele) return false?
  return result;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafForOption(const KeyType &key, Context *context, BPlusTreeOption opt) -> LeafPage * {
  // 空树
  if (GetRootPageIdForInsertOrDelete(context) == INVALID_PAGE_ID) {
    return nullptr;
  }

  auto page_guard = bpm_->FetchPageWrite(context->root_page_id_);
  int index = -1;
  auto base_node = page_guard.As<BPlusTreePage>();  // 不需要置脏位
  if (base_node->IsSafeAfterOption(opt)) {
    context->header_page_ = std::nullopt;
  }

  while (!base_node->IsLeafPage()) {
    context->write_guard_and_index_set_.emplace_back(std::move(page_guard), index);
    auto internal_node = reinterpret_cast<const InternalPage *>(base_node);
    auto child_page_id = internal_node->Search(key, comparator_, &index);
    page_guard = bpm_->FetchPageWrite(child_page_id);
    base_node = page_guard.As<BPlusTreePage>();
    if (base_node->IsSafeAfterOption(opt)) {  // 当前的节点是个安全节点，之前的祖先写锁都可以释放了
      context->write_guard_and_index_set_.clear();
    }
  }

  auto leaf_node = page_guard.AsMut<LeafPage>();
  context->write_guard_and_index_set_.emplace_back(std::move(page_guard), index);
  return leaf_node;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitLeaf(LeafPage *leaf_node) -> LeafPage * {
  page_id_t right_bro_leaf_page_id;
  auto right_bro_leaf_guard = bpm_->NewPageGuarded(&right_bro_leaf_page_id);
  if (right_bro_leaf_guard.IsValid()) {
    auto right_bro_leaf_node = right_bro_leaf_guard.AsMut<LeafPage>();
    right_bro_leaf_node->Init(leaf_max_size_);

    // auto mid = leaf_node->GetMinSize();
    // leaf_node->MoveTo(mid, leaf_node->GetSize() - mid, right_bro_leaf_node, 0);
    // leaf_node->SetSize(mid);
    // right_bro_leaf_node->SetNextPageId(leaf_node->GetNextPageId());
    // leaf_node->SetNextPageId(right_bro_leaf_guard.PageId());

    leaf_node->Split(right_bro_leaf_node, right_bro_leaf_guard.PageId());

    assert(leaf_node->GetSize() >= leaf_node->GetMinSize());
    assert(right_bro_leaf_node->GetSize() >= right_bro_leaf_node->GetMinSize());
    assert(right_bro_leaf_node->GetSize() >= right_bro_leaf_node->GetMinSize());
    return right_bro_leaf_node;
  }
  throw Exception(ExceptionType::OUT_OF_MEMORY, "Cannot allocate new page");
  return nullptr;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::CreateNewTree(const KeyType &key, const ValueType &value, Context *context) -> bool {
  assert(context->header_page_.has_value());
  auto &header_page_guard = context->header_page_.value();
  auto header_node = header_page_guard.AsMut<BPlusTreeHeaderPage>();
  auto page = bpm_->NewPageGuarded(&header_node->root_page_id_);

  if (!page.IsValid()) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Cannot allocate new page");
    return false;
  }

  auto leaf_node = page.AsMut<LeafPage>();
  leaf_node->Init(leaf_max_size_);
  auto result = leaf_node->Insert(key, value, comparator_);
  return result;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertIntoParent(const KeyType &key, const page_id_t &value, Context *context) -> bool {
  auto [mid_page_guard, index] = std::move(context->write_guard_and_index_set_.back());
  context->write_guard_and_index_set_.pop_back();
  auto mid_page_id = mid_page_guard.PageId();

  // 1.父亲就是header_page了，也就是说left_node是原来的根
  if (context->IsRootPage(mid_page_id)) {
    return CreateNewRoot(mid_page_id, key, value, context);
  }

  // 2.父亲不是header_page,但是父亲满了
  assert(!context->write_guard_and_index_set_.empty());
  auto &[father_page_guard, _] = context->write_guard_and_index_set_.back();
  auto father_i_node = father_page_guard.AsMut<InternalPage>();
  if (father_i_node->IsFull()) {
    // 父亲要分裂，然后继续插入父亲的父亲
    auto [new_right_bro_key, new_right_bro_value] = InsertAndSplitInternal(father_i_node, key, value, index + 1);
    return InsertIntoParent(new_right_bro_key, new_right_bro_value, context);
  }

  // 3.父亲不是header_page，父亲也没满
  father_i_node->InsertAt(index + 1, key, value);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::CreateNewRoot(const page_id_t &left, const KeyType &key, const page_id_t &right, Context *context)
    -> bool {
  auto header_node = context->header_page_->AsMut<BPlusTreeHeaderPage>();
  page_id_t new_root_page_id = INVALID_PAGE_ID;
  auto new_root_guard = bpm_->NewPageGuarded(&new_root_page_id);

  if (!new_root_guard.IsValid()) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Cannot allocate new page");
    return false;
  }

  header_node->root_page_id_ = new_root_page_id;
  auto new_root_i_node = new_root_guard.AsMut<InternalPage>();
  new_root_i_node->Init(internal_max_size_);
  new_root_i_node->SetValueAt(0, left);
  new_root_i_node->IncreaseSize(1);
  new_root_i_node->Insert(key, right, comparator_);
  assert(new_root_i_node->GetSize() == 2);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertAndSplitInternal(InternalPage *i_node, const KeyType &key, const page_id_t &value, int index)
    -> std::pair<KeyType, page_id_t> {
  page_id_t right_bro_internal_page_id = INVALID_PAGE_ID;
  auto right_bro_internal_page_guard = bpm_->NewPageGuarded(&right_bro_internal_page_id);
  if (!right_bro_internal_page_guard.IsValid()) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Cannot allocate new page");
    return {KeyType(), INVALID_PAGE_ID};
  }
  auto right_bro_internal_node = right_bro_internal_page_guard.AsMut<InternalPage>();
  right_bro_internal_node->Init(internal_max_size_);

  // 找到新插入的key所应该在的下标，从而决定应该怎么分裂internal node
  // 保证移动完毕后，左节点的个数 == 右节点个数 || 左节点的个数 == 右节点个数 + 1
  // size为偶数时，如果 0 1 3 4 所得MinSize==2,要插入2(index==2<=MinSize),那么我们照样移动，插左边
  //             如果 0 1 2 3 所得MinSize==2,要插入4(index==4>MinSize),那么右边少移动一个，插右边
  // size为奇数时，如果 0 1 2 4 5 所得MinSize==3,要插入3(index==3>=MinSize),那我们照常移动，插入右边
  //             如果 0 2 3 4 5 所得MinSize==3,要插入1(index==2<MinSize),那我们往右边多移动一个，插入左边

  // int index = -1;
  // i_node->Search(key, comparator_, &index); // 第一个比key小的index
  // ++index; // key插入后应该所在的index

  // bool insert_left;
  // auto start = i_node->GetMinSize();
  // if (i_node->GetSize() % 2 == 0) {  // 偶数个
  //   if (index > start) {
  //     insert_left = false;
  //     ++start;
  //   } else {
  //     insert_left = true;
  //   }
  // } else {  // 奇数个
  //   if (index < start) {
  //     --start;
  //     insert_left = true;
  //   } else {
  //     insert_left = false;
  //   }
  // }

  // i_node->MoveTo(start, i_node->GetSize() - start, right_bro_internal_node, 0);

  // if (insert_left) {
  //   i_node->Insert(key, value, comparator_);
  // } else {
  //   right_bro_internal_node->Insert(key, value, comparator_);
  // }

  std::pair<KeyType, page_id_t> temp;
  if (index == i_node->GetSize()) {
    temp = {key, value};
  } else {
    temp = i_node->Back();
    i_node->PopBack();
    i_node->InsertAt(index, key, value);
  }
  int start = i_node->GetMinSize();
  if (i_node->GetSize() % 2 == 0) {  // 偶数个
    ++start;                         // 让i_node的size比right_bro的多一个
  }
  i_node->MoveTo(start, i_node->GetSize() - start, right_bro_internal_node, 0);
  right_bro_internal_node->PushBack(temp);

  return {right_bro_internal_node->KeyAt(0), right_bro_internal_page_guard.PageId()};
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  // Declaration of context instance.
  Context ctx;
  (void)ctx;
  // 1.叶子节点删除，如果小于minsize
  // 1.1 叶节点就是根，如果删除size==0,那么root就要变成-1

  // 1.2 叶节点不是根，如果有左右兄弟
  // 1.2.1右兄弟够借，并且更新右兄弟的key。(借多少合适呢)
  // 1.2.2右兄弟不够借，合并到左兄弟，删除右兄弟的key。

  // 2.internal节点删除，如果小于minsize
  // 2.1右兄弟够借，注意与叶子节点借不太一样，从右兄弟借一个，右兄弟的第一个page_id给左边，第二个key作为右兄弟的key
  //    右兄弟原来的key变成了左兄弟刚才借过去的的page_id的key.
  // 2.2右兄弟不够借，只能合并，很复杂！

  // 删除的时候也是，如果当前节点在删除后是安全的，那么就清空所有祖先的锁，但是可能

  auto leaf_node = FindLeafForOption(key, &ctx, BPlusTreeOption::DELETE);
  auto [leaf_page_guard, index] = std::move(ctx.write_guard_and_index_set_.back());
  ctx.write_guard_and_index_set_.pop_back();

  // 1.空树
  if (leaf_node == nullptr) {
    return;
  }

  // 可能删除前就不存在key
  leaf_node->Delete(key, comparator_);

  // 2.非空树，并且叶子节点删除后安全
  if (leaf_node->IsSafe()) {
    return;
  }

  // 3.叶子节点删除后不安全，但是叶子节点是root
  if (ctx.IsRootPage(leaf_page_guard.PageId())) {
    if (leaf_node->IsEmpty()) {
      auto header_node = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
      header_node->root_page_id_ = INVALID_PAGE_ID;
    }
    return;
  }

  // 4.叶子节点删除后不安全，且不叶子节点不是root，有右兄弟或者左兄弟（父亲相同的兄弟），（不是root,一定会有至少一个兄弟吧）
  assert(!ctx.write_guard_and_index_set_.empty());
  auto &[father_page_guard, _] = ctx.write_guard_and_index_set_.back();
  auto father_node = father_page_guard.AsMut<InternalPage>();
  auto left_bro_page_id = father_node->ValueAt(index - 1);
  auto right_bro_page_id = father_node->ValueAt(index + 1);

  if (left_bro_page_id != INVALID_PAGE_ID || right_bro_page_id != INVALID_PAGE_ID) {
    // 4.1 左兄弟够借
    // 有左兄弟
    WritePageGuard left_bro_page_guard;
    if (left_bro_page_id != INVALID_PAGE_ID) {
      left_bro_page_guard = bpm_->FetchPageWrite(left_bro_page_id);
      auto left_bro_leaf_node = left_bro_page_guard.AsMut<LeafPage>();
      // 够借
      if (left_bro_leaf_node->IsSafeAfterOption(BPlusTreeOption::DELETE)) {
        // left_bro_leaf_node->MoveTo(left_bro_leaf_node->GetSize() - 1, 1, leaf_node, 0);
        leaf_node->PushFront(left_bro_leaf_node->Back());
        left_bro_leaf_node->PopBack();
        father_node->SetKeyAt(index, leaf_node->KeyAt(0));
        return;
      }
    }
    // 4.2 右兄弟够借
    // 有右兄弟
    WritePageGuard right_bro_page_guard;
    if (right_bro_page_id != INVALID_PAGE_ID) {
      right_bro_page_guard = bpm_->FetchPageWrite(right_bro_page_id);
      auto right_bro_leaf_node = right_bro_page_guard.AsMut<LeafPage>();
      // 够借
      if (right_bro_leaf_node->IsSafeAfterOption(BPlusTreeOption::DELETE)) {
        // right_bro_leaf_node->MoveTo(0, 1, leaf_node, leaf_node->GetSize());
        leaf_node->PushBack(right_bro_leaf_node->Front());
        right_bro_leaf_node->PopFront();
        father_node->SetKeyAt(index + 1, right_bro_leaf_node->KeyAt(0));
        return;
      }
    }
    // 4.3 merge to 左兄弟
    if (left_bro_page_guard.IsValid()) {
      auto left_bro_leaf_node = left_bro_page_guard.AsMut<LeafPage>();
      // leaf_node->MoveTo(0, leaf_node->GetSize(), left_bro_leaf_node, left_bro_leaf_node->GetSize());
      leaf_node->MergeToLeftBro(left_bro_leaf_node);
      DeleteFromParent(index, &ctx);
      return;
    }
    // 4.4 右兄弟 merge to
    if (right_bro_page_guard.IsValid()) {
      auto right_bro_leaf_node = right_bro_page_guard.AsMut<LeafPage>();
      right_bro_leaf_node->MergeToLeftBro(leaf_node);
      DeleteFromParent(index + 1, &ctx);
      return;
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DeleteFromParent(int index, Context *context) {
  auto [i_page_guard, pos] = std::move(context->write_guard_and_index_set_.back());
  context->write_guard_and_index_set_.pop_back();
  auto i_node = i_page_guard.AsMut<InternalPage>();

  i_node->DeleteAt(index);
  // 1.删除后安全
  if (i_node->IsSafe()) {
    return;
  }
  // 2.删除后不安全，但是是根
  if (context->IsRootPage(i_page_guard.PageId())) {
    if (i_node->IsEmpty()) {
      auto header_node = context->header_page_->AsMut<BPlusTreeHeaderPage>();
      header_node->root_page_id_ = i_node->ValueAt(0);
    }
    return;
  }
  // 3.删除后不安全，有左兄弟或者右兄弟
  assert(!context->write_guard_and_index_set_.empty());
  auto &[father_page_guard, _] = context->write_guard_and_index_set_.back();
  auto father_node = father_page_guard.AsMut<InternalPage>();
  auto left_bro_page_id = father_node->ValueAt(pos - 1);
  auto right_bro_page_id = father_node->ValueAt(pos + 1);

  if (left_bro_page_id != INVALID_PAGE_ID || right_bro_page_id != INVALID_PAGE_ID) {
    // 3.1 左兄弟够借
    // 有左兄弟
    WritePageGuard left_bro_page_guard;
    if (left_bro_page_id != INVALID_PAGE_ID) {
      left_bro_page_guard = bpm_->FetchPageWrite(left_bro_page_id);
      auto left_bro_i_node = left_bro_page_guard.AsMut<InternalPage>();
      // 够借
      if (left_bro_i_node->IsSafeAfterOption(BPlusTreeOption::DELETE)) {
        // 父亲节点中自己的key借给自己
        i_node->SetKeyAt(0, father_node->KeyAt(pos));
        // 父亲节点中更新自己的key
        father_node->SetKeyAt(pos, left_bro_i_node->Back().first);
        // 左兄弟的最后一个value借给自己
        i_node->PushFrontValue(left_bro_i_node->Back().second);
        left_bro_i_node->PopBack();
        return;
      }
    }
    // 3.2 右兄弟够借
    // 有右兄弟
    WritePageGuard right_bro_page_guard;
    if (right_bro_page_id != INVALID_PAGE_ID) {
      right_bro_page_guard = bpm_->FetchPageWrite(right_bro_page_id);
      auto right_bro_i_node = right_bro_page_guard.AsMut<InternalPage>();
      // 够借
      if (right_bro_i_node->IsSafeAfterOption(BPlusTreeOption::DELETE)) {
        // {父亲节点中右兄弟的key，右兄弟第一个value}借出，
        i_node->EmplaceBack(father_node->KeyAt(pos + 1), right_bro_i_node->FrontValue());
        right_bro_i_node->PopFront();
        // 父亲节点中右兄弟的key更新
        father_node->SetKeyAt(pos + 1, right_bro_i_node->KeyAt(0));
        return;
      }
    }
    // 3.3 merge to 左兄弟
    if (left_bro_page_guard.IsValid()) {
      auto left_bro_i_node = left_bro_page_guard.AsMut<InternalPage>();
      i_node->SetKeyAt(0, father_node->KeyAt(pos));
      i_node->MergeToLeftBro(left_bro_i_node);
      DeleteFromParent(pos, context);
      return;
    }
    // 4.4 右兄弟 merge to
    if (right_bro_page_guard.IsValid()) {
      auto right_bro_i_node = right_bro_page_guard.AsMut<InternalPage>();
      right_bro_i_node->SetKeyAt(0, father_node->KeyAt(pos + 1));
      right_bro_i_node->MergeToLeftBro(i_node);
      DeleteFromParent(pos + 1, context);
      return;
    }
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  Context ctx;
  if (FindLeftMostLeafForRead(&ctx)) {
    return INDEXITERATOR_TYPE(std::move(ctx.read_set_.back().DecayToBasePageGuard()), bpm_, 0);
  }
  return INDEXITERATOR_TYPE();
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  Context ctx;
  if (auto leaf_node = FindLeafForRead(key, &ctx); leaf_node != nullptr) {
    // FIXME(gukele): 目前是返回第一个大于等于key的iterator，应该是正确的
    int index = -1;
    leaf_node->IndexOfKey(key, comparator_, &index);
    assert(!ctx.read_set_.empty());
    // BUG(gukele): 如果是最后一叶，并且还是比最后一个槽还大，实际上就是b+树的end索引了,已经修复
    if (leaf_node->GetNextPageId() != INVALID_PAGE_ID || index != leaf_node->GetSize()) {
      return INDEXITERATOR_TYPE(std::move(ctx.read_set_.back().DecayToBasePageGuard()), bpm_, index);
    }
  }
  return INDEXITERATOR_TYPE();
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  // TODO(gukele) 这个接口含义不清楚
  auto header_page = bpm_->FetchPageRead(header_page_id_);
  auto header_node = header_page.As<BPlusTreeHeaderPage>();
  return header_node->root_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageIdForRead(Context *ctx) const -> page_id_t {
  auto header_page_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_node = header_page_guard.As<BPlusTreeHeaderPage>();
  if (ctx != nullptr) {
    ctx->root_page_id_ = header_node->root_page_id_;
    ctx->read_set_.emplace_back(std::move(header_page_guard));
  }
  return header_node->root_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageIdForInsertOrDelete(Context *ctx) -> page_id_t {
  auto header_page_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_node = header_page_guard.AsMut<BPlusTreeHeaderPage>();

  ctx->header_page_ = std::move(header_page_guard);
  ctx->root_page_id_ = header_node->root_page_id_;

  return header_node->root_page_id_;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input >> key) {
    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, txn);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input >> key) {
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, txn);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage *page) {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;

  } else {
    auto *internal = reinterpret_cast<const InternalPage *>(page);
    std::cout << "Internal Page: " << page_id << std::endl;

    // Print the contents of the internal page.
    std::cout << "Contents: ";
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
      if ((i + 1) < internal->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out) {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << page_id << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }
  } else {
    auto *inner = reinterpret_cast<const InternalPage *>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_guard = bpm_->FetchPageBasic(inner->ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.PageId(), child_page, out);
      if (i > 0) {
        auto sibling_guard = bpm_->FetchPageBasic(inner->ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_guard.PageId() << " " << internal_prefix
              << child_guard.PageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.PageId() << " -> ";
      if (child_page->IsLeafPage()) {
        out << leaf_prefix << child_guard.PageId() << ";\n";
      } else {
        out << internal_prefix << child_guard.PageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string {
  if (IsEmpty()) {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree {
  auto root_page_guard = bpm_->FetchPageBasic(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;

  if (root_page->IsLeafPage()) {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page->ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page->ToString();
  proot.size_ = 0;
  for (int i = 0; i < internal_page->GetSize(); i++) {
    page_id_t child_id = internal_page->ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.push_back(child_node);
  }

  return proot;
}

// 模板提前实例化
template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
