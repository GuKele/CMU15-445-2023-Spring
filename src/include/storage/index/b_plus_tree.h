/**
 * b_plus_tree.h
 *
 * Implementation of simple b+ tree data structure where internal pages direct
 * the search and leaf pages contain actual data.
 * (1) We only support unique key
 * (2) support insert & remove
 * (3) The structure should shrink and grow dynamically
 * (4) Implement index iterator for range scan
 */
#pragma once

#include <algorithm>
#include <deque>
#include <iostream>
#include <optional>
#include <queue>
#include <shared_mutex>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "common/config.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_header_page.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page_guard.h"
#include "type/value.h"

namespace bustub {

struct PrintableBPlusTree;

/**
 * @brief Definition of the Context class.
 *
 * Hint: This class is designed to help you keep track of the pages
 * that you're modifying or accessing.
 */
class Context {
 public:
  // When you insert into / remove from the B+ tree, store the write guard of header page here.
  // Remember to drop the header page guard and set it to nullopt when you want to unlock all.
  std::optional<WritePageGuard> header_page_{std::nullopt};

  // Save the root page id here so that it's easier to know if the current page is the root page.
  page_id_t root_page_id_{INVALID_PAGE_ID};

  // std::deque<std::variant<ReadPageGuard, WritePageGuard>> guard_set_;

  // Store the write guards of the pages that you're modifying here.
  std::deque<WritePageGuard> write_set_;

  // 存放write guards和自己在父亲中的index,方便找自己的同父亲兄弟(少一次在父亲中二分查找，避免重复计算)
  std::deque<std::pair<WritePageGuard, int>> write_guard_and_index_set_;

  // You may want to use this when getting value, but not necessary.
  std::deque<ReadPageGuard> read_set_;

  auto IsRootPage(page_id_t page_id) -> bool { return page_id == root_page_id_; }
};

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>

// Main class providing the API for the Interactive B+ Tree.
INDEX_TEMPLATE_ARGUMENTS
class BPlusTree {
  using BasePage = BPlusTreePage;
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  explicit BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                     const KeyComparator &comparator, int leaf_max_size = LEAF_PAGE_SIZE,
                     int internal_max_size = INTERNAL_PAGE_SIZE);

  // Returns true if this B+ tree has no keys and values.
  auto IsEmpty() const -> bool;

  // Insert a key-value pair into this B+ tree.
  auto Insert(const KeyType &key, const ValueType &value, Transaction *txn = nullptr) -> bool;

  // Remove a key and its value from this B+ tree.
  void Remove(const KeyType &key, Transaction *txn);

  // Return the value associated with a given key
  auto GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn = nullptr) -> bool;

  // Return the page id of the root node
  auto GetRootPageId() -> page_id_t;

  // Index iterator
  auto Begin() -> INDEXITERATOR_TYPE;

  auto End() -> INDEXITERATOR_TYPE;

  auto Begin(const KeyType &key) -> INDEXITERATOR_TYPE;

  // Print the B+ tree
  void Print(BufferPoolManager *bpm);

  // Draw the B+ tree
  void Draw(BufferPoolManager *bpm, const std::string &outf);

  /**
   * @brief draw a B+ tree, below is a printed
   * B+ tree(3 max leaf, 4 max internal) after inserting key:
   *  {1, 5, 9, 13, 17, 21, 25, 29, 33, 37, 18, 19, 20}
   *
   *                               (25)
   *                 (9,17,19)                          (33)
   *  (1,5)    (9,13)    (17,18)    (19,20,21)    (25,29)    (33,37)
   *
   * @return std::string
   */
  auto DrawBPlusTree() -> std::string;

  // read data from file and insert one by one
  void InsertFromFile(const std::string &file_name, Transaction *txn = nullptr);

  // read data from file and remove one by one
  void RemoveFromFile(const std::string &file_name, Transaction *txn = nullptr);

  // enum class Options {
  //   INSERT = 0,
  //   DELETE
  // };

 private:
  /*****************************************************************************
   * SEARCH
   *****************************************************************************/

  auto GetRootPageIdForRead(Context *ctx = nullptr) const -> page_id_t;

  /**
   * @brief 找到key-value可能存在的叶子节点
   *
   * 只读，所以查找叶子节点的过程中拿到孩子的读锁就释放父亲的读锁
   * @param key
   * @param context
   * @return nullptr表示树空
   */
  auto FindLeafForRead(const KeyType &key, Context *context) const -> const LeafPage *;

  auto FindLeftMostLeafForRead(Context *context) const -> const LeafPage *;

  /*****************************************************************************
   * INSERTION
   *****************************************************************************/

  auto GetRootPageIdForInsertOrDelete(Context *ctx) -> page_id_t;

  /**
   * @brief 为了插入或删除，找到叶子节点。
   *
   * 会拿写锁，所以当找到一个安全node(即插入不会分裂，那么其父亲也就不会插入了，父亲不需要写锁了;或者删除不会借兄弟或者合并兄弟)
   * 保证了不会修改父亲以及祖先，马上释放之前拿到的所有祖先写锁，注意如果没有更改不需要GetDataMut()给page置为脏叶
   * @param context
   * @param opt 指明是插入或删除而查找叶子节点
   * @return nullptr表示树空
   */
  auto FindLeafForOption(const KeyType &key, Context *context, BPlusTreeOption opt) -> LeafPage *;

  /**
   * @brief
   *
   * @param leaf_node leaf node to be split
   * @return the pointer of right brother leaf node that split off
   * @return nullptr for cannot allocate new page
   */
  auto SplitLeaf(LeafPage *leaf_node) -> LeafPage *;

  /**
   * @brief 创建一个新叶子作为树根,并更新root_page_id,并且在叶子中插入第一个键值对
   *
   * @param context 存放了write_header_page_guard
   * @return false
   */
  auto CreateNewTree(const KeyType &key, const ValueType &value, Context *context) -> bool;

  /**
   * @brief 将右兄弟的key和page_id插入到父亲节点
   *        1.如果父亲节点不存在(即当前分裂的节点就是root)，则创建新的root
   *        2.如果父亲节点存在但是父亲节点满了，我们就需要分裂父亲节点，再递归往上插入
   *        3.如果父亲节点存在并且没有满，直接插入就可以了
   * @param key 右兄弟对应的key
   * @param value 右兄弟的page_id
   * @param context
   * write_set_最后一个元素是孩子节点(left_page_guard),倒数第二个(如果有，即这个孩子不是root)是被插入的父亲page
   * @return true
   * @return false
   */
  auto InsertIntoParent(const KeyType &key, const page_id_t &value, Context *context) -> bool;

  /**
   * @brief 创建新的root_node,并且更新
   *
   * @param left
   * @param key
   * @param right
   * @param context
   * @return true
   * @return false
   */
  auto CreateNewRoot(const page_id_t &left, const KeyType &key, const page_id_t &right, Context *context) -> bool;

  /**
   * @brief 分裂internal节点,并且插入key-value
   * 在下面展示中，插入key==5的键值对，分裂pid==101的internal node,分裂后返回{key=7, page_id=103}
   *
   * 插入并分裂前：
   *                                               father-internal
   *                                         --------------------------
   *                                         |              k2(12)    |
   *                                         | pid1(101)    pid2(102) |
   *                                         --------------------------
   *                      child-internal pid(101)                child-internale pid(102)
   *  -----------------------------------------------------           ----------
   *  |              k2(3)       k3(7)        k4(9)       |           |        |
   *  |  pid1(-∞,3)  pid2[3,7)   pid3[7,9)    pid4[9,+∞)  |           |        |
   *  -----------------------------------------------------           ----------
   *
   * 分裂并插入父亲节点后：
   *                                             father-internal
   *                                  --------------------------------------
   *                                  |              k2(7)       k3(12)    |
   *                                  | pid1(101)    pid2(103)   pid3(102) |
   *                                  --------------------------------------
   *
   *        child-internal   pid(101)         new child-internal  pid(103)    child-internale pid(102)
   *  --------------------------------------   ---------------------------          ----------
   *  |              k2(3)       k3(5)     |   |              k2(9)      |          |        |
   *  | pid1(-∞,3)   pid2[3,7)   pid3[5,7) |   | pid1[7,9)    pid2[9,+∞) |          |        |
   *  --------------------------------------   ---------------------------          ----------
   *
   * @param i_node internnal node to be split
   * @param index key-pageid 插入后所应该在的下标
   * @return 返回新internal节点的key和page_id用于插入父亲节点。如果分配新page失败，返回INVALID_PAGE_ID
   */
  auto InsertAndSplitInternal(InternalPage *i_node, const KeyType &key, const page_id_t &value, int index)
      -> std::pair<KeyType, page_id_t>;

  /*****************************************************************************
   * REMOVE
   *****************************************************************************/

  /**
   * @brief delete element at index of parent internal node
   *
   * @param index
   */
  void DeleteFromParent(int index, Context *context);

  /* Debug Routines for FREE!! */
  void ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out);

  void PrintTree(page_id_t page_id, const BPlusTreePage *page);

  /**
   * @brief Convert A B+ tree into a Printable B+ tree
   *
   * @param root_id
   * @return PrintableNode
   */
  auto ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree;

 private:
  // member variable
  std::string index_name_;
  BufferPoolManager *bpm_;
  KeyComparator comparator_;
  std::vector<std::string> log;  // NOLINT
  int leaf_max_size_;
  int internal_max_size_;
  page_id_t header_page_id_;
};

/**
 * @brief for test only. PrintableBPlusTree is a printable B+ tree.
 * We first convert B+ tree into a printable B+ tree and the print it.
 */
struct PrintableBPlusTree {
  int size_;
  std::string keys_;
  std::vector<PrintableBPlusTree> children_;

  /**
   * @brief BFS traverse a printable B+ tree and print it into
   * into out_buf
   *
   * @param out_buf
   */
  void Print(std::ostream &out_buf) {
    std::vector<PrintableBPlusTree *> que = {this};
    while (!que.empty()) {
      std::vector<PrintableBPlusTree *> new_que;

      for (auto &t : que) {
        int padding = (t->size_ - t->keys_.size()) / 2;
        out_buf << std::string(padding, ' ');
        out_buf << t->keys_;
        out_buf << std::string(padding, ' ');

        for (auto &c : t->children_) {
          new_que.push_back(&c);
        }
      }
      out_buf << "\n";
      que = new_que;
    }
  }
};

}  // namespace bustub
