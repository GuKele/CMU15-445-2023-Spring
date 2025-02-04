//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_test.cpp
//
// Identification: test/buffer/buffer_pool_manager_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <cstdio>
#include <random>
#include <string>

#include "gtest/gtest.h"

namespace bustub {

// NOLINTNEXTLINE
// Check whether pages containing terminal characters can be recovered
TEST(BufferPoolManagerTest, BinaryDataTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 10;
  const size_t k = 5;

  std::random_device r;
  std::default_random_engine rng(r());
  std::uniform_int_distribution<char> uniform_dist(0);

  auto *disk_manager = new DiskManager(db_name);
  auto *bpm = new BufferPoolManager(buffer_pool_size, disk_manager, k);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);

  // Scenario: The buffer pool is empty. We should be able to create a new page.
  ASSERT_NE(nullptr, page0);
  EXPECT_EQ(0, page_id_temp);

  char random_binary_data[BUSTUB_PAGE_SIZE];
  // Generate random binary data
  for (char &i : random_binary_data) {
    i = uniform_dist(rng);
  }

  // Insert terminal characters both in the middle and at end
  random_binary_data[BUSTUB_PAGE_SIZE / 2] = '\0';
  random_binary_data[BUSTUB_PAGE_SIZE - 1] = '\0';

  // Scenario: Once we have a page, we should be able to read and write content.
  std::memcpy(page0->GetData(), random_binary_data, BUSTUB_PAGE_SIZE);
  EXPECT_EQ(0, std::memcmp(page0->GetData(), random_binary_data, BUSTUB_PAGE_SIZE));

  // Scenario: We should be able to create new pages until we fill up the buffer pool.
  for (size_t i = 1; i < buffer_pool_size; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: Once the buffer pool is full, we should not be able to create any new pages.
  for (size_t i = buffer_pool_size; i < buffer_pool_size * 2; ++i) {
    EXPECT_EQ(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: After unpinning pages {0, 1, 2, 3, 4} we should be able to create 5 new pages
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(true, bpm->UnpinPage(i, true));
    bpm->FlushPage(i);
  }
  for (int i = 0; i < 5; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
    bpm->UnpinPage(page_id_temp, false);
  }
  // Scenario: We should be able to fetch the data we wrote a while ago.
  // 从磁盘重新读回来page0
  page0 = bpm->FetchPage(0);
  EXPECT_EQ(0, memcmp(page0->GetData(), random_binary_data, BUSTUB_PAGE_SIZE));
  EXPECT_EQ(true, bpm->UnpinPage(0, true));

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
  remove("test.db");

  delete bpm;
  delete disk_manager;
}

// NOLINTNEXTLINE
TEST(BufferPoolManagerTest, SampleTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 10;
  const size_t k = 5;

  // BufferPoolManager temp{1, nullptr, 1};
  std::unordered_map<page_id_t, frame_id_t> just_for_test = {
      {11, 11},
  };

  auto *disk_manager = new DiskManager(db_name);
  auto *bpm = new BufferPoolManager(buffer_pool_size, disk_manager, k);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);

  // Scenario: The buffer pool is empty. We should be able to create a new page.
  ASSERT_NE(nullptr, page0);
  EXPECT_EQ(0, page_id_temp);

  // Scenario: Once we have a page, we should be able to read and write content.
  snprintf(page0->GetData(), BUSTUB_PAGE_SIZE, "Hello");
  EXPECT_EQ(0, strcmp(page0->GetData(), "Hello"));

  // Scenario: We should be able to create new pages until we fill up the buffer pool.
  for (size_t i = 1; i < buffer_pool_size; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: Once the buffer pool is full, we should not be able to create any new pages.
  for (size_t i = buffer_pool_size; i < buffer_pool_size * 2; ++i) {
    EXPECT_EQ(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: After unpinning pages {0, 1, 2, 3, 4} and pinning another 4 new pages,
  // there would still be one buffer page left for reading page 0.
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(true, bpm->UnpinPage(i, true));
  }
  for (int i = 0; i < 4; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: We should be able to fetch the data we wrote a while ago.
  page0 = bpm->FetchPage(0);
  EXPECT_EQ(0, strcmp(page0->GetData(), "Hello"));

  // Scenario: If we unpin page 0 and then make a new page, all the buffer pages should
  // now be pinned. Fetching page 0 should fail.
  EXPECT_EQ(true, bpm->UnpinPage(0, true));
  EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
  EXPECT_EQ(nullptr, bpm->FetchPage(0));

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
  remove("test.db");

  delete bpm;
  delete disk_manager;
}

TEST(otherBufferPoolManagerTest, SampleTest) {
  page_id_t temp_page_id;

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager bpm(10, disk_manager);

  auto page_zero = bpm.NewPage(&temp_page_id);
  EXPECT_NE(nullptr, page_zero);
  EXPECT_EQ(0, temp_page_id);

  // The test will fail here if the page is null
  ASSERT_NE(nullptr, page_zero);

  // change content in page one
  snprintf(page_zero->GetData(), sizeof(page_zero->GetData()), "Hello");
  // strcpy(page_zero->GetData(), "Hello");

  for (int i = 1; i < 10; ++i) {
    EXPECT_NE(nullptr, bpm.NewPage(&temp_page_id));
  }
  // all the pages are pinned, the buffer pool is full
  for (int i = 10; i < 15; ++i) {
    EXPECT_EQ(nullptr, bpm.NewPage(&temp_page_id));
  }
  // unpin the first five pages, add them to LRU list, set as dirty
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(true, bpm.UnpinPage(i, true));
  }
  // we have 5 empty slots in LRU list, evict page zero out of buffer pool
  for (int i = 10; i < 14; ++i) {
    EXPECT_NE(nullptr, bpm.NewPage(&temp_page_id));
  }
  // fetch page one again
  page_zero = bpm.FetchPage(0);
  // check read content
  EXPECT_EQ(0, strcmp(page_zero->GetData(), "Hello"));

  remove("test.db");

  delete disk_manager;
}

TEST(otherBufferPoolManagerTest, SampleTest_2) {
  page_id_t temp_page_id;

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager bpm(10, disk_manager);

  auto page_zero = bpm.NewPage(&temp_page_id);
  EXPECT_NE(nullptr, page_zero);
  EXPECT_EQ(0, temp_page_id);

  // The test will fail here if the page is null
  ASSERT_NE(nullptr, page_zero);

  // change content in page one
  // strcpy(page_zero->GetData(), "Hello");
  snprintf(page_zero->GetData(), sizeof(page_zero->GetData()), "Hello");

  for (int i = 1; i < 10; ++i) {
    EXPECT_NE(nullptr, bpm.NewPage(&temp_page_id));
  }

  // unpin the first five pages, add them to LRU list, set as dirty
  for (int i = 0; i < 1; ++i) {
    EXPECT_EQ(true, bpm.UnpinPage(i, true));
    page_zero = bpm.FetchPage(0);
    EXPECT_EQ(0, strcmp(page_zero->GetData(), "Hello"));
    EXPECT_EQ(true, bpm.UnpinPage(i, true));
    EXPECT_NE(nullptr, bpm.NewPage(&temp_page_id));
  }

  std::vector<int> test{5, 6, 7, 8, 9, 10};

  for (auto v : test) {
    Page *page = bpm.FetchPage(v);
    if (page == nullptr) {
      assert(false);
    }
    EXPECT_EQ(v, page->GetPageId());
    bpm.UnpinPage(v, true);
  }

  bpm.UnpinPage(10, true);

  // fetch page one again
  page_zero = bpm.FetchPage(0);
  // check read content
  EXPECT_EQ(0, strcmp(page_zero->GetData(), "Hello"));

  remove("test.db");

  delete disk_manager;
}

TEST(BufferPoolManagerTest, HardTest_1) {  // NOLINT
  page_id_t temp_page_id;
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManager(10, disk_manager, 5);

  std::vector<page_id_t> page_ids;
  for (int j = 0; j < 1000; j++) {
    for (int i = 0; i < 10; i++) {
      auto *new_page = bpm->NewPage(&temp_page_id);
      ASSERT_NE(nullptr, new_page);
      std::snprintf(new_page->GetData(), sizeof(new_page->GetData()), "%s",
                    std::to_string(temp_page_id).c_str());  // NOLINT
      page_ids.push_back(temp_page_id);
    }
    for (unsigned int i = page_ids.size() - 10; i < page_ids.size() - 5; i++) {
      ASSERT_EQ(1, bpm->UnpinPage(page_ids[i], false));
    }
    for (unsigned int i = page_ids.size() - 5; i < page_ids.size(); i++) {
      ASSERT_EQ(1, bpm->UnpinPage(page_ids[i], true));
    }
  }

  for (int j = 0; j < 10000; j++) {
    auto *page = bpm->FetchPage(page_ids[j]);
    ASSERT_NE(nullptr, page);
    if (j % 10 < 5) {
      ASSERT_NE(0, std::strcmp(std::to_string(page_ids[j]).c_str(), (page->GetData())));
    } else {
      ASSERT_EQ(0, std::strcmp(std::to_string(page_ids[j]).c_str(), (page->GetData())));
    }
    ASSERT_EQ(1, bpm->UnpinPage(page_ids[j], true));
  }

  auto rng = std::default_random_engine{};
  std::shuffle(page_ids.begin(), page_ids.end(), rng);

  for (int j = 0; j < 5000; j++) {
    auto *page = bpm->FetchPage(page_ids[j]);
    ASSERT_NE(nullptr, page);
    ASSERT_EQ(1, bpm->UnpinPage(page_ids[j], false));
    ASSERT_EQ(1, bpm->DeletePage(page_ids[j]));
  }

  for (int j = 5000; j < 10000; j++) {
    auto *page = bpm->FetchPage(page_ids[j]);
    ASSERT_NE(nullptr, page);
    if (page_ids[j] % 10 < 5) {
      ASSERT_NE(0, std::strcmp(std::to_string(page_ids[j]).c_str(), (page->GetData())));
    } else {
      ASSERT_EQ(0, std::strcmp(std::to_string(page_ids[j]).c_str(), (page->GetData())));
    }
    ASSERT_EQ(1, bpm->UnpinPage(page_ids[j], false));
    ASSERT_EQ(1, bpm->DeletePage(page_ids[j]));
  }

  remove("test.db");
  remove("test.log");
  delete bpm;
  delete disk_manager;
}

TEST(BufferPoolManagerTest, HardTest_2) {  // NOLINT
  const int num_threads = 5;
  const int num_runs = 50;
  for (int run = 0; run < num_runs; run++) {
    auto *disk_manager = new DiskManager("test.db");
    std::shared_ptr<BufferPoolManager> bpm{new BufferPoolManager(50, disk_manager)};
    std::vector<std::thread> threads;

    page_id_t temp_page_id;
    std::vector<page_id_t> page_ids;
    for (int i = 0; i < 50; i++) {
      auto *new_page = bpm->NewPage(&temp_page_id);
      ASSERT_NE(nullptr, new_page);
      std::snprintf(new_page->GetData(), sizeof(new_page->GetData()), "%s",
                    std::to_string(temp_page_id).c_str());  // NOLINT
      page_ids.push_back(temp_page_id);
    }

    for (int i = 0; i < 50; i++) {
      if (i % 2 == 0) {  // 偶数被标记为dirty
        ASSERT_EQ(1, bpm->UnpinPage(page_ids[i], true));
      } else {
        ASSERT_EQ(1, bpm->UnpinPage(page_ids[i], false));
      }
    }

    // 将之前的page替换一轮并刷盘(只有偶数被标记为dirty)
    for (int i = 0; i < 50; i++) {
      auto *new_page = bpm->NewPage(&temp_page_id);
      ASSERT_NE(nullptr, new_page);
      ASSERT_EQ(1, bpm->UnpinPage(temp_page_id, true));
    }

    for (int j = 0; j < 50; j++) {
      auto *page = bpm->FetchPage(page_ids[j]);
      ASSERT_NE(nullptr, page);
      std::snprintf(page->GetData(), sizeof(page->GetData()), "%s",
                    (std::string("Hard") + std::to_string(page_ids[j])).c_str());  // NOLINT
    }

    for (int i = 0; i < 50; i++) {
      if (i % 2 == 0) {
        ASSERT_EQ(1, bpm->UnpinPage(page_ids[i], false));
      } else {  // 奇数被标记为dirty
        ASSERT_EQ(1, bpm->UnpinPage(page_ids[i], true));
      }
    }

    // 将之前的page替换一轮并刷盘(只有奇数被标记为dirty)，所以前五十中偶数page内容为page id，而奇数为Hard + page id
    for (int i = 0; i < 50; i++) {
      auto *new_page = bpm->NewPage(&temp_page_id);
      ASSERT_NE(nullptr, new_page);
      ASSERT_EQ(1, bpm->UnpinPage(temp_page_id, true));
    }

    for (int tid = 0; tid < num_threads; tid++) {
      threads.push_back(std::thread([&bpm, tid, page_ids]() {  // NOLINT
        int j = (tid * 10);
        while (j < 50) {
          auto *page = bpm->FetchPage(page_ids[j]);
          while (page == nullptr) {
            page = bpm->FetchPage(page_ids[j]);
          }
          ASSERT_NE(nullptr, page);
          if (j % 2 == 0) {
            // FIGURE OUT(gukele): 为什么此时GetData() 不需要加读锁呢？
            ASSERT_EQ(0, std::strcmp(std::to_string(page_ids[j]).c_str(), (page->GetData())));
            ASSERT_EQ(1, bpm->UnpinPage(page_ids[j], false));
          } else {
            ASSERT_EQ(0, std::strcmp((std::string("Hard") + std::to_string(page_ids[j])).c_str(), (page->GetData())));
            ASSERT_EQ(1, bpm->UnpinPage(page_ids[j], false));
          }
          j = (j + 1);
        }
      }));
    }

    for (int i = 0; i < num_threads; i++) {
      threads[i].join();
    }

    for (int j = 0; j < 50; j++) {
      ASSERT_EQ(1, bpm->DeletePage(page_ids[j]));
    }

    remove("test.db");
    remove("test.log");
    delete disk_manager;
  }
}

TEST(BufferPoolManagerTest, HardTest_3) {  // NOLINT
  const int num_threads = 5;
  const int num_runs = 50;
  for (int run = 0; run < num_runs; run++) {
    auto *disk_manager = new DiskManager("test.db");
    std::shared_ptr<BufferPoolManager> bpm{new BufferPoolManager(50, disk_manager)};
    std::vector<std::thread> threads;

    page_id_t temp_page_id;
    std::vector<page_id_t> page_ids;
    for (int i = 0; i < 50; i++) {
      auto *new_page = bpm->NewPage(&temp_page_id);
      ASSERT_NE(nullptr, new_page);
      std::snprintf(new_page->GetData(), sizeof(new_page->GetData()), "%s",
                    std::to_string(temp_page_id).c_str());  // NOLINT
      page_ids.push_back(temp_page_id);
    }

    for (int i = 0; i < 50; i++) {
      if (i % 2 == 0) {
        ASSERT_EQ(1, bpm->UnpinPage(page_ids[i], true));
      } else {
        ASSERT_EQ(1, bpm->UnpinPage(page_ids[i], false));
      }
    }

    for (int i = 0; i < 50; i++) {
      auto *new_page = bpm->NewPage(&temp_page_id);
      ASSERT_NE(nullptr, new_page);
      ASSERT_EQ(1, bpm->UnpinPage(temp_page_id, true));
    }

    for (int j = 0; j < 50; j++) {
      auto *page = bpm->FetchPage(page_ids[j]);
      ASSERT_NE(nullptr, page);
      std::snprintf(page->GetData(), sizeof(page->GetData()), "%s",
                    (std::string("Hard") + std::to_string(page_ids[j])).c_str());  // NOLINT
    }

    for (int i = 0; i < 50; i++) {
      if (i % 2 == 0) {
        ASSERT_EQ(1, bpm->UnpinPage(page_ids[i], false));
      } else {
        ASSERT_EQ(1, bpm->UnpinPage(page_ids[i], true));
      }
    }

    for (int i = 0; i < 50; i++) {
      auto *new_page = bpm->NewPage(&temp_page_id);
      ASSERT_NE(nullptr, new_page);
      ASSERT_EQ(1, bpm->UnpinPage(temp_page_id, true));
    }

    for (int tid = 0; tid < num_threads; tid++) {
      threads.push_back(std::thread([&bpm, tid, page_ids]() {  // NOLINT
        page_id_t temp_page_id;
        int j = (tid * 10);
        while (j < 50) {
          if (j != tid * 10) {
            auto *page_local = bpm->FetchPage(temp_page_id);
            while (page_local == nullptr) {
              page_local = bpm->FetchPage(temp_page_id);
            }
            ASSERT_NE(nullptr, page_local);
            ASSERT_EQ(0, std::strcmp(std::to_string(temp_page_id).c_str(), (page_local->GetData())));
            ASSERT_EQ(1, bpm->UnpinPage(temp_page_id, false));
            // If the page is still in buffer pool then put it in free list,
            // else also we are happy
            ASSERT_EQ(1, bpm->DeletePage(temp_page_id));
          }

          auto *page = bpm->FetchPage(page_ids[j]);
          while (page == nullptr) {
            page = bpm->FetchPage(page_ids[j]);
          }
          ASSERT_NE(nullptr, page);
          if (j % 2 == 0) {
            ASSERT_EQ(0, std::strcmp(std::to_string(page_ids[j]).c_str(), (page->GetData())));
            ASSERT_EQ(1, bpm->UnpinPage(page_ids[j], false));
          } else {
            ASSERT_EQ(0, std::strcmp((std::string("Hard") + std::to_string(page_ids[j])).c_str(), (page->GetData())));
            ASSERT_EQ(1, bpm->UnpinPage(page_ids[j], false));
          }
          j = (j + 1);

          page = bpm->NewPage(&temp_page_id);
          while (page == nullptr) {
            page = bpm->NewPage(&temp_page_id);
          }
          ASSERT_NE(nullptr, page);
          std::snprintf(page->GetData(), sizeof(page->GetData()), "%s",
                        std::to_string(temp_page_id).c_str());  // NOLINT
          ASSERT_EQ(1, bpm->UnpinPage(temp_page_id, true));
        }
      }));
    }

    for (int i = 0; i < num_threads; i++) {
      threads[i].join();
    }

    for (int j = 0; j < 50; j++) {
      ASSERT_EQ(1, bpm->DeletePage(page_ids[j]));
    }

    remove("test.db");
    remove("test.log");
    delete disk_manager;
  }
}

TEST(BufferPoolManagerTest, HardTest_4) {  // NOLINT
  const int num_threads = 5;
  const int num_runs = 50;
  for (int run = 0; run < num_runs; run++) {
    auto *disk_manager = new DiskManager("test.db");
    std::shared_ptr<BufferPoolManager> bpm{new BufferPoolManager(50, disk_manager)};
    std::vector<std::thread> threads;

    page_id_t temp_page_id;
    std::vector<page_id_t> page_ids;
    for (int i = 0; i < 50; i++) {
      auto *new_page = bpm->NewPage(&temp_page_id);
      ASSERT_NE(nullptr, new_page);
      ASSERT_NE(nullptr, new_page);
      std::snprintf(new_page->GetData(), sizeof(new_page->GetData()), "%s",
                    std::to_string(temp_page_id).c_str());  // NOLINT
      page_ids.push_back(temp_page_id);
    }

    for (int i = 0; i < 50; i++) {
      if (i % 2 == 0) {
        ASSERT_EQ(1, bpm->UnpinPage(page_ids[i], true));
      } else {
        ASSERT_EQ(1, bpm->UnpinPage(page_ids[i], false));
      }
    }

    for (int i = 0; i < 50; i++) {
      auto *new_page = bpm->NewPage(&temp_page_id);
      ASSERT_NE(nullptr, new_page);
      ASSERT_EQ(1, bpm->UnpinPage(temp_page_id, true));
    }

    for (int j = 0; j < 50; j++) {
      auto *page = bpm->FetchPage(page_ids[j]);
      ASSERT_NE(nullptr, page);
      std::snprintf(page->GetData(), sizeof(page->GetData()), "%s",
                    (std::string("Hard") + std::to_string(page_ids[j])).c_str());  // NOLINT
    }

    for (int i = 0; i < 50; i++) {
      if (i % 2 == 0) {
        ASSERT_EQ(1, bpm->UnpinPage(page_ids[i], false));
      } else {
        ASSERT_EQ(1, bpm->UnpinPage(page_ids[i], true));
      }
    }

    for (int i = 0; i < 50; i++) {
      auto *new_page = bpm->NewPage(&temp_page_id);
      ASSERT_NE(nullptr, new_page);
      ASSERT_EQ(1, bpm->UnpinPage(temp_page_id, true));
    }

    for (int tid = 0; tid < num_threads; tid++) {
      threads.push_back(std::thread([&bpm, tid, page_ids]() {  // NOLINT
        page_id_t temp_page_id;
        int j = (tid * 10);
        while (j < 50) {
          if (j != tid * 10) {
            auto *page_local = bpm->FetchPage(temp_page_id);
            while (page_local == nullptr) {
              page_local = bpm->FetchPage(temp_page_id);
            }
            ASSERT_NE(nullptr, page_local);
            ASSERT_EQ(0, std::strcmp(std::to_string(temp_page_id).c_str(), (page_local->GetData())));
            ASSERT_EQ(1, bpm->UnpinPage(temp_page_id, false));
            // If the page is still in buffer pool then put it in free list,
            // else also we are happy
            ASSERT_EQ(1, bpm->DeletePage(temp_page_id));
          }

          auto *page = bpm->FetchPage(page_ids[j]);
          while (page == nullptr) {
            page = bpm->FetchPage(page_ids[j]);
          }
          ASSERT_NE(nullptr, page);
          if (j % 2 == 0) {
            ASSERT_EQ(0, std::strcmp(std::to_string(page_ids[j]).c_str(), (page->GetData())));
            ASSERT_EQ(1, bpm->UnpinPage(page_ids[j], false));
          } else {
            ASSERT_EQ(0, std::strcmp((std::string("Hard") + std::to_string(page_ids[j])).c_str(), (page->GetData())));
            ASSERT_EQ(1, bpm->UnpinPage(page_ids[j], false));
          }
          j = (j + 1);

          page = bpm->NewPage(&temp_page_id);
          while (page == nullptr) {
            page = bpm->NewPage(&temp_page_id);
          }
          ASSERT_NE(nullptr, page);
          std::snprintf(page->GetData(), sizeof(page->GetData()), "%s",
                        std::to_string(temp_page_id).c_str());  // NOLINT
          // FLush page instead of unpinning with true
          ASSERT_EQ(1, bpm->FlushPage(temp_page_id));
          ASSERT_EQ(1, bpm->UnpinPage(temp_page_id, false));

          // Flood with new pages
          for (int k = 0; k < 10; k++) {
            page_id_t flood_page_id;
            auto *flood_page = bpm->NewPage(&flood_page_id);
            while (flood_page == nullptr) {
              flood_page = bpm->NewPage(&flood_page_id);
            }
            ASSERT_NE(nullptr, flood_page);
            ASSERT_EQ(1, bpm->UnpinPage(flood_page_id, false));
            // If the page is still in buffer pool then put it in free list,
            // else also we are happy
            ASSERT_EQ(1, bpm->DeletePage(flood_page_id));
          }
        }
      }));
    }

    for (int i = 0; i < num_threads; i++) {
      threads[i].join();
    }

    for (int j = 0; j < 50; j++) {
      ASSERT_EQ(1, bpm->DeletePage(page_ids[j]));
    }

    remove("test.db");
    remove("test.log");
    delete disk_manager;
  }
}

}  // namespace bustub
