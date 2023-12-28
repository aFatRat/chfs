//===----------------------------------------------------------------------===//
//
//                         Chfs
//
// commit_log.h
//
// Identification: src/include/distributed/commit_log.h
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "block/manager.h"
#include "common/config.h"
#include "common/macros.h"
#include "filesystem/operations.h"
#include <atomic>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <unordered_set>
#include <vector>

namespace chfs
{
  // using LogInfo = std::tuple<txn_id_t, block_id_t, mac_id_t>;

  /**
   * `CommitLog` is a class that records the block edits into the
   * commit log. It's used to redo the operation when the system
   * is crashed.
   */
  class CommitLog
  {
  public:
    explicit CommitLog(std::shared_ptr<BlockManager> bm,
                       bool is_checkpoint_enabled);
    ~CommitLog();
    auto append_log(txn_id_t txn_id,
                    std::vector<std::shared_ptr<BlockOperation>> ops) -> void;
    auto commit_log(txn_id_t txn_id) -> bool;
    auto checkpoint() -> void;
    auto recover() -> void;
    auto get_log_entry_num() -> usize;

    bool is_checkpoint_enabled_;
    std::shared_ptr<BlockManager> bm_;
    /**
     * {Append anything if you need}
     */
    auto allocate() -> block_id_t;
    auto deallocate(block_id_t block_id) -> bool;
    auto append_entry(LogEntry entry) -> block_id_t ;
    auto parse_entry(block_id_t block_id, usize entry_num) -> std::vector<LogEntry>;
    auto get_all_entries() -> std::vector<LogEntry>;
    auto flush_all_entries(std::vector<LogEntry> all_entries) -> void;
    auto prepare() -> void;
    auto clear() -> void;

    usize cur_txn_id;
    usize log_blocks;
    usize meta_start_id;
    usize meta_blocks;
    usize bitmap_start_id;
    usize bitmap_blocks;
    usize data_start_id;
    usize data_blocks;
    usize cur_log_entries;
  };

} // namespace chfs