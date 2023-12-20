#include <algorithm>

#include "common/bitmap.h"
#include "distributed/commit_log.h"
#include "distributed/metadata_server.h"
#include "filesystem/directory_op.h"
#include "metadata/inode.h"
#include <chrono>

namespace chfs {
    template<typename T>
    auto check(std::vector<T> vector, T element) -> bool {
        for (auto itr = vector.begin(); itr != vector.end(); itr++) {
            if (*itr == element) {
                return true;
            }
        }

        return false;
    }
    /**
     * `CommitLog` part
     */
    // {Your code here}
    CommitLog::CommitLog(std::shared_ptr<BlockManager> bm,
                         bool is_checkpoint_enabled)
            : is_checkpoint_enabled_(is_checkpoint_enabled), bm_(bm) {
        // this->log_entry_ = bm->log_entry_;
        this->cur_txn_id = 0;
        this->cur_log_entries = 0;
        this->log_blocks = bm->log_blocks;

        this->meta_start_id = bm->total_blocks();
        this->meta_blocks = this->log_blocks * sizeof(LogEntry) / bm->block_size();
        if (this->meta_blocks * bm->block_size() < this->log_blocks * sizeof(LogEntry)) {
            this->meta_blocks++;
        }

        this->bitmap_start_id = this->meta_start_id + this->meta_blocks;
        this->bitmap_blocks = this->log_blocks / DiskBlockSize * KBitsPerByte;
        if (this->bitmap_blocks * DiskBlockSize * KBitsPerByte < this->log_blocks) {
            this->bitmap_blocks++;
        }

        this->data_start_id = this->bitmap_start_id + this->bitmap_blocks;
        this->data_blocks = this->log_blocks - this->meta_blocks - this->bitmap_blocks;
    }

    CommitLog::~CommitLog() {}

    // {Your code here}
    auto CommitLog::get_log_entry_num() -> usize {
        // TODO: Implement this function.
        // UNIMPLEMENTED();
        return this->cur_log_entries;
    }

    // {Your code here}
    auto CommitLog::append_log(txn_id_t txn_id,
                               std::vector<std::shared_ptr<BlockOperation>> ops)
    -> void {
        // TODO: Implement this function.
        // UNIMPLEMENTED();
        //    std::vector<u8> buf(DiskBlockSize);
        for (auto op: ops) {
            block_id_t metadata_server_block_id = op->block_id_;
            std::vector<u8> content = op->new_block_state_;
            block_id_t operation_saving_block_id = allocate();
            CHFS_ASSERT(operation_saving_block_id >= data_start_id, "No enough space to save new state.");

            LogEntry entry = {txn_id, metadata_server_block_id, operation_saving_block_id};
            block_id_t entry_bid= append_entry(entry);
            bm_->sync(entry_bid);

            bm_->write_block(operation_saving_block_id, content.data());
            bm_->sync(operation_saving_block_id);
        }
    }

    // {Your code here}
    auto CommitLog::commit_log(txn_id_t txn_id) -> bool {
        // TODO: Implement this function.
        //    UNIMPLEMENTED();

        auto all_entry = get_all_entries();

        std::vector<u8> buf(DiskBlockSize);
        for (auto &e: all_entry) {
            auto [tid, meta_bid, op_bid] = e;
            if (tid == txn_id) {
                bm_->read_block(op_bid, buf.data());
                if (bm_->write_block(meta_bid, buf.data()).is_err()) {
                    return false;
                }
                bm_->sync(meta_bid);
            }
        }

        LogEntry commitEntry = {txn_id, 0, 0};
        block_id_t entry_bid= append_entry(commitEntry);
        bm_->sync(entry_bid);

        return true;
    }

    // {Your code here}
    auto CommitLog::checkpoint() -> void {
        // TODO: Implement this function.
        // UNIMPLEMENTED();
        auto all_entries = get_all_entries();
        std::vector<u8> buf(DiskBlockSize);
        std::vector<txn_id_t> logs_to_remove;
        std::vector<LogEntry> new_entries;
        // traverse entries
        for (auto itr = all_entries.begin(); itr != all_entries.end(); itr++) {
            auto [tid, meta_bid, op_bid] = (*itr);
            if (meta_bid == 0 && op_bid == 0) {
                logs_to_remove.push_back(tid);
            }
        }
        // delete committed logs
        for (auto itr = all_entries.begin(); itr != all_entries.end(); itr++) {
            auto [tid, meta_bid, op_bid] = (*itr);
            if(check(logs_to_remove,tid)){
                deallocate(op_bid);
            }else{
                new_entries.emplace_back(*itr);
            }
        }
        // flush new entries
        flush_all_entries(new_entries);
        this->cur_log_entries=new_entries.size();
    }

    // {Your code here}
    auto CommitLog::recover() -> void {
        // TODO: Implement this function.
        // UNIMPLEMENTED();
        checkpoint();
        auto all_entries = get_all_entries();
//        std::cout<<"entry size:"<<all_entries.size()<<std::endl;
//        for(auto &e:all_entries){
//            auto [tid, meta_bid, op_bid] =e;
//            std::cout<<"\tone entry: "<<tid<<" "<<meta_bid<<" "<<op_bid<<std::endl;
//        }
        std::vector<u8> buf(DiskBlockSize);
        for (auto itr = all_entries.begin(); itr != all_entries.end(); itr++) {
            auto [tid, meta_bid, op_bid] = (*itr);
            bm_->read_block(op_bid,buf.data());
            bm_->write_block(meta_bid,buf.data());
        }
    }

    auto CommitLog::allocate() -> block_id_t {
        std::vector<u8> buf(DiskBlockSize);

        for (u32 i = 0; i < bitmap_blocks; i++) {
            u32 start = 0, end = DiskBlockSize * KBitsPerByte;
            if (i == 0)
                start = data_start_id - meta_start_id;
            if (i == bitmap_blocks - 1)
                end = log_blocks - i * DiskBlockSize * KBitsPerByte;

            u32 bitmap_block_id = bitmap_start_id + i;
            bm_->read_block(bitmap_block_id, buf.data());

            Bitmap bitmap(buf.data(), DiskBlockSize);
            for (u32 j = start; j < end; j++) {
                bool is_used = bitmap.check(j);
                if (!is_used) {
                    bitmap.set(j);
                    bm_->write_block(bitmap_block_id, buf.data());
                    bm_->sync(bitmap_block_id);
                    return meta_start_id + i * DiskBlockSize * KBitsPerByte + j;
                }
            }
        }

        return 0;
    }

    auto CommitLog::deallocate(block_id_t block_id) -> bool {
        std::vector<u8> buf(DiskBlockSize);

        if (block_id < data_start_id || block_id >= data_start_id + data_blocks) {
            return false;
        }

        u32 bitmap_block_id =
                (block_id - meta_start_id) / (DiskBlockSize * KBitsPerByte) + bitmap_start_id;
        u32 bitmap_block_idx = (block_id - meta_start_id) % (DiskBlockSize * KBitsPerByte);
        bm_->read_block(bitmap_block_id, buf.data());
        Bitmap bitmap(buf.data(), DiskBlockSize);

        if (bitmap.check(bitmap_block_idx)) {
            bitmap.clear(bitmap_block_idx);
            bm_->write_block(bitmap_block_id, buf.data());
            bm_->sync(bitmap_block_id);

            bm_->zero_block(block_id);
            return true;
        }

        return false;
    }

    auto CommitLog::append_entry(LogEntry entry) -> block_id_t {
        std::vector<u8> buf(sizeof(LogEntry));
        *(LogEntry *) (buf.data()) = entry;

        block_id_t log_entry_block_id = cur_log_entries / (DiskBlockSize / sizeof(LogEntry)) + meta_start_id;
        CHFS_ASSERT(log_entry_block_id >= meta_start_id && log_entry_block_id < bitmap_start_id,
                    "No enough space to save entry.");
        block_id_t log_entry_block_idx = cur_log_entries % (DiskBlockSize / sizeof(LogEntry));
        CHFS_ASSERT(log_entry_block_idx * sizeof(LogEntry) <= DiskBlockSize, "Overflow!");

        bm_->write_partial_block(log_entry_block_id, buf.data(), log_entry_block_idx * sizeof(LogEntry),
                                 sizeof(LogEntry));

        bm_->sync(log_entry_block_id);
        cur_log_entries++;
        return log_entry_block_id;
    }

    auto CommitLog::parse_entry(block_id_t block_id, usize entry_num) -> std::vector<LogEntry> {
        std::vector<u8> buf(DiskBlockSize);
        std::vector<LogEntry> entry_vector;

        bm_->read_block(block_id, buf.data());
        for (u32 i = 0; i < entry_num; i++) {
            LogEntry entry;
            entry = *(LogEntry *) (buf.data() + i * sizeof(LogEntry));
            entry_vector.push_back(entry);
        }

        return entry_vector;
    }

    auto CommitLog::get_all_entries() -> std::vector<LogEntry> {
        usize total_entry_num = cur_log_entries;
        usize max_entry_in_one_block = DiskBlockSize / sizeof(LogEntry);
        auto cur_parse_block_id = meta_start_id;

        std::vector<LogEntry> all_entries;
        while (total_entry_num > 0) {
            auto parse_sz = std::min(max_entry_in_one_block, total_entry_num);
            std::vector<LogEntry> entry = parse_entry(cur_parse_block_id, parse_sz);
            all_entries.insert(all_entries.end(), entry.begin(), entry.end());

            cur_parse_block_id++;
            total_entry_num -= parse_sz;
        }

        return all_entries;
    }

    auto CommitLog::flush_all_entries(std::vector<LogEntry> all_entries) -> void {
        // auto size = all_entries.size();
        auto entry_per_block = DiskBlockSize / sizeof(LogEntry);
        auto cur_block_id = this->meta_start_id;
        std::vector<u8> buf(DiskBlockSize);

        for (auto i = 0; i < all_entries.size(); i++) {
            auto meta_id = i / entry_per_block + this->meta_start_id;
            auto meta_idx = i % entry_per_block;
            if (meta_id != cur_block_id) {
                bm_->write_block(cur_block_id, buf.data());
                memset(buf.data(), 0, DiskBlockSize);
                cur_block_id = meta_id;
            }
            *(LogEntry *) (buf.data() + meta_idx * sizeof(LogEntry)) = all_entries[i];
        }

        for (auto i = cur_block_id; i < bitmap_start_id; i++) {
            bm_->write_block(i, buf.data());
            memset(buf.data(), 0, DiskBlockSize);
        }
    }

    auto CommitLog::prepare() -> void {
        this->bm_->log_entry_ = std::make_shared<std::vector<std::shared_ptr<BlockOperation>>>();
    }

    auto CommitLog::clear() -> void {
        this->bm_->log_entry_ = nullptr;
    }

}; // namespace chfs