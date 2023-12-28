#include "block/allocator.h"
#include "common/bitmap.h"

namespace chfs
{

    BlockAllocator::BlockAllocator(std::shared_ptr<BlockManager> block_manager)
        : BlockAllocator(std::move(block_manager), 0, true) {}

    // Your implementation
    BlockAllocator::BlockAllocator(std::shared_ptr<BlockManager> block_manager,
                                   usize bitmap_block_id, bool will_initialize)
        : bm(std::move(block_manager)), bitmap_block_id(bitmap_block_id)
    {
        // calculate the total blocks required
        const auto total_bits_per_block = this->bm->block_size() * KBitsPerByte;
        auto total_bitmap_block = this->bm->total_blocks() / total_bits_per_block;
        if (this->bm->total_blocks() % total_bits_per_block != 0)
        {
            total_bitmap_block += 1;
        }

        CHFS_VERIFY(total_bitmap_block > 0, "Need blocks in the manager!");
        CHFS_VERIFY(total_bitmap_block + this->bitmap_block_id <= bm->total_blocks(),
                    "not available blocks to store the bitmap");

        this->bitmap_block_cnt = total_bitmap_block;
        if (this->bitmap_block_cnt * total_bits_per_block ==
            this->bm->total_blocks())
        {
            this->last_block_num = total_bits_per_block;
        }
        else
        {
            this->last_block_num = this->bm->total_blocks() % total_bits_per_block;
        }
        CHFS_VERIFY(this->last_block_num <= total_bits_per_block,
                    "last block num should be less than total bits per block");

        if (!will_initialize)
        {
            return;
        }

        // zeroing
        for (block_id_t i = 0; i < this->bitmap_block_cnt; i++)
        {
            this->bm->zero_block(i + this->bitmap_block_id);
        }

        block_id_t cur_block_id = this->bitmap_block_id;
        std::vector<u8> buffer(bm->block_size());
        auto bitmap = Bitmap(buffer.data(), bm->block_size());
        bitmap.zeroed();

        // set the blocks of the bitmap block to 1
        for (block_id_t i = 0; i < this->bitmap_block_cnt + this->bitmap_block_id;
             i++)
        {
            // + bitmap_block_id is necessary, since the bitmap block starts with an
            // offset
            auto block_id = i / total_bits_per_block + this->bitmap_block_id;
            auto block_idx = i % total_bits_per_block;

            if (block_id != cur_block_id)
            {
                bm->write_block(cur_block_id, buffer.data());

                cur_block_id = block_id;
                bitmap.zeroed();
            }

            bitmap.set(block_idx);
        }

        bm->write_block(cur_block_id, buffer.data());
    }

    // Fixme: currently we don't consider errors in this implementation
    auto BlockAllocator::free_block_cnt() const -> usize
    {
        usize total_free_blocks = 0;
        std::vector<u8> buffer(bm->block_size());

        for (block_id_t i = 0; i < this->bitmap_block_cnt; i++)
        {
            bm->read_block(i + this->bitmap_block_id, buffer.data()).unwrap();

            usize n_free_blocks = 0;
            if (i == this->bitmap_block_cnt - 1)
            {
                // last one
                n_free_blocks = Bitmap(buffer.data(), bm->block_size())
                                    .count_zeros_to_bound(this->last_block_num);
            }
            else
            {
                n_free_blocks = Bitmap(buffer.data(), bm->block_size()).count_zeros();
            }
            total_free_blocks += n_free_blocks;
        }
        return total_free_blocks;
    }

    // Your implementation
    auto BlockAllocator::allocate() -> ChfsResult<block_id_t>
    {
        std::vector<u8> buffer(bm->block_size());

        for (uint i = 0; i < this->bitmap_block_cnt; i++)
        {
            if (bm->read_block(i + this->bitmap_block_id, buffer.data()).is_err())
            {
                return ChfsResult<block_id_t>(ErrorType::INVALID);
            }

            // The index of the allocated bit inside current bitmap block.
            std::optional<block_id_t> res = std::nullopt;

            if (i == this->bitmap_block_cnt - 1)
            {
                // If current block is the last block of the bitmap.

                // TODO: Find the first free bit of current bitmap block
                // and store it in `res`.
                res = Bitmap(buffer.data(), bm->block_size()).find_first_free_w_bound(last_block_num);

                //                UNIMPLEMENTED();
            }
            else
            {

                // TODO: Find the first free bit of current bitmap block
                // and store it in `res`.
                res = Bitmap(buffer.data(), bm->block_size()).find_first_free();
                //                UNIMPLEMENTED();
            }

            // If we find one free bit inside current bitmap block.
            if (res)
            {
                // The block id of the allocated block.
                block_id_t retval = static_cast<block_id_t>(0);

                // TODO:
                // 1. Set the free bit we found to 1 in the bitmap.
                Bitmap(buffer.data(), bm->block_size()).set(res.value());

                // 2. Flush the changed bitmap block back to the block manager.
                // if (bm->write_block(i + this->bitmap_block_id, buffer.data()).is_err())
                // {
                //     return ChfsResult<block_id_t>(ErrorType::INVALID);
                // }
                // auto op = std::make_shared<BlockOperation>(new BlockOperation(i + this->bitmap_block_id, buffer));
                // bm->log_entry_->emplace_back(op);
                bm->append_to_entry(i + this->bitmap_block_id,buffer);

                // 3. Calculate the value of `retval`.
                retval = res.value() + i * bm->block_size() * KBitsPerByte;
                //                bm->zero_block(retval);
                //                UNIMPLEMENTED();
                //                printf("allocate block:%lu\n",retval);
                return ChfsResult<block_id_t>(retval);
            }
        }
        return ChfsResult<block_id_t>(ErrorType::OUT_OF_RESOURCE);
    }

    // Your implementation
    auto BlockAllocator::deallocate(block_id_t block_id) -> ChfsNullResult
    {
        //        printf("Deallocate %lu\n",block_id);
        if (block_id >= this->bm->total_blocks())
        {
            printf("Deallocate error1!\n");
            return ChfsNullResult(ErrorType::INVALID_ARG);
        }

        // TODO: Implement this function.
        // 1. According to `block_id`, zero the bit in the bitmap.
        // 2. Flush the changed bitmap block back to the block manager.
        // 3. Return ChfsNullResult(ErrorType::INVALID_ARG)
        //    if you find `block_id` is invalid (e.g. already freed).

        std::vector<u8> buffer(bm->block_size());
        u8 cur_bitmap_block_id = block_id / (bm->block_size() * KBitsPerByte);
        usize cur_bitmap_block_offset = block_id % (bm->block_size() * KBitsPerByte);
        if (bm->read_block(cur_bitmap_block_id + this->bitmap_block_id, buffer.data()).is_err())
        {
            return ChfsNullResult(ErrorType::INVALID);
        }
        Bitmap bitmap = Bitmap(buffer.data(), bm->block_size());

        if (bitmap.check(cur_bitmap_block_offset))
        {
            bitmap.clear(cur_bitmap_block_offset);
            // if (bm->write_block(cur_bitmap_block_id + this->bitmap_block_id, buffer.data()).is_err())
            // {
            //     return ChfsNullResult(ErrorType::INVALID);
            // }
            // auto op=std::make_shared<BlockOperation>(new BlockOperation(cur_bitmap_block_id + this->bitmap_block_id,buffer));
            // bm->log_entry_->emplace_back(op);
            bm->append_to_entry(cur_bitmap_block_id + this->bitmap_block_id,buffer);
            // if (bm->zero_block(block_id).is_err())
            // {
            //     return ChfsNullResult(ErrorType::INVALID);
            // }
            std::vector<u8> buf(DiskBlockSize);
            // auto op_=std::make_shared<BlockOperation>(new BlockOperation(block_id,buf));
            // bm->log_entry_->emplace_back(op_);
            bm->append_to_entry(block_id,buf);
        }
        else
        {
            printf("Deallocate error2!\n");
            return ChfsNullResult(ErrorType::INVALID_ARG);
        }

        //        UNIMPLEMENTED();

        return KNullOk;
    }

} // namespace chfs
