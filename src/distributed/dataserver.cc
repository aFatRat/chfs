#include "distributed/dataserver.h"
#include "common/util.h"

namespace chfs
{
  // const usize KBitsPerByte = 8;
  auto DataServer::initialize(std::string const &data_path)
  {
    /**
     * At first check whether the file exists or not.
     * If so, which means the distributed chfs has
     * already been initialized and can be rebuilt from
     * existing data.
     */
    bool is_initialized = is_file_exist(data_path);
    usize block_cnt = KDefaultBlockCnt;

    usize version_block_cnt = (block_cnt * sizeof(version_t)) / DiskBlockSize;
    if (version_block_cnt * (DiskBlockSize / sizeof(version_t)) < block_cnt)
    {
      version_block_cnt += 1;
    }

    auto bm = std::shared_ptr<BlockManager>(
        new BlockManager(data_path, KDefaultBlockCnt));
    if (is_initialized)
    {
      block_allocator_ =
          std::make_shared<BlockAllocator>(bm, version_block_cnt, false);
    }
    else
    {
      // We need to reserve some blocks for storing the version of each block
      block_allocator_ = std::shared_ptr<BlockAllocator>(
          new BlockAllocator(bm, version_block_cnt, true));
    }

    // Initialize the RPC server and bind all handlers
    server_->bind("read_data", [this](block_id_t block_id, usize offset,
                                      usize len, version_t version)
                  { return this->read_data(block_id, offset, len, version); });
    server_->bind("write_data", [this](block_id_t block_id, usize offset,
                                       std::vector<u8> &buffer)
                  { return this->write_data(block_id, offset, buffer); });
    server_->bind("alloc_block", [this]()
                  { return this->alloc_block(); });
    server_->bind("free_block", [this](block_id_t block_id)
                  { return this->free_block(block_id); });

    // Launch the rpc server to listen for requests
    server_->run(true, num_worker_threads);
  }

  DataServer::DataServer(u16 port, const std::string &data_path)
      : server_(std::make_unique<RpcServer>(port))
  {
    initialize(data_path);
  }

  DataServer::DataServer(std::string const &address, u16 port,
                         const std::string &data_path)
      : server_(std::make_unique<RpcServer>(address, port))
  {
    initialize(data_path);
  }

  DataServer::~DataServer() { server_.reset(); }

  auto read_version(std::shared_ptr<BlockManager> bm, block_id_t block_id) -> version_t
  {
    std::vector<u8> buf(DiskBlockSize);

    block_id_t version_block_id = block_id / (DiskBlockSize / sizeof(version_t));
    usize version_block_idx = block_id % (DiskBlockSize / sizeof(version_t));
    version_t block_version = 0;
    bm->read_block(version_block_id, buf.data());
    // for (int i = version_block_idx; i < version_block_idx + sizeof(version_t); i++)
    // {
    //   block_version = (block_version << 8) | buf[i];
    // }
    block_version = *(version_t *)(buf.data() + version_block_idx * sizeof(version_t));

    return block_version;
  }

  auto write_version(std::shared_ptr<BlockManager> bm, block_id_t block_id, version_t version) -> bool
  {
    // std::vector<u8> buf(DiskBlockSize);

    block_id_t version_block_id = block_id / (DiskBlockSize / sizeof(version_t));
    usize version_block_idx = block_id % (DiskBlockSize / sizeof(version_t));

    // *(buf.data()+version_block_idx*sizeof(version_t))=version;
    std::vector<u8> buf(sizeof(version_t));
    *(buf.data()) = version;

    auto res=bm->write_partial_block(version_block_id, buf.data(), version_block_idx * sizeof(version_t), sizeof(version_t));

    return res.is_ok();
  }

  // {Your code here}
  auto DataServer::read_data(block_id_t block_id, usize offset, usize len,
                             version_t version) -> std::vector<u8>
  {
    // TODO: Implement this function.
    //  UNIMPLEMENTED();
    std::vector<u8> ret(0);
    std::vector<u8> buf(DiskBlockSize);

    // check version
    // block_id_t version_block_id = block_id / (DiskBlockSize / sizeof(version_t));
    // usize version_block_idx = block_id % (DiskBlockSize / sizeof(version_t));
    // version_t block_version = 0;
    // this->block_allocator_->bm->read_block(version_block_id, buf.data());
    // for (int i = version_block_idx; i < version_block_idx + sizeof(version_t); i++)
    // {
    //   block_version = (block_version << 8) | buf[i];
    // }
    version_t block_version = read_version(this->block_allocator_->bm, block_id);

    // read data
    usize block_size = DiskBlockSize;
    usize real_len = std::min(block_size - offset, len);

    if (version < block_version || block_id >= block_allocator_->bm->total_blocks() || block_id < 0 || offset >= block_size)
    {
      // return ChfsNullResult(ErrorType::INVALID_ARG);
    }
    else
    {
      ret = std::vector<u8>(len);
      this->block_allocator_->bm->read_block(block_id, buf.data());
      std::copy(buf.begin() + offset, buf.begin() + offset + real_len, ret.begin());
      // return KNullOk;
    }

    return ret;

    // return {};
  }

  // {Your code here}
  auto DataServer::write_data(block_id_t block_id, usize offset,
                              std::vector<u8> &buffer) -> bool
  {
    // TODO: Implement this function.
    // UNIMPLEMENTED();
    if (true)
    {
      auto res= this->block_allocator_->bm->write_partial_block(block_id, buffer.data(), offset, buffer.size());

      // update version
      // version_t block_version = read_version(this->block_allocator_->bm, block_id);
      // block_version += 1;
      // write_version(this->block_allocator_->bm, block_id, block_version);

      return res.is_ok();
    }

    return false;
  }

  // {Your code here}
  auto DataServer::alloc_block() -> std::pair<block_id_t, version_t>
  {
    // TODO: Implement this function.
    // UNIMPLEMENTED();
    
    block_id_t block_id = this->block_allocator_->allocate().unwrap();

    version_t block_version = read_version(this->block_allocator_->bm, block_id);
    block_version += 1;
    bool res=write_version(this->block_allocator_->bm, block_id, block_version);
    if(!res){
      return {0,0};
    }

    return {block_id, block_version};
  }

  // {Your code here}
  auto DataServer::free_block(block_id_t block_id) -> bool
  {
    // TODO: Implement this function.
    // UNIMPLEMENTED();
    if (true)
    {
      if(this->block_allocator_->deallocate(block_id).is_err()){
        return false;
      }

      version_t block_version = read_version(this->block_allocator_->bm, block_id);
      block_version += 1;
      bool res=write_version(this->block_allocator_->bm, block_id, block_version);

      return res;
    }

    return false;
  }
} // namespace chfs