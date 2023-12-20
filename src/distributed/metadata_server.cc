#include "distributed/metadata_server.h"
#include "common/util.h"
#include "filesystem/directory_op.h"
#include <fstream>

namespace chfs
{
  inline auto MetadataServer::bind_handlers()
  {
    server_->bind("mknode",
                  [this](u8 type, inode_id_t parent, std::string const &name)
                  {
                    return this->mknode(type, parent, name);
                  });
    server_->bind("unlink", [this](inode_id_t parent, std::string const &name)
                  { return this->unlink(parent, name); });
    server_->bind("lookup", [this](inode_id_t parent, std::string const &name)
                  { return this->lookup(parent, name); });
    server_->bind("get_block_map",
                  [this](inode_id_t id)
                  { return this->get_block_map(id); });
    server_->bind("alloc_block",
                  [this](inode_id_t id)
                  { return this->allocate_block(id); });
    server_->bind("free_block",
                  [this](inode_id_t id, block_id_t block, mac_id_t machine_id)
                  {
                    return this->free_block(id, block, machine_id);
                  });
    server_->bind("readdir", [this](inode_id_t id)
                  { return this->readdir(id); });
    server_->bind("get_type_attr",
                  [this](inode_id_t id)
                  { return this->get_type_attr(id); });
  }

  inline auto MetadataServer::init_fs(const std::string &data_path)
  {
    /**
     * Check whether the metadata exists or not.
     * If exists, we wouldn't create one from scratch.
     */
    bool is_initialed = is_file_exist(data_path);

    auto block_manager = std::shared_ptr<BlockManager>(nullptr);
    if (is_log_enabled_)
    {
      block_manager =
          std::make_shared<BlockManager>(data_path, KDefaultBlockCnt, true);
    }
    else
    {
      block_manager = std::make_shared<BlockManager>(data_path, KDefaultBlockCnt);
    }

    CHFS_ASSERT(block_manager != nullptr, "Cannot create block manager.");

    if (is_initialed)
    {
      auto origin_res = FileOperation::create_from_raw(block_manager);
      std::cout << "Restarting..." << std::endl;
      if (origin_res.is_err())
      {
        std::cerr << "Original FS is bad, please remove files manually."
                  << std::endl;
        exit(1);
      }

      operation_ = origin_res.unwrap();
    }
    else
    {
      operation_ = std::make_shared<FileOperation>(block_manager,
                                                   DistributedMaxInodeSupported);
      std::cout << "We should init one new FS..." << std::endl;
      /**
       * If the filesystem on metadata server is not initialized, create
       * a root directory.
       */
      auto init_res = operation_->alloc_inode(InodeType::Directory);
      if (init_res.is_err())
      {
        std::cerr << "Cannot allocate inode for root directory." << std::endl;
        exit(1);
      }

      CHFS_ASSERT(init_res.unwrap() == 1, "Bad initialization on root dir.");
    }

    running = false;
    num_data_servers =
        0; // Default no data server. Need to call `reg_server` to add.

    if (is_log_enabled_)
    {
      if (may_failed_)
        operation_->block_manager_->set_may_fail(true);
      commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                               is_checkpoint_enabled_);
    }

    bind_handlers();

    /**
     * The metadata server wouldn't start immediately after construction.
     * It should be launched after all the data servers are registered.
     */
  }

  MetadataServer::MetadataServer(u16 port, const std::string &data_path,
                                 bool is_log_enabled, bool is_checkpoint_enabled,
                                 bool may_failed)
      : is_log_enabled_(is_log_enabled), may_failed_(may_failed),
        is_checkpoint_enabled_(is_checkpoint_enabled)
  {
    server_ = std::make_unique<RpcServer>(port);
    init_fs(data_path);
    if (is_log_enabled_)
    {
      commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                               is_checkpoint_enabled);
    }
  }

  MetadataServer::MetadataServer(std::string const &address, u16 port,
                                 const std::string &data_path,
                                 bool is_log_enabled, bool is_checkpoint_enabled,
                                 bool may_failed)
      : is_log_enabled_(is_log_enabled), may_failed_(may_failed),
        is_checkpoint_enabled_(is_checkpoint_enabled)
  {
    server_ = std::make_unique<RpcServer>(address, port);
    init_fs(data_path);
    if (is_log_enabled_)
    {
      commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                               is_checkpoint_enabled);
    }
  }

  // {Your code here}
  auto MetadataServer::mknode(u8 type, inode_id_t parent, const std::string &name)
      -> inode_id_t
  {
    // TODO: Implement this function.
    // UNIMPLEMENTED();
    inode_id_t inode_id = 0;

    InodeType inode_type = InodeType::Unknown;
    if (type == RegularFileType)
    {
      inode_type = InodeType::FILE;
    }
    else if (type == DirectoryType)
    {
      inode_type = InodeType::Directory;
    }

    // inode_id_t inode_id = operation_->alloc_inode(inode_type);
    meta_lock_all();

    txn_id_t txn_id;
    if (is_log_enabled_)
    {
        if (is_checkpoint_enabled_)
        {
            commit_log->checkpoint();
        }
      // log_mtx.lock();
      txn_id = commit_log->cur_txn_id++;
      commit_log->prepare();
    }

    auto res = operation_->mk_helper(parent, name.data(), inode_type);

    if (is_log_enabled_)
    {
      commit_log->append_log(txn_id, *(commit_log->bm_->log_entry_));
      commit_log->clear();
      // log_mtx.unlock();
      if (res.is_ok() && commit_log->commit_log(txn_id))
//      if (res.is_ok())
      {
        inode_id = res.unwrap();
      }
    }
    else
    {
      if (res.is_ok())
      {
        inode_id = res.unwrap();
      }
    }

    meta_unlock_all();

    return inode_id;
  }

  // {Your code here}
  auto MetadataServer::unlink(inode_id_t parent, const std::string &name)
      -> bool
  {
    // TODO: Implement this function.
    // UNIMPLEMENTED();
    meta_lock_all();
    auto inode_id = lookup(parent, name);
    if (inode_id == 0)
    {
      meta_unlock_all();
      return false;
    }
    else
    {
      std::vector<BlockInfo> blockinfo = get_block_map(inode_id);
      data_lock_all();
      for (auto itr = blockinfo.begin(); itr != blockinfo.end(); itr++)
      {
        auto [block_id, mac_id, version] = (*itr);
        auto pair = clients_.find(mac_id);

        auto cli_res = pair->second->call("free_block", block_id);
      }
      data_unlock_all();
    }

    txn_id_t txn_id;
    bool ret = false;
    if (is_log_enabled_)
    {
        if (is_checkpoint_enabled_)
        {
            commit_log->checkpoint();
        }
      // log_mtx.lock();
      txn_id = commit_log->cur_txn_id++;
      commit_log->prepare();
    }
    auto res = operation_->unlink(parent, name.data());
    if (is_log_enabled_)
    {
      commit_log->append_log(txn_id, *(commit_log->bm_->log_entry_));
      commit_log->clear();
      if (res.is_ok() && commit_log->commit_log(txn_id))
//      if (res.is_ok())
      {
        ret = true;
      }
      // log_mtx.unlock();
    }
    else
    {
      if (res.is_ok())
      {
        ret = true;
      }
    }

    meta_unlock_all();

    return ret;
  }

  // {Your code here}
  auto MetadataServer::lookup(inode_id_t parent, const std::string &name)
      -> inode_id_t
  {
    // TODO: Implement this function.
    // UNIMPLEMENTED();
    // meta_mtx[meta_lock_num(parent)].lock();
    auto res = operation_->lookup(parent, name.data());
    // meta_mtx[meta_lock_num(parent)].unlock();

    if (res.is_ok())
    {
      return res.unwrap();
    }

    return 0;
  }

  // {Your code here}
  auto MetadataServer::get_block_map(inode_id_t id) -> std::vector<BlockInfo>
  {
    // TODO: Implement this function.
    // UNIMPLEMENTED();
    std::vector<BlockInfo> block_info;

    // std::string src = operation_->read_file(id).unwrap();
    // meta_mtx[meta_lock_num(id)].lock();
    ChfsResult<std::vector<u8>> result = operation_->read_file(id);
    // meta_mtx[meta_lock_num(id)].unlock();
    if (result.is_err())
    {
      return block_info;
    }
    std::vector<u8> buffer = result.unwrap();

    // auto blockinfo_size = sizeof(block_id_t) + sizeof(mac_id_t) + sizeof(version_t);
    for (int i = 0; i < buffer.size(); i += sizeof(BlockInfo))
    {
      // block_id_t block_id = *(block_id_t *)(buffer.data() + i);
      // mac_id_t mac_id = *(block_id_t *)(buffer.data() + i + sizeof(block_id_t));
      // version_t version = *(block_id_t *)(buffer.data() + i + sizeof(block_id_t) + sizeof(mac_id_t));
      auto [block_id, mac_id, version] = *(BlockInfo *)(buffer.data() + i);

      block_info.emplace_back(block_id, mac_id, version);
    }

    return block_info;
  }

  // {Your code here}
  auto MetadataServer::allocate_block(inode_id_t id) -> BlockInfo
  {
    // TODO: Implement this function.
    // UNIMPLEMENTED();

    BlockInfo blockinfo;
    bool find_a_mac = false;

    for (auto itr = clients_.begin(); itr != clients_.end(); itr++)
    {
      mac_id_t mac_id = itr->first;
      std::shared_ptr<RpcClient> cli = itr->second;
      data_mtx[data_lock_num(mac_id)].lock();
      auto res = cli->call("alloc_block");
      data_mtx[data_lock_num(mac_id)].unlock();
      if (res.is_err())
      {
        continue;
      }
      else
      {
        auto [block_id, version] = res.unwrap()->as<std::pair<block_id_t, version_t>>();
        blockinfo = std::tuple(block_id, mac_id, version);
        find_a_mac = true;
        break;
      }
    }

    if (find_a_mac)
    {
      // auto [block_id, mac_id, version] = blockinfo;

      meta_mtx[meta_lock_num(id)].lock();
      // data_mtx[data_lock_num(mac_id)].lock();

      ChfsResult<std::vector<u8>> result = operation_->read_file(id);
      if (result.is_err())
      {
        meta_mtx[meta_lock_num(id)].unlock();
        return {0, 0, 0};
      }
      std::vector<u8> buffer = result.unwrap();

      std::vector<u8> content(sizeof(BlockInfo));
      // *((block_id_t *)(content.data())) = block_id;
      // *((mac_id_t *)(content.data() + sizeof(block_id_t))) = mac_id;
      // *((version_t *)(content.data() + sizeof(block_id_t) + sizeof(mac_id_t))) = version;
      *((BlockInfo *)(content.data())) = blockinfo;
      buffer.insert(buffer.end(), content.begin(), content.end());

      if (operation_->write_file(id, buffer).is_err())
      {
        meta_mtx[meta_lock_num(id)].unlock();
        return {0, 0, 0};
      }

      // data_mtx[data_lock_num(mac_id)].unlock();
      meta_mtx[meta_lock_num(id)].unlock();
    }

    return blockinfo;
  }

  // {Your code here}
  auto MetadataServer::free_block(inode_id_t id, block_id_t block_id,
                                  mac_id_t machine_id) -> bool
  {
    // TODO: Implement this function.
    // UNIMPLEMENTED();
    bool has_find_block = false;

    meta_mtx[meta_lock_num(id)].lock();
    std::vector<BlockInfo> block_info = this->get_block_map(id);
    std::vector<u8> buffer;

    for (auto itr = block_info.begin(); itr != block_info.end(); itr++)
    {
      // block_id_t bid = std::get<0>(*itr);
      // mac_id_t mac_id = std::get<1>(*itr);
      // version_t version = std::get<2>(*itr);
      auto [bid, mac_id, version] = (*itr);

      if (mac_id == machine_id && bid == block_id)
      {
        has_find_block = true;
        continue;
      }

      std::vector<u8> content(sizeof(BlockInfo));
      *(BlockInfo *)(content.data()) = (*itr);

      buffer.insert(buffer.end(), content.begin(), content.end());
    }

    ///////////////////
    if (has_find_block)
    {
      if (operation_->write_file(id, buffer).is_err())
      {
        meta_mtx[meta_lock_num(id)].unlock();
        return false;
      }
    }

    meta_mtx[meta_lock_num(id)].unlock();

    if (has_find_block)
    {
      auto itr = clients_.find(machine_id);
      std::shared_ptr<RpcClient> cli;
      if (itr != clients_.end())
      {
        auto [mac_id, cli] = (*itr);

        data_mtx[data_lock_num(mac_id)].lock();
        auto res = cli->call("free_block", block_id);
        data_mtx[data_lock_num(mac_id)].unlock();

        if (res.is_ok())
        {
          return true;
        }
      }
    }

    return false;
  }

  // {Your code here}
  auto MetadataServer::readdir(inode_id_t node)
      -> std::vector<std::pair<std::string, inode_id_t>>
  {
    // TODO: Implement this function.
    // UNIMPLEMENTED();
    std::vector<std::pair<std::string, inode_id_t>> vector;

    meta_mtx[meta_lock_num(node)].lock();
    ChfsResult<std::vector<u8>> result = operation_->read_file(node);
    meta_mtx[meta_lock_num(node)].unlock();

    if (result.is_err())
    {
      return vector;
    }

    std::vector<u8> buffer = result.unwrap();
    std::string src(buffer.begin(), buffer.end());

    std::list<DirectoryEntry> entry_list;
    parse_directory(src, entry_list);

    for (auto itr = entry_list.begin(); itr != entry_list.end(); itr++)
    {
      vector.emplace_back(itr->name, itr->id);
    }
    return vector;
  }

  // {Your code here}
  auto MetadataServer::get_type_attr(inode_id_t id)
      -> std::tuple<u64, u64, u64, u64, u8>
  {
    // TODO: Implement this function.
    // UNIMPLEMENTED();
    auto res = operation_->get_type_attr(id);
    if (res.is_err())
    {
      return {0, 0, 0, 0, 0};
    }
    auto [inode_type, attr] = res.unwrap();
    // FileAttr attr = operation_->inode_manager_->get_attr(id).unwrap();
    // InodeType inode_type = operation_->inode_manager_->get_type(id).unwrap();

    u8 type = static_cast<u8>(inode_type);

    if(inode_type==InodeType::FILE){
        auto infos=this->get_block_map(id);
        attr.size=infos.size()*DiskBlockSize;
    }

    std::tuple<u64, u64, u64, u64, u8> type_attr(attr.size, attr.atime, attr.mtime, attr.ctime, type);

    return type_attr;
  }

  auto MetadataServer::reg_server(const std::string &address, u16 port,
                                  bool reliable) -> bool
  {
    num_data_servers += 1;
    auto cli = std::make_shared<RpcClient>(address, port, reliable);
    clients_.insert(std::make_pair(num_data_servers, cli));

    return true;
  }

  auto MetadataServer::run() -> bool
  {
    if (running)
      return false;

    // Currently we only support async start
    server_->run(true, num_worker_threads);
    running = true;
    return true;
  }

} // namespace chfs