#include "distributed/client.h"
#include "common/macros.h"
#include "common/util.h"
#include "distributed/metadata_server.h"

namespace chfs
{

  ChfsClient::ChfsClient() : num_data_servers(0) {}

  auto ChfsClient::reg_server(ServerType type, const std::string &address,
                              u16 port, bool reliable) -> ChfsNullResult
  {
    switch (type)
    {
    case ServerType::DATA_SERVER:
      num_data_servers += 1;
      data_servers_.insert({num_data_servers, std::make_shared<RpcClient>(
                                                  address, port, reliable)});
      break;
    case ServerType::METADATA_SERVER:
      metadata_server_ = std::make_shared<RpcClient>(address, port, reliable);
      break;
    default:
      std::cerr << "Unknown Type" << std::endl;
      exit(1);
    }

    return KNullOk;
  }

  // {Your code here}
  auto ChfsClient::mknode(FileType type, inode_id_t parent,
                          const std::string &name) -> ChfsResult<inode_id_t>
  {
    // TODO: Implement this function.
    // UNIMPLEMENTED();
    u8 type_u8=0;
    if (type == FileType::REGULAR)
    {
      type_u8 = RegularFileType;
    }
    else if (type == FileType::DIRECTORY)
    {
      type_u8 = DirectoryType;
    }

    auto res_metadata = metadata_server_->call("mknode", type_u8, parent, name);

    if (res_metadata.is_err())
    {
      return res_metadata.unwrap_error();
    }
    else
    {
      auto inode_id = res_metadata.unwrap()->as<inode_id_t>();
      if (inode_id == 0)
      {
        return ChfsResult<inode_id_t>(ErrorType::AlreadyExist);
      }
      else
      {
        return ChfsResult<inode_id_t>(inode_id);
      }
    }
  }

  // {Your code here}
  auto ChfsClient::unlink(inode_id_t parent, std::string const &name)
      -> ChfsNullResult
  {
    // TODO: Implement this function.
    // UNIMPLEMENTED();
    auto res_metadata = metadata_server_->call("unlink", parent, name);
    if (res_metadata.is_err())
    {
      return res_metadata.unwrap_error();
    }
    else
    {
      return KNullOk;
    }
  }

  // {Your code here}
  auto ChfsClient::lookup(inode_id_t parent, const std::string &name)
      -> ChfsResult<inode_id_t>
  {
    // TODO: Implement this function.
    // UNIMPLEMENTED();
    auto res_metadata = metadata_server_->call("lookup", parent, name);
    if (res_metadata.is_err())
    {
      return res_metadata.unwrap_error();
    }
    else
    {
      auto inode_id = res_metadata.unwrap()->as<inode_id_t>();
      return ChfsResult<inode_id_t>(inode_id);
    }
  }

  // {Your code here}
  auto ChfsClient::readdir(inode_id_t id)
      -> ChfsResult<std::vector<std::pair<std::string, inode_id_t>>>
  {
    // TODO: Implement this function.
    // UNIMPLEMENTED();
    auto res_metadata = metadata_server_->call("readdir", id);
    if (res_metadata.is_err())
    {
      return res_metadata.unwrap_error();
    }
    else
    {
      auto dir = res_metadata.unwrap()->as<std::vector<std::pair<std::string, inode_id_t>>>();
      return ChfsResult<std::vector<std::pair<std::string, inode_id_t>>>(dir);
    }
  }

  // {Your code here}
  auto ChfsClient::get_type_attr(inode_id_t id)
      -> ChfsResult<std::pair<InodeType, FileAttr>>
  {
    // TODO: Implement this function.
    // UNIMPLEMENTED();
    auto res_metadata = metadata_server_->call("get_type_attr", id);
    if (res_metadata.is_err())
    {
      return res_metadata.unwrap_error();
    }
    else
    {
        auto [size, atime, mtime, ctime, type] = res_metadata.unwrap()->as<std::tuple<u64, u64, u64, u64, u8>>();
//      std::cerr<<"client get_type_attr:"<<type<<std::endl;
        InodeType inode_type=InodeType::Unknown;
        if(type==DirectoryType){
            inode_type=InodeType::Directory;
        }else if(type==RegularFileType){
            inode_type=InodeType::FILE;
        }
        FileAttr attr;
        attr.size = size;
        attr.atime = atime;
        attr.mtime = mtime;
        attr.ctime = ctime;

        auto res=ChfsResult<std::pair<InodeType, FileAttr>>({inode_type, attr});
        return res;
    }
  }

  /**
   * Read and Write operations are more complicated.
   */
  // {Your code here}
  auto ChfsClient::read_file(inode_id_t id, usize offset, usize size)
      -> ChfsResult<std::vector<u8>>
  {
    // TODO: Implement this function.
    // UNIMPLEMENTED();
    auto res_metadata = metadata_server_->call("get_block_map", id);
    if (res_metadata.is_err())
    {
      return res_metadata.unwrap_error();
    }

    auto block_info = res_metadata.unwrap()->as<std::vector<BlockInfo>>();

    std::vector<u8> content;
    // auto file_size = block_info.size() * DiskBlockSize;
    auto block_info_idx = offset / DiskBlockSize;
    auto left_offset = offset % DiskBlockSize;

    while (size > 0)
    {
      auto read_size = std::min((size_t)(DiskBlockSize - left_offset), (size_t)size);
      auto [block_id, mac_id, version] = block_info[block_info_idx];
      auto res_data = data_servers_[mac_id]->call("read_data", block_id, left_offset, read_size, version);
      if (res_data.is_err())
      {
        return res_data.unwrap_error();
      }

      auto data = res_data.unwrap()->as<std::vector<u8>>();
      content.insert(content.end(), data.begin(), data.end());

      size -= read_size;
      block_info_idx++;
      left_offset = 0;
    }

    return ChfsResult<std::vector<u8>>(content);
  }

  // {Your code here}
  auto ChfsClient::write_file(inode_id_t id, usize offset, std::vector<u8> data)
      -> ChfsNullResult
  {
    // TODO: Implement this function.
    // UNIMPLEMENTED();
    auto res_metadata = metadata_server_->call("get_block_map", id);
    if (res_metadata.is_err())
    {
      return res_metadata.unwrap_error();
    }

    auto block_info = res_metadata.unwrap()->as<std::vector<BlockInfo>>();

    auto size = data.size();
    auto already_write = 0;
    auto block_info_idx = offset / DiskBlockSize;
    auto left_offset = offset % DiskBlockSize;

    while (size > 0)
    {
      auto write_size = std::min((size_t)(DiskBlockSize - left_offset), (size_t)size);
      if (block_info_idx < block_info.size())
      {
        // no need to alloc block
      }
      else
      {
        auto res_alloc = metadata_server_->call("alloc_block", id);
        if (res_alloc.is_err())
        {
          return res_alloc.unwrap_error();
        }

        block_info.push_back(res_alloc.unwrap()->as<BlockInfo>());
      }

      auto [block_id, mac_id, version] = block_info[block_info_idx];
      std::vector<u8> buffer;
      buffer.insert(buffer.end(), data.data() + already_write, data.data() + already_write + write_size);

      auto res_data = data_servers_[mac_id]->call("write_data", block_id, left_offset, buffer);
      if (res_data.is_err())
      {
        return res_data.unwrap_error();
      }

      size -= write_size;
      already_write += write_size;
      block_info_idx++;
      left_offset = 0;
    }

    return KNullOk;
  }

  // {Your code here}
  auto ChfsClient::free_file_block(inode_id_t id, block_id_t block_id,
                                   mac_id_t mac_id)
      -> ChfsNullResult
  {
    // TODO: Implement this function.
    // UNIMPLEMENTED();
    auto res = metadata_server_->call("free_block", id, block_id, mac_id);
    return KNullOk;
  }

} // namespace chfs