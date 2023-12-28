#include <algorithm>
#include <sstream>

#include "filesystem/directory_op.h"

namespace chfs
{

    /**
     * Some helper functions
     */
    auto string_to_inode_id(std::string &data) -> inode_id_t
    {
        std::stringstream ss(data);
        inode_id_t inode;
        ss >> inode;
        return inode;
    }

    auto inode_id_to_string(inode_id_t id) -> std::string
    {
        std::stringstream ss;
        ss << id;
        return ss.str();
    }

    // {Your code here}
    auto dir_list_to_string(const std::list<DirectoryEntry> &entries)
        -> std::string
    {
        std::ostringstream oss;
        usize cnt = 0;
        for (const auto &entry : entries)
        {
            oss << entry.name << ':' << entry.id;
            if (cnt < entries.size() - 1)
            {
                oss << '/';
            }
            cnt += 1;
        }
        return oss.str();
    }

    // {Your code here}
    auto append_to_directory(std::string src, std::string filename, inode_id_t id)
        -> std::string
    {

        // TODO: Implement this function.
        //       Append the new directory entry to `src`.
        std::string inode_string = inode_id_to_string(id);
        if (!src.empty())
        {
            src += "/";
        }
        src += filename + ":" + inode_string;
        //  UNIMPLEMENTED();

        return src;
    }

    // {Your code here}
    void parse_directory(std::string &src, std::list<DirectoryEntry> &list)
    {

        // TODO: Implement this function.
        list.clear();
        std::size_t pos = 0;
        while (true)
        {
            std::size_t colon_pos = src.find(':', pos);
            if (colon_pos == std::string::npos)
            {
                return;
            }
            std::string name = src.substr(pos, colon_pos - pos);
            pos = colon_pos + 1;

            std::size_t slash_pos = src.find('/', pos);
            std::string inode_id_str;
            if (slash_pos == std::string::npos)
            {
                inode_id_str = src.substr(pos);
            }
            else
            {
                inode_id_str = src.substr(pos, slash_pos - pos);
            }
            inode_id_t inode_id = std::stoull(inode_id_str);

            list.push_back((DirectoryEntry){std::move(name), inode_id});
            if (slash_pos == std::string::npos)
            {
                return;
            }

            pos = slash_pos + 1;
        }
    }

    // {Your code here}
    auto rm_from_directory(std::string src, std::string filename) -> std::string
    {

        auto res = std::string("");

        // TODO: Implement this function.
        //       Remove the directory entry from `src`.
        std::list<DirectoryEntry> list;
        parse_directory(src, list);
        for (auto itr = list.begin(); itr != list.end(); itr++)
        {
            if (filename != itr->name)
            {
                res = append_to_directory(res, itr->name, itr->id);
            }
        }
        //        UNIMPLEMENTED();

        return res;
    }

    /**
     * { Your implementation here }
     */
    auto read_directory(FileOperation *fs, inode_id_t id,
                        std::list<DirectoryEntry> &list) -> ChfsNullResult
    {

        // TODO: Implement this function.
        std::vector<u8> buffer = fs->read_file(id).unwrap();
        if (buffer.empty())
        {
            return KNullOk;
        }

        std::string src(buffer.begin(), buffer.end());
        parse_directory(src, list);
        //        UNIMPLEMENTED();

        return KNullOk;
    }

    // {Your code here}
    auto FileOperation::lookup(inode_id_t id, const char *name)
        -> ChfsResult<inode_id_t>
    {
        std::list<DirectoryEntry> list;

        // TODO: Implement this function.
        if (read_directory(this, id, list).is_err())
        {
            return ChfsResult<inode_id_t>(ErrorType::INVALID);
        }

        for (auto d : list)
        {
            if (d.name == name)
            {
                return ChfsResult<inode_id_t>(d.id);
            }
        }
        //        UNIMPLEMENTED();
        return ChfsResult<inode_id_t>(ErrorType::NotExist);
    }

    // {Your code here}
    auto FileOperation::mk_helper(inode_id_t id, const char *name, InodeType type)
        -> ChfsResult<inode_id_t>
    {

        // TODO:
        // 1. Check if `name` already exists in the parent.
        //    If already exist, return ErrorType::AlreadyExist.
        // 2. Create the new inode.
        // 3. Append the new entry to the parent directory.
        //        UNIMPLEMENTED();
//        std::cout<<"mk_helper:"<<static_cast<int>(type)<<std::endl;
        ChfsResult<inode_id_t> res = lookup(id, name);
        if (res.is_err() && res.unwrap_error() == ErrorType::NotExist)
        {
            std::list<DirectoryEntry> list;
            if (read_directory(this, id, list).is_err())
            {
                return ChfsResult<inode_id_t>(ErrorType::INVALID);
            }
            std::string src = dir_list_to_string(list);
            auto inode_res = alloc_inode(type);
            if (inode_res.is_err())
            {
                return ChfsResult<inode_id_t>(ErrorType::INVALID);
            }
            inode_id_t inode_id = inode_res.unwrap();
            src = append_to_directory(src, name, inode_id);

            std::vector<u8> content;
            for (auto itr = src.begin(); itr != src.end(); itr++)
            {
                content.push_back(static_cast<u8>(*itr));
            }
            content.push_back('\0');
            if (write_file(id, content).is_err())
            {
                return ChfsResult<inode_id_t>(ErrorType::INVALID);
            }
            return ChfsResult<inode_id_t>(inode_id);
        }
        else
        {
            return ChfsResult<inode_id_t>(ErrorType::AlreadyExist);
        }

        return ChfsResult<inode_id_t>(static_cast<inode_id_t>(0));
    }

    // {Your code here}
    auto FileOperation::unlink(inode_id_t parent, const char *name)
        -> ChfsNullResult
    {

        // TODO:
        // 1. Remove the file, you can use the function `remove_file`
        // 2. Remove the entry from the directory.
        auto res = lookup(parent, name);
        if (res.is_err())
        {
            return ChfsNullResult(ErrorType::NotExist);
        }
        else
        {
            // std::vector<u8> inode(DiskBlockSize);
            std::vector<u8> content;

            inode_id_t cur_inode_id = res.unwrap();
            if (remove_file(cur_inode_id).is_err())
            {
                return ChfsNullResult(ErrorType::INVALID);
            }

            std::list<DirectoryEntry> list;
            if (read_directory(this, parent, list).is_err())
            {
                return ChfsNullResult(ErrorType::INVALID);
            }
            std::string src = dir_list_to_string(list);
            std::string new_dir = rm_from_directory(src, name);
            content.insert(content.end(), new_dir.begin(), new_dir.end());
            if (this->write_file(parent, content).is_err())
            {
                return ChfsNullResult(ErrorType::INVALID);
            }
        }
        //        UNIMPLEMENTED();

        return KNullOk;
    }

} // namespace chfs
