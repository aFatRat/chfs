#include <string>
#include <utility>
#include <vector>
#include <algorithm>

#include "map_reduce/protocol.h"

namespace mapReduce {
    SequentialMapReduce::SequentialMapReduce(std::shared_ptr<chfs::ChfsClient> client,
                                             const std::vector<std::string> &files_, std::string resultFile) {
        chfs_client = std::move(client);
        files = files_;
        outPutFile = resultFile;
        // Your code goes here (optional)
    }

    void SequentialMapReduce::doWork() {
        // Your code goes here
        std::map<std::string,int> map_result;
        for(const auto& file:files){
          auto inode_id=chfs_client->lookup(1,file).unwrap();
          auto [type,attr]=chfs_client->get_type_attr(inode_id).unwrap();
          auto content_vec=chfs_client->read_file(inode_id,0,attr.size).unwrap();
          std::string content(content_vec.begin(),content_vec.end());

          auto single_map_result=Map(content);
          for(auto [key,value]:single_map_result){
            map_result[key]+=std::stoi(value);
          }
        }

        std::vector<chfs::u8> buf;
        for(auto [word,quantity]:map_result){
          std::string line=word+' '+std::to_string(quantity)+'\n';
          buf.insert(buf.end(),line.begin(),line.end());
        }

        auto output_inode_id=chfs_client->lookup(1,outPutFile).unwrap();
        chfs_client->write_file(output_inode_id,0,buf);
    }
}