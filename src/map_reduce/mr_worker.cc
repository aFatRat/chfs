#include <iostream>
#include <fstream>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <cstdlib>
#include <ctime>

#include <mutex>
#include <string>
#include <vector>
#include <unordered_map>
#include <thread>

#include "map_reduce/protocol.h"

namespace mapReduce {

    Worker::Worker(MR_CoordinatorConfig config) {
        mr_client = std::make_unique<chfs::RpcClient>(config.ip_address, config.port, true);
        outPutFile = config.resultFile;
        chfs_client = config.client;
        work_thread = std::make_unique<std::thread>(&Worker::doWork, this);
        // Lab4: Your code goes here (Optional).
        this->n_reducer=0;
    }

    void Worker::doMap(int index, const std::string &filename) {
        // Lab4: Your code goes here.

        // make a node to store map result
        std::string map_result_filename="map-"+std::to_string(index);
        auto mknode_res=chfs_client->mknode(chfs::ChfsClient::FileType::REGULAR,1,map_result_filename);
        if(mknode_res.is_err()){
          return;
        }

        // read file
        auto lookup_res=chfs_client->lookup(1,filename);
        if(lookup_res.is_err()){
          return;
        }
        auto inode_id=lookup_res.unwrap();
        auto [type,attr]=chfs_client->get_type_attr(inode_id).unwrap();
        auto content_vec=chfs_client->read_file(inode_id,0,attr.size).unwrap();
        std::string content(content_vec.begin(),content_vec.end());

        // map
        std::vector<KeyVal> map_result= Map(content);

        // write to file
        auto map_result_inode_id=mknode_res.unwrap();
        std::string map_result_str;
        for(auto [key,value]:map_result){
          std::string line=key+' '+value+'\n';
          map_result_str+=line;
        }
        std::vector<chfs::u8> buf_vec(map_result_str.begin(),map_result_str.end());
        chfs_client->write_file(map_result_inode_id,0,buf_vec);

        // submit
        doSubmit(MAP,index);
    }

    void Worker::doReduce(int index, int nfiles) {
        // Lab4: Your code goes here.

//        // make a node to store reduce result
//        std::string reduce_result_filename="reduce-"+std::to_string(index);
//        auto mknode_res=chfs_client->mknode(chfs::ChfsClient::FileType::REGULAR,1,reduce_result_filename);
//        if(mknode_res.is_err()){
//            return;
//        }
//
//        // create value map
//        std::map<std::string,std::vector<std::string>> reduce_map;
//        int start=index*nfiles;
//        int end=(index+1)*nfiles;
//        for(int i=start;i<end;i++){
//            std::string map_result_filename="map-"+std::to_string(i);
//            auto inode_id_res=chfs_client->lookup(1,map_result_filename);
//            if(inode_id_res.is_err()){
//                std::cout<<"Read error!"<<std::endl;
//                break;
//            }
//            auto inode_id=inode_id_res.unwrap();
//            if(inode_id==0){
//                break;
//            }
//
//            auto [type,attr]=chfs_client->get_type_attr(inode_id).unwrap();
//            auto content_vec=chfs_client->read_file(inode_id,0,attr.size).unwrap();
//            std::string content(content_vec.begin(),content_vec.end());
//
//            std::stringstream ss(content);
//            std::string word;
//            std::string quantity;
//            while(ss>>word>>quantity){
//                reduce_map[word].push_back(quantity);
//            }
//        }
//
//        // reduce
//        std::map<std::string,int> reduce_result;
//        for(auto [word,quantity_vec]:reduce_map){
//            std::string quantity_str= Reduce(word,quantity_vec);
//            int quantity=std::stoi(quantity_str);
////            int quantity=0;
//            reduce_result[word]=quantity;
//        }
//
//        // write to file
//        auto reduce_result_inode_id=mknode_res.unwrap();
//        std::string reduce_result_str;
//        for(auto [key,value]:reduce_result){
//            std::string line=key+' '+std::to_string(value)+'\n';
//            reduce_result_str+=line;
//        }
//        std::vector<chfs::u8> buf_vec(reduce_result_str.begin(),reduce_result_str.end());
//        chfs_client->write_file(reduce_result_inode_id,0,buf_vec);
//
//        // submit
//        doSubmit(REDUCE,index);

        // make a node to store reduce result
        std::string reduce_result_filename="reduce-"+std::to_string(index);
        auto mknode_res=chfs_client->mknode(chfs::ChfsClient::FileType::REGULAR,1,reduce_result_filename);
        if(mknode_res.is_err()){
            return;
        }

        // create value map
        std::map<std::string,std::vector<std::string>> reduce_map;
        if(this->n_reducer<=0){
          return;
        }
        int char_per_reducer=26/this->n_reducer;
        int char_left=26-char_per_reducer*this->n_reducer;
        int start_index=index*char_per_reducer+std::min(index,char_left);
        int end_index=(index+1)*char_per_reducer+std::min((index+1),char_left);

        std::srand(std::time(nullptr));
        int random=std::rand();
        for(int i=0;i<nfiles;i++){
            int read_index=(i+random)%6;

            std::string map_result_filename="map-"+std::to_string(read_index);
            auto inode_id_res=chfs_client->lookup(1,map_result_filename);
            if(inode_id_res.is_err()){
                std::cout<<"Read error!"<<std::endl;
                break;
            }
            auto inode_id=inode_id_res.unwrap();
            if(inode_id==0){
                break;
            }

            auto [type,attr]=chfs_client->get_type_attr(inode_id).unwrap();
            auto content_vec=chfs_client->read_file(inode_id,0,attr.size).unwrap();
            std::string content(content_vec.begin(),content_vec.end());

            std::stringstream ss(content);
            std::string word;
            std::string quantity;
            while(ss>>word>>quantity){
                char c=word[0];
                if((c>='a'+start_index&&c<'a'+end_index)||(c>='A'+start_index&&c<'A'+end_index)){
                  reduce_map[word].push_back(quantity);
                }
            }
        }

        // reduce
        std::map<std::string,int> reduce_result;
        for(auto [word,quantity_vec]:reduce_map){
            std::string quantity_str= Reduce(word,quantity_vec);
            int quantity=std::stoi(quantity_str);
            reduce_result[word]=quantity;
        }

        // write to file
        auto reduce_result_inode_id=mknode_res.unwrap();
        std::string reduce_result_str;
        for(auto [key,value]:reduce_result){
            std::string line=key+' '+std::to_string(value)+'\n';
            reduce_result_str+=line;
        }
        std::vector<chfs::u8> buf_vec(reduce_result_str.begin(),reduce_result_str.end());
        chfs_client->write_file(reduce_result_inode_id,0,buf_vec);

        // submit
        doSubmit(REDUCE,index);
    }

    void Worker::doSubmit(mr_tasktype taskType, int index) {
        // Lab4: Your code goes here.
        mr_client->call("submit_task",static_cast<int>(taskType),index);
    }

    void Worker::merge(){
        if(this->n_reducer==0){
            return;
        }

        auto output_file_inode_id=chfs_client->lookup(1,outPutFile).unwrap();
//        std::cout<<"OUTPUT_INODE_ID: "+std::to_string(output_file_inode_id)+"\n";
        std::vector<chfs::u8> buf;
        for(int i=0;i<n_reducer;i++){
            std::string reduce_result_filename="reduce-"+std::to_string(i);
//            std::cout<<"MERGE READ: "+reduce_result_filename+"\n";
            auto inode_id=chfs_client->lookup(1,reduce_result_filename).unwrap();
            auto [type,attr]=chfs_client->get_type_attr(inode_id).unwrap();
            auto content_vec=chfs_client->read_file(inode_id,0,attr.size).unwrap();
            buf.insert(buf.end(),content_vec.begin(),content_vec.end());
        }

        chfs_client->write_file(output_file_inode_id,0,buf);

        doSubmit(MERGE,0);
    }

    void Worker::stop() {
        shouldStop = true;
        work_thread->join();
    }

    void Worker::doWork() {
        while (!shouldStop) {
            // Lab4: Your code goes here.
            auto taskInfo=mr_client->call("ask_task",0).unwrap()->as<TaskInfo>();
            if(taskInfo.n_reducer!=0){
              this->n_reducer=taskInfo.n_reducer;
            }

            if(taskInfo.tasktype==0){
              doMap(taskInfo.nfiles,taskInfo.filename);
              continue;
            }else if(taskInfo.tasktype>0){
              doReduce(taskInfo.tasktype-1,taskInfo.nfiles);
              continue;
            }else if(taskInfo.tasktype==-2) {
              merge();
              continue;
            }
            else{
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
                continue;
            }
        }
    }
}