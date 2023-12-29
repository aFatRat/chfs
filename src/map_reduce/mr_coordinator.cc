#include <string>
#include <vector>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <mutex>

#include "map_reduce/protocol.h"

namespace mapReduce {
    // (task type, nfiles, filename, n_reducer)
    // -2:merge -1:sleep 0:map >=1:reduce number
    TaskInfo Coordinator::askTask(int) {
        // Lab4 : Your code goes here.
        // Free to change the type of return value.
//        std::cout<<"MAP__"+std::to_string(files.size())+"__"+std::to_string(map_assign_num)+"\n";
//        std::cout<<"REDUCE__"+std::to_string(n_reducer)+"__"+std::to_string(reduce_assign_num)+"\n";

        TaskInfo taskInfo;
        taskInfo.n_reducer=this->n_reducer;

        if(!map_finished){
            if(map_assign_num>=files.size()){
                taskInfo.tasktype=-1;
            }else{
                int index=map_assign_num++;
                taskInfo.tasktype=0;
                taskInfo.nfiles=index;
                taskInfo.filename=files[index];
            }
        }else if(!reduce_finished){
            if(reduce_assign_num>=n_reducer){
                taskInfo.tasktype=-1;
            }else{
                int index=reduce_assign_num++;
                taskInfo.tasktype=index+1;
                taskInfo.nfiles=(int)files.size();
            }
        }else if(!merge_assigned){
            merge_assigned= true;
            taskInfo.tasktype=-2;
        }

        return taskInfo;
    }

    int Coordinator::submitTask(int taskType, int index) {
        // Lab4 : Your code goes here.
        if(taskType==MAP){
            if(!map_status[index]){
//                std::cout<<"MAP "+std::to_string(index)+" FINISH\n";
                map_status[index]=true;
                map_finish_num++;
            }

            if(map_finish_num>=files.size()){
//                std::cout<<"MAP ALL FINISH\n";
                map_finished= true;
            }
        }else if(taskType==REDUCE){
            if(!reduce_status[index]){
//                std::cout<<"REDUCE "+std::to_string(index)+" FINISH\n";
                reduce_status[index]=true;
                reduce_finish_num++;
            }

            if(reduce_finish_num>=n_reducer){
//                std::cout<<"REDUCE ALL FINISH\n";
                reduce_finished= true;
            }
        }else if(taskType==MERGE){
//            std::cout<<"MERGE FINISH\n";
            isFinished= true;
        }
        return 0;
    }

    // mr_coordinator calls Done() periodically to find out
    // if the entire job has finished.
    bool Coordinator::Done() {
        std::unique_lock<std::mutex> uniqueLock(this->mtx);
        return this->isFinished;
    }

//    int calculate_reduce_num(int files,int n_reduce){
//        double log= std::log(files);
//        int log_int=(int)log;
//        return std::min(log_int,n_reduce);
//    }

    // create a Coordinator.
    // nReduce is the number of reduce tasks to use.
    Coordinator::Coordinator(MR_CoordinatorConfig config, const std::vector<std::string> &files, int nReduce) {
        this->files = files;
//        std::cout<<"FILES NUM: "+std::to_string(files.size())+"\n";
        this->isFinished = false;
        // Lab4: Your code goes here (Optional).
//        this->n_reducer=1;
        this->n_reducer= nReduce;
        this->map_assign_num=0;
        this->map_finish_num=0;
        this->reduce_assign_num=0;
        this->reduce_finish_num=0;
        this->map_finished=false;
        this->reduce_finished=false;
        this->merge_assigned= false;
        this->merge_finished= false;
        this->map_status=std::vector<bool>(files.size());
        this->reduce_status=std::vector<bool>(nReduce);
    
        rpc_server = std::make_unique<chfs::RpcServer>(config.ip_address, config.port);
        rpc_server->bind(ASK_TASK, [this](int i) { return this->askTask(i); });
        rpc_server->bind(SUBMIT_TASK, [this](int taskType, int index) { return this->submitTask(taskType, index); });
        rpc_server->run(true, 1);
    }
}