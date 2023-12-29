#include <string>
#include <utility>
#include <vector>
#include <mutex>
#include "librpc/client.h"
#include "librpc/server.h"
#include "distributed/client.h"

//Lab4: Free to modify this file

namespace mapReduce {
    struct KeyVal {
        KeyVal(const std::string &key, const std::string &val) : key(key), val(val) {}
        KeyVal(){}
        std::string key;
        std::string val;
    };

    enum mr_tasktype {
        NONE = 0,
        MAP,
        REDUCE,
        MERGE
    };

    struct TaskInfo{
       TaskInfo(){
          this->tasktype=-1;
          this->n_reducer=0;
          this->filename="";
          this->nfiles=0;
       };
       TaskInfo(int tasktype,int nfiles,const std::string &filename, int n_reducer){
          this->tasktype=tasktype;
          this->nfiles=nfiles;
          this->filename=filename;
          this->n_reducer=n_reducer;
       };
       int tasktype;
       int nfiles;
       std::string filename;
       int n_reducer{};

        MSGPACK_DEFINE(tasktype, nfiles, filename, n_reducer)
    };

    std::vector<KeyVal> Map(const std::string &content);

    std::string Reduce(const std::string &key, const std::vector<std::string> &values);

    const std::string ASK_TASK = "ask_task";
    const std::string SUBMIT_TASK = "submit_task";

    struct MR_CoordinatorConfig {
        uint16_t port;
        std::string ip_address;
        std::string resultFile;
        std::shared_ptr<chfs::ChfsClient> client;

        MR_CoordinatorConfig(std::string ip_address, uint16_t port, std::shared_ptr<chfs::ChfsClient> client,
                             std::string resultFile) : port(port), ip_address(std::move(ip_address)),
                                                       resultFile(resultFile), client(std::move(client)) {}
    };

    class SequentialMapReduce {
    public:
        SequentialMapReduce(std::shared_ptr<chfs::ChfsClient> client, const std::vector<std::string> &files, std::string resultFile);
        void doWork();

    private:
        std::shared_ptr<chfs::ChfsClient> chfs_client;
        std::vector<std::string> files;
        std::string outPutFile;
    };

    class Coordinator {
    public:
        Coordinator(MR_CoordinatorConfig config, const std::vector<std::string> &files, int nReduce);
        TaskInfo askTask(int);
        int submitTask(int taskType, int index);
        bool Done();

    private:
        std::vector<std::string> files;
        std::mutex mtx;
        bool isFinished;
        std::unique_ptr<chfs::RpcServer> rpc_server;

        int n_reducer;
        int map_assign_num;
        int map_finish_num;
        int reduce_assign_num;
        int reduce_finish_num;
        bool map_finished;
        bool reduce_finished;
        bool merge_assigned;
        bool merge_finished;
        std::vector<bool> map_status;
        std::vector<bool> reduce_status;
    };

    class Worker {
    public:
        explicit Worker(MR_CoordinatorConfig config);
        void doWork();
        void stop();

    private:
        void doMap(int index, const std::string &filename);
        void doReduce(int index, int nfiles);
        void doSubmit(mr_tasktype taskType, int index);

        std::string outPutFile;
        std::unique_ptr<chfs::RpcClient> mr_client;
        std::shared_ptr<chfs::ChfsClient> chfs_client;
        std::unique_ptr<std::thread> work_thread;
        bool shouldStop = false;

        void merge();
        int n_reducer;
    };
}