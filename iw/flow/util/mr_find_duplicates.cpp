#include "deluge/deluge.h"
#include "snap/google/glog/logging.h"

using namespace std;
using namespace deluge;


class Mapper : public HadoopPipes::Mapper {
public:
  Mapper(HadoopPipes::TaskContext& context) {
    JobInfo conf(context);
  }

  void map(HadoopPipes::MapContext& context) {
    context.emit(context.getInputKey(), "");
  }
};


class Reducer : public HadoopPipes::Reducer {
public:
  Reducer(HadoopPipes::TaskContext& context)  {
  }


  void reduce(HadoopPipes::ReduceContext& context) {
    int count = 0;
    while (context.nextValue()){
      ++count;
    }
    if (count > 1){
      context.emit(context.getInputKey(), StringPrintf("%d", count));
    }
  }

private:
};


int main(int argc, char *argv[]) {
  google::InstallFailureSignalHandler();
  ReaderWriterJob<Mapper, void, ModKeyPartitioner, Reducer> job;
  return HadoopPipes::runTask(job);
}




