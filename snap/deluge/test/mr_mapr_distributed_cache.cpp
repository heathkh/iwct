#include "snap/deluge/deluge.h"
#include "snap/google/glog/logging.h"
#include "snap/pert/stringtable.h"

using namespace deluge;

class Mapper : public HadoopPipes::Mapper {
public:

  Mapper(HadoopPipes::TaskContext& context) {
    JobInfo info(context);
    //read in a very large 2nd file from cache
    std::string secondary_input_uri = info.GetStringOrDie( "secondary_input_uri");

    MaprDistributedCache cache(info);

    CHECK(cache.IsUriCached(secondary_input_uri)) << "secondary_input_uri: " << secondary_input_uri;
    pert::StringTableReader reader;
    reader.Open(cache.GetCacheUriOrDie(secondary_input_uri));
    std::string key, value;
    while(reader.Next(&key, &value)){
      //LOG(INFO) << key << " " << value;
    }
  }

  void map(HadoopPipes::MapContext& context) {
    context.emit(context.getInputKey(), context.getInputValue());
  }
};


int main(int argc, char *argv[]) {
  google::InstallFailureSignalHandler();
  using namespace deluge;
  ReaderWriterJob<Mapper, void, ModKeyPartitioner, IdentityReducer> job;
  return HadoopPipes::runTask(job);
}
