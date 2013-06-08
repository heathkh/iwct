#include "deluge/deluge.h"

using namespace std;
using namespace deluge;

class Mapper : public JoinMapper {
public:
  Mapper(HadoopPipes::TaskContext& context) : JoinMapper(context) {
  }

  virtual void map(const JoinTuple& join_tuple, HadoopPipes::MapContext& context) {
    string v1, v2;
    CHECK(join_tuple.getTupleValue(0, &v1));
    CHECK(join_tuple.getTupleValue(1, &v2));
    context.emit(join_tuple.getKey(), v1 + " " + v2);
  }
};


int main(int argc, char *argv[]) {
  ReaderWriterJob<Mapper, void, void, IdentityReducer> job;
  return HadoopPipes::runTask(job);
}



