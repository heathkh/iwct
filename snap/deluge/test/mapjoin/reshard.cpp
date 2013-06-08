#include "deluge/deluge.h"

using namespace deluge;

int main(int argc, char *argv[]) {
  ReaderWriterJob<IdentityMapper, void, void, IdentityReducer> job;
  return HadoopPipes::runTask(job);
}



