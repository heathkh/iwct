#include "deluge/pertio.h"
#include "deluge/util.h"

#include "deluge/pipes/StringUtils.hh"
#include "snap/google/base/strtoint.h"

using namespace std;

namespace deluge {

PertRecordReader::PertRecordReader(HadoopPipes::MapContext& context) {

  //int partition_num = context.getJobConf()->getInt("mapred.task.partition");
  //std::string work_dir = context.getJobConf()->get("mapred.work.output.dir");
  //std::string output_dir = context.getJobConf()->get("mapred.output.dir");

  LOG(INFO) << "map.input.file: "  << context.getJobConf()->get("map.input.file");
  LOG(INFO) << "map.input.start: "  << context.getJobConf()->getInt("map.input.start");
  LOG(INFO) << "map.input.length: "  << context.getJobConf()->getInt("map.input.length");

  string input_uri = context.getJobConf()->get("map.input.file");
  input_uri = CleanupMaprPathFormat(input_uri);

  string split_start_string = context.getJobConf()->get("map.input.start");
  string split_length_string = context.getJobConf()->get("map.input.length");

  uint64 split_start = ParseLeadingUInt64Value(split_start_string, kuint64max);
  uint64 split_length = ParseLeadingUInt64Value(split_length_string, kuint64max);
  CHECK_NE(split_start, kuint64max);
  CHECK_NE(split_length, kuint64max);

  uint64 split_end = split_start+split_length;

  CHECK(reader_.OpenSplit(input_uri, split_start, split_end));

}

PertRecordReader::~PertRecordReader(){

}


void PertRecordReader::close() {

}

bool PertRecordReader::next(std::string& key, std::string& value){
  return reader_.Next(&key, &value);
}

float PertRecordReader::getProgress(){
  return reader_.SplitProgress();
}


} // close namespace deluge
