#pragma once

#include "deluge/job.h"
#include "snap/pert/stringtable.h"
#include "deluge/pipes/Pipes.hh"
#include "deluge/pipes/TemplateFactory.hh"
#include "snap/pert/pert.h"
#include "deluge/util.h"
#include "snap/google/base/stringprintf.h"

namespace deluge {

/**
 * Class to read a SimpleRecordFile from a StringTable file.
 */
class PertRecordReader : public HadoopPipes::RecordReader {
public:
	PertRecordReader(HadoopPipes::MapContext& context);
  virtual ~PertRecordReader();

  virtual void close();
  virtual bool next(std::string& key, std::string& value);
  virtual float getProgress();

private:
  pert::StringTableShardReader reader_;
};


template <typename Writer>
class PertRecordWriter : public HadoopPipes::RecordWriter {
public:

	PertRecordWriter(HadoopPipes::ReduceContext& context) {
    JobInfo info(context);
    int partition_num = info.GetIntOrDie("mapred.task.partition");
    std::string work_dir = CleanupMaprPathFormat(info.GetStringOrDie("mapred.work.output.dir"));
    std::string output_dir = info.GetStringOrDie("mapred.output.dir");
    std::string output_path = StringPrintf("%s/part-%05d", work_dir.c_str(), partition_num);
    std::string comparator_name = info.GetStringOrDie("pert.recordwriter.comparator_name");
    std::string compression_codec_name = info.GetStringOrDie("pert.recordwriter.compression_codec_name");
    pert::WriterOptions options;
    options.SetSorted(comparator_name);
    options.SetCompressed(compression_codec_name);
    CHECK(writer_.Open(output_path, options));
  }

  ~PertRecordWriter() {
    close();
    //LOG(INFO) << "RecordWriter::~RecordWriter()";
  }

  void close() {
    writer_.Close();
    //LOG(INFO) << "RecordWriter::close()";
  }

  void emit(const std::string& key, const std::string& value){
    CHECK(writer_.Add(key, value));
    //LOG(INFO) << "RecordWriter::emit() - key: " << key;
  }

private:
  Writer writer_;
};

typedef PertRecordWriter<pert::StringTableShardWriter> StringTableRecordWriter;


template <class Mapper, class Combiner, class Partitioner, class Reducer>
class ReaderWriterJob : public HadoopPipes::TemplateFactory<Mapper,   // map
                              Reducer, // reduce
                              Partitioner,   // partition
                              Combiner,   // combiner
                              PertRecordReader,        // reader
                              StringTableRecordWriter > {

};


template <class Mapper, class Combiner, class Partitioner, class Reducer, class Proto>
class ReaderProtoWriterJob : public HadoopPipes::TemplateFactory<Mapper,   // map
                              Reducer, // reduce
                              Partitioner,   // partition
                              Combiner,   // combiner
                              PertRecordReader,        // reader
                              PertRecordWriter< pert::ProtoTableShardWriter<Proto> > > {

};



} // close namespace deluge
