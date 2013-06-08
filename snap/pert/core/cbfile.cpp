#include "snap/pert/core/cbfile.h"
#include "snap/pert/core/fs_local.h"
#include "snap/google/glog/logging.h"

#ifdef PERT_USE_MAPR_FS
#include "snap/pert/core/fs_mapr.h"
#endif

namespace pert {



////////////////////////////////////////////////////////////////////////////////
// InputFile
////////////////////////////////////////////////////////////////////////////////

bool InputFile::ReadToString(std::string* data){
  CHECK(data);
  data->clear();
  uint32 buffer_size = 1048576; //
  char* buffer = new char[buffer_size];
  uint64 num_bytes_src_file = Size();
  if (num_bytes_src_file >= 21474836480) {
    LOG(INFO) << "File too large to read into a string";
    return false;
  }
  uint64 num_bytes_copied = 0;
  while (num_bytes_copied < num_bytes_src_file){
    uint32 bytes_read = 0;
    CHECK(Read(buffer_size, buffer, &bytes_read));
    CHECK_LE(bytes_read, buffer_size);
    data->append(buffer, bytes_read);
    num_bytes_copied += bytes_read;
  }
  delete [] buffer;
  return true;
}


////////////////////////////////////////////////////////////////////////////////
// InputCodedBlockFile
////////////////////////////////////////////////////////////////////////////////

InputCodedBlockFile::InputCodedBlockFile() :
    is_open_(false)
{

}

InputCodedBlockFile::~InputCodedBlockFile() {
  is_open_ = false;
}

bool InputCodedBlockFile::OpenBlock(uint64 position, uint64 length) {
  CHECK(is_open_);
  coded_stream_.reset(CreateCodedStream(position, length));
  coded_stream_->SetTotalBytesLimit(512*(2<<20),-1); // a coded stream can deal with sizes up to 500MB (see doc in coded_stream.h)
  bool success = (coded_stream_.get() != NULL);
  return success;
}

bool InputCodedBlockFile::Read(std::string* out, int size) {
  CHECK(coded_stream_.get());
  return coded_stream_->ReadString(out, size);
}

bool InputCodedBlockFile::Read(google::protobuf::Message* proto) {
  CHECK(coded_stream_.get());
  return proto->ParseFromCodedStream(coded_stream_.get());
}

void InputCodedBlockFile::CloseBlock() {
  CHECK(is_open_);
  coded_stream_.reset();
}

////////////////////////////////////////////////////////////////////////////////
// OutputPacketFile
////////////////////////////////////////////////////////////////////////////////

OutputCodedBlockFile::OutputCodedBlockFile() :
    bytes_written_(0),
    is_open_(false){

}

OutputCodedBlockFile::~OutputCodedBlockFile() {
}

uint64 OutputCodedBlockFile::OpenBlock() {
  CHECK(is_open_);
  coded_stream_.reset(CreateCodedStream());
  CHECK(coded_stream_.get());
  return bytes_written_;
}

void OutputCodedBlockFile::Write(const std::string& out) {
  CHECK(coded_stream_.get());
  coded_stream_->WriteString(out);
}

void OutputCodedBlockFile::Write(uint64 out) {
  CHECK(coded_stream_.get());
  coded_stream_->WriteLittleEndian64(out);
}

void OutputCodedBlockFile::Write(const google::protobuf::Message& proto) {
  CHECK(coded_stream_.get());
  proto.SerializeToCodedStream(coded_stream_.get());
}

uint64 OutputCodedBlockFile::CloseBlock() {
  CHECK(is_open_);
  bytes_written_ += coded_stream_->ByteCount();
  coded_stream_.reset(NULL);
  return bytes_written_;
}

std::string OutputCodedBlockFile::GetUri(){
  return uri_;
}

} // close namespace

