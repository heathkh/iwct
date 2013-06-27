#include "snap/pert/core/fs_mapr.h"
#include "snap/pert/core/ufs.h"
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include "snap/google/glog/logging.h"
#include "snap/boost/regex.hpp"
#include "snap/boost/foreach.hpp"
#include "snap/boost/filesystem.hpp"
#include "snap/google/base/stringprintf.h"
#include "snap/boost/filesystem.hpp"
//#include "snap/progressbar/ezRateProgressBar.hpp"
#include <iostream>
#include <fstream>
namespace fs =  boost::filesystem;

#ifdef PERT_USE_MAPR_FS

#define DEFAULT_BUFFER_SIZE  1048576

namespace pert {

std::vector<std::string> MaprGetShardUris(std::string uri){
  CHECK(false) << "not yet implemented";
  std::vector<std::string> matching_uris;
  return matching_uris;
}



///////////////////////////////////////////////////////////////////////////////
// MaprInputCodedBlockFile
///////////////////////////////////////////////////////////////////////////////
MaprInputCodedBlockFile::MaprInputCodedBlockFile() {
  std::string host = "default";
  fs_ = hdfsConnect(host.c_str(), 0); // use default config file settings
  CHECK(fs_) << "error connecting to maprfs";
  copying_input_stream_.reset(new MaprCopyingInputStream(this));
}

MaprInputCodedBlockFile::~MaprInputCodedBlockFile() {
  CHECK_EQ(hdfsCloseFile(fs_, file_), 0);
  CHECK_EQ(hdfsDisconnect(fs_), 0);
}

bool MaprInputCodedBlockFile::Open(std::string uri) {
  CHECK(!is_open_) << "File already open.";
  if (!IsValidUri(uri, "maprfs")) {
    LOG(ERROR) << "failed to validate uri: " << uri;
    return false;
  }
  std::string scheme, path;
  CHECK(ParseUri(uri, &scheme, &path)) << "Invalid uri format: " << uri;

  file_ = hdfsOpenFile(fs_, path.c_str(), O_RDONLY, 0, 0, 0);
  if (file_ == NULL) {
    LOG(ERROR) << "Failed to open file: " << path;
    return false;
  }
  is_open_ = true;
  path_ = path;

  // Cache file size
  hdfsFileInfo* info = hdfsGetPathInfo(fs_, path_.c_str());
  CHECK(info);
  size_ = info->mSize;
  hdfsFreeFileInfo(info, 1);

  return true;
}

uint64 MaprInputCodedBlockFile::Size() {
  CHECK(is_open_);
  return size_;
}

// Caller takes ownership of returned object and must delete it when done
google::protobuf::io::CodedInputStream*
MaprInputCodedBlockFile::CreateCodedStream(uint64 position, uint64 length) {
  CHECK(is_open_);

  // Seek to requested position (relative to start of file).
  CHECK_LT(position, size_);
  CHECK_LE(position+length, size_);

  // Starting with MapR V2.1.1, sometimes seek fails and the hdfs connection
  // must be reset in order to try again.  Not sure why this transient error
  // happens with MapR V2.1.1 but not earlier versions.
  bool success = false;
  for (int i=0; i < 10; ++i){
   if (hdfsSeek(fs_, file_, position) == 0){
     success = true;
     break;
   }
    //LOG(INFO) << "seek attempt failed: " << i;
    //LOG(INFO) << "path:" << path_ << "\n position: " << position << "\n length: " << length << "\n size: " << size_;  // success if returns 0
    CHECK_EQ(hdfsCloseFile(fs_, file_), 0);
    CHECK_EQ(hdfsDisconnect(fs_), 0);
    std::string host = "default";
    fs_ = hdfsConnect(host.c_str(), 0); // use default config file settings
    CHECK(fs_) << "error connecting to maprfs";
    file_ = hdfsOpenFile(fs_, path_.c_str(), O_RDONLY, 0, 0, 0);
    CHECK(file_ != NULL);
    sleep(2*i);
  }
  CHECK(success);

  // Create a coded stream (hold it in a scoped ptr to manage deleting).
  limiting_stream_.reset(NULL); // the destructor references the copying_stream_, so must destroy it before destroying it
  copying_stream_.reset(new google::protobuf::io::CopyingInputStreamAdaptor(copying_input_stream_.get()));
  limiting_stream_.reset(new google::protobuf::io::LimitingInputStream(copying_stream_.get(), length));
  return new google::protobuf::io::CodedInputStream(limiting_stream_.get());
}




///////////////////////////////////////////////////////////////////////////////
// MaprCopyingInputStream
///////////////////////////////////////////////////////////////////////////////

MaprCopyingInputStream::MaprCopyingInputStream(MaprInputCodedBlockFile* input ) :
    input_(input) {
}

MaprCopyingInputStream::~MaprCopyingInputStream() {

}

int MaprCopyingInputStream::Read(void * buffer, int size){
  CHECK(input_->is_open_);
  return hdfsRead(input_->fs_, input_->file_, buffer, size);
}

int MaprCopyingInputStream::Skip(int count){
  CHECK(input_->is_open_);
  CHECK_GE(count, 0);
  tOffset start_pos = hdfsTell(input_->fs_, input_->file_);
  tOffset desired_pos = start_pos + count;

  //TODO(kheath): add optimization by avoiding extra "tell" if seek is successful
  hdfsSeek(input_->fs_, input_->file_, desired_pos);  // success if returns 0
  tOffset cur_pos = hdfsTell(input_->fs_, input_->file_);
  int actual_skipped = cur_pos - start_pos;
  return actual_skipped;
}




///////////////////////////////////////////////////////////////////////////////
// MaprOutputCodedBlockFile
///////////////////////////////////////////////////////////////////////////////

MaprOutputCodedBlockFile::MaprOutputCodedBlockFile() {
  fs_ = hdfsConnect("default", 0); // use default config file settings
  CHECK(fs_) << "error connecting to hdfs";
}

MaprOutputCodedBlockFile::~MaprOutputCodedBlockFile() {
  //LOG(INFO) << "MaprOutputCodedBlockFile::~MaprOutputCodedBlockFile()";
  // force destructors to be called that cause a write to happen before
  // releasing resources needed for a write
  output_stream_.reset(NULL);
  copying_output_stream_.reset(NULL);
  CHECK_EQ(hdfsFlush(fs_, file_), 0);
  //LOG(INFO) << "closing file: " << file_;
  CHECK_EQ(hdfsCloseFile(fs_, file_), 0);
  //LOG(INFO) << "disconnecting fs: " << fs_;
  CHECK_EQ(hdfsDisconnect(fs_), 0);
}

bool MaprOutputCodedBlockFile::Open(std::string uri, short replication, uint64 chunk_size) {
  CHECK_GE(replication, 0);
  CHECK_LE(replication, 6);
  CHECK_GE(chunk_size, 0);

  CHECK(!is_open_);
  if (!IsValidUri(uri, "maprfs")) {
    return false;
  }

  std::string scheme, path;
  CHECK(ParseUri(uri, &scheme, &path)) << "Invalid uri format: " << uri;

  path_ = path;

  // note a chunk_size of zero to hdfs means use hdfs's default... however, we want to use maprfs's default... which should be based on the settings of the parent directory... if it exists
  string parent_path = fs::path(path).remove_filename().string();;
  if (chunk_size == 0){
    string parent_uri = Uri(scheme, parent_path);
    while (!Exists(parent_uri)){
      parent_path = fs::path(parent_path).remove_filename().string();
      parent_uri = Uri(scheme, parent_path);
      LOG(INFO) << "parent_uri: " << parent_uri;
    }
    CHECK(ChunkSize(parent_uri, &chunk_size));
  }

  CHECK_EQ(chunk_size % (1 << 16), 0) << "MaprFS requires chunk size is a multiple of 2^16";
  CHECK_LE(chunk_size, 1024 * (1<<20)) << "hdfs.h uses a signed 32 int which artificially limits the chunk size to 1GB... maprfs can do more, but not through the c api... ;-(";

  file_ = hdfsOpenFile(fs_, path.c_str(), O_WRONLY, 0, replication, chunk_size);
  if (file_ == NULL){
    LOG(ERROR) << "Failed to open file: " << path;
    return false;
  }

  copying_output_stream_.reset(new MaprCopyingOutputStream(this));
  output_stream_.reset(new google::protobuf::io::CopyingOutputStreamAdaptor(copying_output_stream_.get()));

  is_open_ = true;
  uri_ = uri;
  return true;
}

// Caller takes ownership of returned object and must delete it when done
google::protobuf::io::CodedOutputStream*
MaprOutputCodedBlockFile::CreateCodedStream() {
  CHECK(output_stream_.get());
  return new google::protobuf::io::CodedOutputStream(output_stream_.get());
}


///////////////////////////////////////////////////////////////////////////////
// MaprCopyingOutputStream
///////////////////////////////////////////////////////////////////////////////

MaprCopyingOutputStream::MaprCopyingOutputStream(MaprOutputCodedBlockFile* output) :
  output_(output) {
}

MaprCopyingOutputStream::~MaprCopyingOutputStream() { }

/*
bool MaprCopyingOutputStream::Write(const void * buffer, int size){
  CHECK(output_->is_open_);
  if (hdfsWrite(output_->fs_, output_->file_, buffer, size) != size) {
    return false;
  }
  return true;
}
*/

bool MaprCopyingOutputStream::Write(const void * buffer, int size){
  CHECK(output_->is_open_);

  // Starting with MapR V2.1.1, sometimes write fails and the hdfs connection
  // must be reset in order to try again.  Not sure why this transient error
  // happens with MapR V2.1.1 but not earlier versions.
  //TODO(heathkh): find source of this bug and remove need for retry!
  bool success = false;
  for (int i=0; i < 10; ++i){
    int bytes_written = hdfsWrite(output_->fs_, output_->file_, buffer, size);
    if (bytes_written == size){
      success = true;
      break;
    }
    else if (bytes_written > 0){
      // if we wrote less than requested... something weird happened... signal error and give up.
      break;
    }

    // if we failed to write anything, there may be a transient error with maprfs... worth trying again...

    //LOG(INFO) << "seek attempt failed: " << i;
    //LOG(INFO) << "path:" << path_ << "\n position: " << position << "\n length: " << length << "\n size: " << size_;  // success if returns 0
    CHECK_EQ(hdfsCloseFile(output_->fs_, output_->file_), 0);
    CHECK_EQ(hdfsDisconnect(output_->fs_), 0);
    std::string host = "default";
    output_->fs_ = hdfsConnect(host.c_str(), 0); // use default config file settings
    CHECK(output_->fs_) << "error connecting to maprfs";
    output_->file_ = hdfsOpenFile(output_->fs_, output_->path_.c_str(), O_WRONLY, 0, 0, 0);
    CHECK(output_->file_ != NULL);
    sleep(2*i);
  }

  return success;
}




///////////////////////////////////////////////////////////////////////////////
// MaprFileSystem
///////////////////////////////////////////////////////////////////////////////


MaprFileSystem::MaprFileSystem() {

}

MaprFileSystem::~MaprFileSystem(){

}


bool MaprFileSystem::GetUriPath(const std::string& uri, std::string* path){
  CHECK(path);
  std::string scheme;
  if (!ParseUri(uri, &scheme, path)){
    return false;
  }
  if (scheme != "maprfs"){
    return false;
  }
  return true;
}


bool MaprFileSystem::Exists(const std::string& uri){
  std::string path = GetUriPathOrDie(uri);
  std::string host = "default";
  hdfsFS fs = hdfsConnect(host.c_str(), 0); // use default config file settings
  CHECK(fs) << "Can't connect to filesystem for this uri: " << uri;
  bool exists = (hdfsExists(fs, path.c_str()) == 0);
  CHECK_EQ(hdfsDisconnect(fs), 0);
  return exists;
}


bool MaprFileSystem::IsFile(const std::string& uri){
  std::string path = GetUriPathOrDie(uri);
  std::string host = "default";
  hdfsFS fs = hdfsConnect(host.c_str(), 0); // use default config file settings
  hdfsFileInfo* info = hdfsGetPathInfo(fs, path.c_str());

  bool is_file = false;
  if (info){
    is_file = (info->mKind == kObjectKindFile);
    hdfsFreeFileInfo(info,1);
  }
  else{
    LOG(FATAL) << "uri does not exist: " << uri;
  }
  CHECK_EQ(hdfsDisconnect(fs), 0);
  return is_file;
}


bool MaprFileSystem::IsDirectory(const std::string& uri){
  std::string path = GetUriPathOrDie(uri);
  std::string host = "default";
  hdfsFS fs = hdfsConnect(host.c_str(), 0); // use default config file settings
  CHECK(fs);
  hdfsFileInfo* info = hdfsGetPathInfo(fs, path.c_str());
  bool is_directory = false;
  if (info){
    is_directory = (info->mKind == kObjectKindDirectory);
    hdfsFreeFileInfo(info,1);
  }
  else{
    LOG(FATAL) << "uri does not exist: " << uri;
  }
  CHECK_EQ(hdfsDisconnect(fs), 0);
  return is_directory;
}


bool MaprFileSystem::MakeDirectory(const std::string& uri){
  std::string path = GetUriPathOrDie(uri);
  std::string host = "default";
  hdfsFS fs = hdfsConnect(host.c_str(), 0); // use default config file settings
  CHECK(fs);
  bool success = (hdfsCreateDirectory(fs, path.c_str()) == 0);
  CHECK_EQ(hdfsDisconnect(fs), 0);
  return success;
}

bool MaprFileSystem::ListDirectory(const std::string& uri, std::vector<std::string>* contents){
  CHECK(contents);
  contents->clear();
  std::string path = GetUriPathOrDie(uri);
  std::string host = "default";
  hdfsFS fs = hdfsConnect(host.c_str(), 0); // use default config file settings
  int num_entries;
  hdfsFileInfo* entries = hdfsListDirectory(fs, path.c_str(), &num_entries);
  hdfsFileInfo* cur_entry = entries;
  for (int i=0; i < num_entries; ++i) {
    // Sometimes the list directory command returns paths with the scheme and sometimes it doesn't
    // Strange.
    // Anyway, we need to consistently output uris with a proper scheme prefix.
    std::string cur_scheme, cur_path, error;
    if (ParseUri(cur_entry->mName, &cur_scheme, &cur_path, &error)){
      CHECK_EQ(cur_scheme, "maprfs"); // if it has a scheme prefix, make sure it is maprfs as expected
    }
    else{
      // this doesn't have a uri scheme prefix, so assume it is just the path portion
      cur_path = cur_entry->mName;
    }

    contents->push_back(Uri("maprfs", cur_path));

    cur_entry++;
  }
  hdfsFreeFileInfo(entries, num_entries);
  CHECK_EQ(hdfsDisconnect(fs), 0);
  return true;
}

bool MaprFileSystem::Remove(const std::string& uri){
  if (!Exists(uri)){
    VLOG(2) << "Can't remove file, it doesn't exist: " << uri;
    return false;
  }

  std::string path = GetUriPathOrDie(uri);
  std::string host = "default";
  hdfsFS fs = hdfsConnect(host.c_str(), 0); // use default config file settings
  CHECK(fs);
  if (IsFile(uri)){
    //LOG(INFO) << "removing file: " << uri;
    int retval = hdfsDelete(fs, path.c_str());
    CHECK_EQ(retval, 0);
  }
  else if (IsDirectory(uri)){
    //LOG(INFO) << "removing dir: " << uri;
    std::vector<std::string> dir_file_uris;
    CHECK(ListDirectory(uri, &dir_file_uris));
    BOOST_FOREACH(std::string file_uri, dir_file_uris){
      CHECK(Remove(file_uri));
    }
    int retval = hdfsDelete(fs, path.c_str());
    CHECK_EQ(retval, 0);
  }
  else{
    LOG(FATAL) << "unexpected condition";
  }

  CHECK_EQ(hdfsDisconnect(fs), 0);
  return true;
}


bool MaprFileSystem::FileSize(const std::string& uri, uint64* size){
  std::string path;
  CHECK(size);
  if (!GetUriPath(uri, &path)){
    return false;
  }
  std::string host = "default";
  hdfsFS fs = hdfsConnect(host.c_str(), 0); // use default config file settings
  if (!fs){
    LOG(WARNING) << "Can't get fs for uri: " << uri;
    return false;
  }
  if (hdfsExists(fs, path.c_str()) != 0){
    LOG(WARNING) << "Requested uri doesn't exist: " << uri;
    return false;
  }
  hdfsFileInfo* info = hdfsGetPathInfo(fs, path.c_str());
  if (info){
    *size = info->mSize;
    hdfsFreeFileInfo(info,1);
  }
  else{
    LOG(WARNING) << "uri doesn't exist: " << uri;
    return false;
  }
  CHECK_EQ(hdfsDisconnect(fs), 0);
  return true;
}



InputCodedBlockFile* MaprFileSystem::OpenInput(const std::string& uri){
  std::auto_ptr<MaprInputCodedBlockFile> file_ptr(new MaprInputCodedBlockFile());
  if (!file_ptr->Open(uri)){
    file_ptr.reset(NULL);
  }
  return file_ptr.release();
}

OutputCodedBlockFile* MaprFileSystem::OpenOutput(const std::string& uri, short replication, uint64 chunk_size){
  std::auto_ptr<MaprOutputCodedBlockFile> file_ptr(new MaprOutputCodedBlockFile());
  if (!file_ptr->Open(uri, replication, chunk_size)){
    file_ptr.reset(NULL);
  }
  return file_ptr.release();
}

bool MaprFileSystem::DownloadUri(const std::string& uri, const std::string& local_path){
  std::string path = GetUriPathOrDie(uri);
  std::string host = "default";
  hdfsFS fs = hdfsConnect(host.c_str(), 0); // use default config file settings

  // Get file info
  hdfsFileInfo* info = hdfsGetPathInfo(fs, path.c_str());
  CHECK(info) << "Can't get file info: " << path;
  uint64 size = info->mSize;
  uint64 src_last_write_time = info->mLastMod;
  bool is_file = info->mKind == kObjectKindFile;
  bool is_dir = info->mKind == kObjectKindDirectory;
  hdfsFreeFileInfo(info, 1);

  CHECK_NE(is_file, is_dir);
  if (is_file){
    //LOG(INFO) << "is file: " << uri;
    hdfsFile file = hdfsOpenFile(fs, path.c_str(), O_RDONLY, 0, 0, 0);
    if (file == NULL) {
      LOG(ERROR) << "Failed to open file: " << path;
      return false;
    }

    //std::string tmp_download_path = fs::unique_path().string();
    std::string tmp_download_path = local_path + ".download";
    std::ofstream outfile(tmp_download_path.c_str(), std::ios::binary);
    CHECK(outfile);

    // Read remote file to local file
    uint32 buf_size = 10240;
    char buffer[buf_size];
    uint64 remaining_bytes = size;
    uint64 read_size = buf_size;

    //ez::ezRateProgressBar<int> data_progress(size, "bytes");
    LOG(INFO) << "Downloading uri: " << uri;
    //data_progress.start();

    while (remaining_bytes > 0){
      if (remaining_bytes < read_size){
        read_size = remaining_bytes;
      }
      CHECK(hdfsRead(fs, file, buffer, read_size));
      outfile.write(buffer, read_size);
      remaining_bytes -= read_size;
      //data_progress += read_size;
    }

    outfile.close();

    // Create parent path structure for final file if it doesn't exist
    string parent_path = fs::path(local_path).remove_filename().string();
    fs::create_directories(parent_path);

    // mv tmp file to final dest
    fs::rename(tmp_download_path, local_path);

    // update the local modified timestamp to match the src
    fs::last_write_time(local_path, src_last_write_time);

    CHECK_EQ(hdfsCloseFile(fs, file), 0);
  }
  else if (is_dir){
    //LOG(INFO) << "is dir: " << uri;
    std::vector<std::string> dir_file_uris;
    CHECK(ListDirectory(uri, &dir_file_uris));

    BOOST_FOREACH(std::string file_uri, dir_file_uris){
      std::string file_path;
      CHECK(GetUriPath(file_uri, &file_path));

      std::string cur_local_file = local_path + "/" + fs::path(file_path).filename().string();
      DownloadUri(file_uri, cur_local_file);
    }

    // update the local modified timestamp to match the src (is this needed for dirs?)
    fs::last_write_time(local_path, src_last_write_time);
  }
  else{
    LOG(FATAL) << "unexpected condition";
  }

  CHECK_EQ(hdfsDisconnect(fs), 0);
  return true;
}

bool MaprFileSystem::CopyLocalToUri(const std::string& local_path, const std::string& destintation_uri){

  if (!fs::exists(local_path)) {
    return false;
  }

  std::string path = GetUriPathOrDie(destintation_uri);
  std::string host = "default";
  hdfsFS fs = hdfsConnect(host.c_str(), 0); // use default config file settings

  // Get file info

  uint64 src_last_write_time = fs::last_write_time(local_path);
  bool is_file = fs::is_regular_file(local_path);
  bool is_dir = fs::is_directory(local_path);

  CHECK_NE(is_file, is_dir);

  if (is_file){
    uint64 size = fs::file_size(local_path);

    // Create parent path structure for file if it doesn't exist
    string parent_path = fs::path(path).remove_filename().string();
    if (hdfsExists(fs, parent_path.c_str()) != 0){
      CHECK_EQ(hdfsCreateDirectory(fs, parent_path.c_str()), 0);
    }

    //LOG(INFO) << "is file: " << uri;
    hdfsFile file = hdfsOpenFile(fs, path.c_str(), O_WRONLY, 0, 0, 0);
    if (file == NULL) {
      LOG(ERROR) << "Failed to open file: " << path;
      return false;
    }

    std::ifstream infile(local_path.c_str(), std::ios::binary);
    CHECK(infile);

    // Read local file and copy to remote file
    uint32 buf_size = 10240;
    char buffer[buf_size];
    uint64 remaining_bytes = size;
    uint64 read_size = buf_size;
    while (remaining_bytes > 0){
      if (remaining_bytes < read_size){
        read_size = remaining_bytes;
      }
      infile.read(buffer, read_size);
      CHECK_EQ(hdfsWrite(fs, file, buffer, read_size), read_size);
      remaining_bytes -= read_size;
    }
    infile.close();

    // update the remote modified timestamp to match the src
    CHECK_EQ(hdfsUtime(fs, path.c_str(), src_last_write_time, 0), 0);
    CHECK_EQ(hdfsCloseFile(fs, file), 0);
  }
  else if (is_dir){
    //LOG(INFO) << "is dir: " << uri;
    std::vector<std::string> dir_paths;
    for (fs::directory_iterator iter(local_path), end; iter != end;  ++iter) {
      dir_paths.push_back(iter->path().string());
    }

    BOOST_FOREACH(std::string file_path, dir_paths){
      string cur_local_file = local_path + "/" + fs::path(file_path).filename().string();
      string dst_uri = destintation_uri + "/" + fs::path(file_path).filename().string();
      CopyLocalToUri(cur_local_file, dst_uri);
    }

    // update the local modified timestamp to match the src (is this needed for dirs?)
    CHECK_EQ(hdfsUtime(fs, path.c_str(), src_last_write_time, 0), 0);
  }
  else{
    LOG(FATAL) << "unexpected condition";
  }

  CHECK_EQ(hdfsDisconnect(fs), 0);
  return true;
}



bool MaprFileSystem::ModifiedTimestamp(const std::string& uri, uint64* timestamp) {
  std::string path;
  if (!GetUriPath(uri, &path)){
    return false;
  }
  std::string host = "default";
  hdfsFS fs = hdfsConnect(host.c_str(), 0); // use default config file settings
  if (!fs){
    LOG(WARNING) << "Can't get fs for uri: " << uri;
    return false;
  }
  if (hdfsExists(fs, path.c_str()) != 0){
    LOG(WARNING) << "Requested uri doesn't exist: " << uri;
    return false;
  }
  hdfsFileInfo* info = hdfsGetPathInfo(fs, path.c_str());
  if (info){
    *timestamp = info->mLastMod;
    hdfsFreeFileInfo(info,1);
  }
  else{
    LOG(WARNING) << "uri doesn't exist: " << uri;
    return false;
  }
  CHECK_EQ(hdfsDisconnect(fs), 0);
  return true;
}


bool MaprFileSystem::SetModifiedTimestamp(const std::string& uri, uint64 timestamp) {
  std::string path;
  if (!GetUriPath(uri, &path)){
    return false;
  }
  std::string host = "default";
  hdfsFS fs = hdfsConnect(host.c_str(), 0); // use default config file settings
  if (!fs){
    LOG(WARNING) << "Can't get fs for uri: " << uri;
    return false;
  }
  if (hdfsExists(fs, path.c_str()) != 0){
    LOG(WARNING) << "Requested uri doesn't exist: " << uri;
    return false;
  }
  CHECK_EQ(hdfsUtime(fs, path.c_str(), timestamp, 0), 0);
  CHECK_EQ(hdfsDisconnect(fs), 0);
  return true;
}



bool MaprFileSystem::ChunkSize(const std::string& uri, uint64* chunk_size_bytes){
  std::string path;
  CHECK(chunk_size_bytes);
  if (!GetUriPath(uri, &path)){
    return false;
  }
  std::string host = "default";
  hdfsFS fs = hdfsConnect(host.c_str(), 0); // use default config file settings
  if (!fs){
    LOG(WARNING) << "Can't get fs for uri: " << uri;
    return false;
  }
  if (hdfsExists(fs, path.c_str()) != 0){
    LOG(WARNING) << "Requested uri doesn't exist: " << uri;
    return false;
  }
  hdfsFileInfo* info = hdfsGetPathInfo(fs, path.c_str());
  if (info){
    *chunk_size_bytes = info->mBlockSize;
    hdfsFreeFileInfo(info,1);
  }
  else{
    LOG(WARNING) << "uri doesn't exist: " << uri;
    return false;
  }
  CHECK_EQ(hdfsDisconnect(fs), 0);
  return true;
}


//////////////////////////////////////////////////////////////////////////////


MaprInputFile::MaprInputFile() : is_open_(false), hdfs_buffer_size_(DEFAULT_BUFFER_SIZE) {
  std::string host = "default";
  fs_ = hdfsConnect(host.c_str(), 0); // use default config file settings
  CHECK(fs_) << "error connecting to maprfs";
}

MaprInputFile::~MaprInputFile(){
  CHECK_EQ(hdfsCloseFile(fs_, file_), 0);
  CHECK_EQ(hdfsDisconnect(fs_), 0);
}

bool MaprInputFile::Open(std::string uri){
  CHECK(!is_open_) << "File already open.";
  if (!IsValidUri(uri, "maprfs")) {
    LOG(ERROR) << "failed to validate uri: " << uri;
    return false;
  }
  std::string scheme, path;
  CHECK(ParseUri(uri, &scheme, &path)) << "Invalid uri format: " << uri;

  file_ = hdfsOpenFile(fs_, path.c_str(), O_RDONLY, hdfs_buffer_size_, 0, 0);
  if (file_ == NULL) {
    LOG(ERROR) << "Failed to open file: " << path;
    return false;
  }
  is_open_ = true;
  path_ = path;

  // Cache file size
  hdfsFileInfo* info = hdfsGetPathInfo(fs_, path_.c_str());
  CHECK(info);
  size_ = info->mSize;
  modification_time_ = info->mLastMod;
  hdfsFreeFileInfo(info, 1);
  return true;
}

uint64 MaprInputFile::Size(){
  CHECK(is_open_);
  return size_;
}

uint64 MaprInputFile::ModificationTime(){
  CHECK(is_open_);
  return modification_time_;
}

bool MaprInputFile::Read(uint32 buffer_size, char* buffer, uint32* bytes_read){
  CHECK_LE(buffer_size, hdfs_buffer_size_);
  CHECK(is_open_);
  int status = hdfsRead(fs_, file_, buffer, buffer_size);
  if (status < 0){
    return false;
  }
  *bytes_read = status;
  return true;
}


MaprOutputFile::MaprOutputFile() : is_open_(false), hdfs_buffer_size_(DEFAULT_BUFFER_SIZE) {
  std::string host("default");
  fs_ = hdfsConnect(host.c_str(), 0); // use default config file settings
  CHECK(fs_ != NULL) << "error connecting to maprfs";
}

MaprOutputFile::~MaprOutputFile(){
  if (is_open_){
    CHECK_EQ(hdfsFlush(fs_, file_), 0);
    CHECK_EQ(hdfsCloseFile(fs_, file_), 0);
    if (desired_mod_time_){
      CHECK_EQ(hdfsUtime(fs_, path_.c_str(), desired_mod_time_, 0), 0);
    }
  }
  CHECK_EQ(hdfsDisconnect(fs_), 0);
}

bool MaprOutputFile::Open(std::string uri, short replication, uint64 chunk_size, uint64 desired_mod_time){
  CHECK_GE(replication, 0);
  CHECK_LE(replication, 6);
  CHECK_GE(chunk_size, 0);

  CHECK(!is_open_);
  if (!IsValidUri(uri, "maprfs")) {
    return false;
  }

  std::string scheme, path;
  CHECK(ParseUri(uri, &scheme, &path)) << "Invalid uri format: " << uri;
  path_ = path;
  uri_ = uri;
  desired_mod_time_ = desired_mod_time;

  // note a chunk_size of zero to hdfs means use hdfs's default... however, we want to use maprfs's default... which should be based on the settings of the parent directory... if it exists
  string parent_path = fs::path(path).remove_filename().string();;
  if (chunk_size == 0){
    string parent_uri = Uri(scheme, parent_path);
    while (!Exists(parent_uri)){
      parent_path = fs::path(parent_path).remove_filename().string();
      parent_uri = Uri(scheme, parent_path);
      //LOG(INFO) << "parent_uri: " << parent_uri;
    }
    CHECK(ChunkSize(parent_uri, &chunk_size));
  }

  CHECK_EQ(chunk_size % (1 << 16), 0) << "MaprFS requires chunk size is a multiple of 2^16";
  CHECK_LE(chunk_size, 1024 * (1<<20)) << "hdfs.h uses a signed 32 int which artificially limits the chunk size to 1GB... maprfs can do more, but not through the c api... ;-(";

  file_ = hdfsOpenFile(fs_, path.c_str(), O_WRONLY, hdfs_buffer_size_, replication, chunk_size);
  if (file_ == NULL){
    LOG(ERROR) << "Failed to open file: " << path;
    return false;
  }

  is_open_ = true;
  uri_ = uri;
  return true;
}

bool MaprOutputFile::Write(uint32 buffer_size, const char* buffer){
  CHECK_LE(buffer_size, hdfs_buffer_size_);
  CHECK(is_open_);
  if (hdfsWrite(fs_, file_, buffer, buffer_size) != buffer_size) {
    return false;
  }
  return true;
}


//////////////////////

InputFile* MaprFileSystem::OpenInputFile(const std::string& uri){
  InputFile* new_file = new MaprInputFile();
  if (!new_file->Open(uri)){
    delete new_file;
    new_file = NULL;
  }
  return new_file;
}

OutputFile* MaprFileSystem::OpenOutputFile(const std::string& uri, short replication, uint64 chunk_size, uint64 desired_modification_time){
  OutputFile* new_file = new MaprOutputFile();
  if (!new_file->Open(uri, replication, chunk_size, desired_modification_time)){
    delete new_file;
    new_file = NULL;
  }
  return new_file;
}

} // close namespace pert

#endif
