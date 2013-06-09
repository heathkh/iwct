#include "snap/pert/core/ufs.h"
#include "snap/pert/core/fs_local.h"
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include "snap/google/glog/logging.h"
#include <sys/stat.h>
#include <fcntl.h>
#include <string>
#include <stdio.h>
#include <errno.h>

#include "snap/boost/filesystem.hpp"
//#include "snap/boost/regex.hpp"
#include "snap/boost/foreach.hpp"
#include "snap/google/base/stringprintf.h"

namespace fs =  boost::filesystem;

namespace pert {

///////////////////////////////////////////////////////////////////////////////
// LocalInputFile
///////////////////////////////////////////////////////////////////////////////
LocalInputFile::LocalInputFile() : is_open_(false)  {

}

LocalInputFile::~LocalInputFile(){

}

bool LocalInputFile::Open(std::string uri) {
  CHECK(!is_open_) << "File already open.";
  if (!IsValidUri(uri, "local")) {
    LOG(ERROR) << "failed to validate uri: " << uri;
    return false;
  }
  std::string scheme, path;
  ParseUri(uri, &scheme, &path);

  fd_ = open(path.c_str(), O_RDONLY);
  if (fd_ < 0) {
    LOG(ERROR) << "Failed to open file: " << path;
    LOG(ERROR) << strerror(errno);
    return false;
  }

  // Check file size
  struct stat st;
  CHECK_EQ(stat(path.c_str(), &st), 0) << "Cannot determine size of "
      << path << " " << strerror(errno);
  size_ = st.st_size;
  modification_time_ = fs::last_write_time(path);
  is_open_ = true;
  return true;
}

uint64 LocalInputFile::Size(){
  CHECK(is_open_);
  return size_;
}

uint64 LocalInputFile::ModificationTime(){
  CHECK(is_open_);
  return modification_time_;
}

bool LocalInputFile::Read(uint32 buffer_size, char* buffer, uint32* bytes_read){
  CHECK(is_open_);
  size_t num_bytes_read = read(fd_, buffer, buffer_size);
  *bytes_read = num_bytes_read;
  return true;
}

///////////////////////////////////////////////////////////////////////////////
// LocalOutputFile
///////////////////////////////////////////////////////////////////////////////

LocalOutputFile::LocalOutputFile() : is_open_(false) {

}

LocalOutputFile::~LocalOutputFile(){
  if (is_open_){
    CHECK_EQ(close(fd_), 0);
  }
}

bool LocalOutputFile::Open(std::string uri, short replication, uint64 chunk_size, uint64 desired_mod_time){
  CHECK_EQ(replication, 0);
  CHECK_EQ(chunk_size, 0);
  CHECK_EQ(desired_mod_time, 0) << "setting mod time for local file is not yet implemented";
  CHECK(!is_open_);
  if (!IsValidUri(uri, "local")) {
    return false;
  }

  std::string scheme, path;
  CHECK(ParseUri(uri, &scheme, &path)) << "Invalid uri format: " << uri;
  path_ = path;
  uri_ = uri;
  desired_mod_time_ = desired_mod_time;

  // create parent directories if they don't yet exist
  string parent_path = fs::path(path).remove_filename().string();
  if (!fs::exists(parent_path)){
    CHECK(fs::create_directories(parent_path));
  }

  fd_ = open(path.c_str(), O_WRONLY | O_TRUNC | O_CREAT,
      S_IRWXU | S_IRWXG | S_IRWXO);
  if (fd_ < 0) {
    LOG(ERROR) << "Failed to open file: " << path;
    LOG(ERROR) << strerror(errno);
    return false;
  }

  is_open_ = true;
  uri_ = uri;
  return true;
}

bool LocalOutputFile::Write(uint32 buffer_size, const char* buffer){
  CHECK(is_open_);
  if (write(fd_, buffer, buffer_size) != buffer_size) {
    return false;
  }
  return true;
}


///////////////////////////////////////////////////////////////////////////////
// LocalInputCodedBlockFile
///////////////////////////////////////////////////////////////////////////////
LocalInputCodedBlockFile::LocalInputCodedBlockFile() {

}

LocalInputCodedBlockFile::~LocalInputCodedBlockFile() {

}

bool LocalInputCodedBlockFile::Open(std::string uri) {
  CHECK(!is_open_) << "File already open.";
  if (!IsValidUri(uri, "local")) {
    LOG(ERROR) << "failed to validate uri: " << uri;
    return false;
  }
  std::string scheme, path;
  ParseUri(uri, &scheme, &path);

  fd_ = open(path.c_str(), O_RDONLY);
  if (fd_ < 0) {
    LOG(ERROR) << "Failed to open file: " << path;
    LOG(ERROR) << strerror(errno);
    return false;
  }

  // Check file size
  struct stat st;
  CHECK_EQ(stat(path.c_str(), &st), 0) << "Cannot determine size of "
      << path << " " << strerror(errno);
  size_ = st.st_size;

  is_open_ = true;

  return true;
}

uint64 LocalInputCodedBlockFile::Size() {
  CHECK(is_open_);
  return size_;
}

google::protobuf::io::CodedInputStream*
LocalInputCodedBlockFile::CreateCodedStream(uint64 position, uint64 length) {
  CHECK(is_open_);

  // Seek to requested position (relative to start of file).
  off_t new_pos = lseek(fd_, position, SEEK_SET);
  CHECK_EQ(new_pos, position);

  // Create a coded stream
  limiting_stream_.reset(NULL); // the destructor references the file_stream_, so must destroy it before destroying file_stream_
  file_stream_.reset(new google::protobuf::io::FileInputStream(fd_));
  limiting_stream_.reset(
      new google::protobuf::io::LimitingInputStream(file_stream_.get(),
          length));
  return new google::protobuf::io::CodedInputStream(limiting_stream_.get());
}

///////////////////////////////////////////////////////////////////////////////
// LocalOutputCodedBlockFile
///////////////////////////////////////////////////////////////////////////////

LocalOutputCodedBlockFile::LocalOutputCodedBlockFile() {

}

LocalOutputCodedBlockFile::~LocalOutputCodedBlockFile() {

}

bool LocalOutputCodedBlockFile::Open(std::string uri, short replication, uint64 chunk_size) {
  CHECK(!is_open_);
  CHECK_EQ(replication, 0);
  CHECK_EQ(chunk_size, 0);
  if (!IsValidUri(uri, "local")) {
    return false;
  }

  std::string scheme, path;
  ParseUri(uri, &scheme, &path);

  // Ensure parent path exists.
  fs::path parent_path = fs::path(path).parent_path();
  if (!fs::is_directory(parent_path)) {
    fs::create_directories(parent_path);
  }

  fd_ = open(path.c_str(), O_WRONLY | O_TRUNC | O_CREAT,
      S_IRWXU | S_IRWXG | S_IRWXO);
  if (fd_ < 0) {
    LOG(ERROR) << "LocalOutputStream::Open - failed to open path: " << path;
    LOG(ERROR) << strerror(errno);
    return false;
  }
  file_stream_.reset(new google::protobuf::io::FileOutputStream(fd_));

  is_open_ = true;
  uri_ = uri;
  return true;
}

google::protobuf::io::CodedOutputStream*
LocalOutputCodedBlockFile::CreateCodedStream() {
  CHECK(file_stream_.get());
  return new google::protobuf::io::CodedOutputStream(file_stream_.get());
}


///////////////////////////////////////////////////////////////////////////////
// LocalFileSystem
///////////////////////////////////////////////////////////////////////////////


LocalFileSystem::LocalFileSystem() {

}

LocalFileSystem::~LocalFileSystem(){

}


bool LocalFileSystem::GetUriPath(const std::string& uri, std::string* path){
  CHECK(path);
  std::string scheme;
  if (!ParseUri(uri, &scheme, path)){
    return false;
  }
  if (scheme != "local"){
    return false;
  }
  return true;
}


bool LocalFileSystem::Exists(const std::string& uri){
  std::string path;
  if (!GetUriPath(uri, &path)){
    return false;
  }
  return fs::exists(path);
}

bool LocalFileSystem::IsFile(const std::string& uri){
  std::string path;
  if (!GetUriPath(uri, &path)){
    return false;
  }
  return fs::is_regular(path);
}

bool LocalFileSystem::IsDirectory(const std::string& uri){
  std::string path;
  if (!GetUriPath(uri, &path)){
    return false;
  }
  return fs::is_directory(path);
}


bool LocalFileSystem::MakeDirectory(const std::string& uri){
  std::string path;
  if (!GetUriPath(uri, &path)){
    return false;
  }
  return fs::create_directories(path);
}


bool LocalFileSystem::ListDirectory(const std::string& uri, std::vector<std::string>* contents){
  CHECK(contents);
  contents->clear();

  std::string path;
  if (!GetUriPath(uri, &path)){
    return false;
  }

  for (fs::directory_iterator iter(path), end; iter != end;  ++iter) {
    contents->push_back(Uri("local",iter->path().string()));
  }
  return true;
}

bool LocalFileSystem::Remove(const std::string& uri){
  std::string path;
  if (!GetUriPath(uri, &path)){
    return false;
  }
  int num_removed = fs::remove_all(path);
  return (num_removed > 0);
}


bool LocalFileSystem::FileSize(const std::string& uri, uint64* size){
  std::string path;
  if (!GetUriPath(uri, &path)){
    return false;
  }
  LOG(FATAL) << "not yet implemented";
  return false;
}


bool LocalFileSystem::ModifiedTimestamp(const std::string& uri, uint64* timestamp){
  CHECK(timestamp);
  std::string path;
  if (!GetUriPath(uri, &path)){
    LOG(WARNING) << "Can't parse uri: " << uri;
    return false;
  }
  if (!Exists(uri)){
    LOG(WARNING) << "uri doesn't exist: " << uri;
    return false;
  }
  *timestamp = fs::last_write_time(path);
  return true;
}


bool LocalFileSystem::ChunkSize(const std::string& uri, uint64* chunk_size_bytes){
  LOG(FATAL) << "local file system doesn't use chunks.";
  return false;
}


bool LocalFileSystem::SetModifiedTimestamp(const std::string& uri, uint64 timestamp){
  std::string path;
  if (!GetUriPath(uri, &path)){
    LOG(WARNING) << "Can't parse uri: " << uri;
    return false;
  }
  if (!Exists(uri)){
    LOG(WARNING) << "uri doesn't exist: " << uri;
    return false;
  }
  fs::last_write_time(path, timestamp);
  return true;
}


InputCodedBlockFile* LocalFileSystem::OpenInput(const std::string& uri){
  std::auto_ptr<LocalInputCodedBlockFile> file_ptr(new LocalInputCodedBlockFile());
  if (!file_ptr->Open(uri)){
    file_ptr.reset();
  }
  return file_ptr.release();
}

OutputCodedBlockFile* LocalFileSystem::OpenOutput(const std::string& uri, short replication, uint64 chunk_size){
  CHECK_EQ(replication, 0) << "local file system doesn't support replication";
  CHECK_EQ(chunk_size, 0) << "local file system doesn't use chunks.";
  std::auto_ptr<LocalOutputCodedBlockFile> file_ptr(new LocalOutputCodedBlockFile());
  if (!file_ptr->Open(uri, replication, chunk_size)){
    file_ptr.reset();
  }
  return file_ptr.release();
}

bool LocalFileSystem::DownloadUri(const std::string& uri, const std::string& local_path){
  std::string path;
  if (!GetUriPath(uri, &path)){
    return false;
  }
  string parent_path = fs::path(local_path).remove_filename().string();
  fs::create_directories(parent_path);
  fs::copy_file(path, local_path);
  if(!fs::exists(local_path)){
    return false;
  }
  return true;
}




bool LocalFileSystem::CopyLocalToUri(const std::string& src_path, const std::string& dest_uri){
  if (!fs::exists(src_path)) {
    return false;
  }

  std::string dst_path;
  if (!GetUriPath(dest_uri, &dst_path)){
    return false;
  }

  // Get file info
  uint64 src_last_write_time = fs::last_write_time(src_path);
  bool is_file = fs::is_regular_file(src_path);
  bool is_dir = fs::is_directory(src_path);
  CHECK_NE(is_file, is_dir);

  if (is_file){
    uint64 size = fs::file_size(src_path);

    // Create parent path structure for file if it doesn't exist
    string parent_path = fs::path(dst_path).remove_filename().string();
    if (!fs::exists(parent_path)){
      fs::create_directories(parent_path);
    }

    fs::copy_file(src_path, dst_path, fs::copy_option::overwrite_if_exists);

    // TODO: update the remote modified timestamp to match the src
  }
  else if (is_dir){
    std::vector<std::string> dir_paths;
    for (fs::directory_iterator iter(src_path), end; iter != end;  ++iter) {
      dir_paths.push_back(iter->path().string());
    }

    BOOST_FOREACH(std::string file_path, dir_paths){
      string cur_local_file = src_path + "/" + fs::path(file_path).filename().string();
      string cur_dest_uri = dest_uri + "/" + fs::path(file_path).filename().string();
      CopyLocalToUri(cur_local_file, cur_dest_uri);
    }

    //TODO: update the local modified timestamp to match the src (is this needed for dirs?)
  }
  else{
    LOG(FATAL) << "unexpected condition";
  }

  return true;
}

InputFile* LocalFileSystem::OpenInputFile(const std::string& uri){
  InputFile* new_file = new LocalInputFile();
  if (!new_file->Open(uri)){
    delete new_file;
    new_file = NULL;
  }
  return new_file;
}

OutputFile* LocalFileSystem::OpenOutputFile(const std::string& uri, short replication, uint64 chunk_size, uint64 desired_modification_time){
  OutputFile* new_file = new LocalOutputFile();
  if (!new_file->Open(uri, replication, chunk_size, desired_modification_time)){
    delete new_file;
    new_file = NULL;
  }
  return new_file;
}

} // close namespace pert
