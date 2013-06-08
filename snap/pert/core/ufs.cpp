#include "snap/pert/core/ufs.h"
#include "snap/google/glog/logging.h"
#include "snap/boost/filesystem.hpp"
#include "snap/pert/core/fs_local.h"

#include "snap/boost/algorithm/string/split.hpp"
#include "snap/boost/foreach.hpp"
#include "snap/boost/algorithm/string/classification.hpp"
#include "snap/pert/core/md5.h"

#ifdef PERT_USE_MAPR_FS
#include "snap/pert/core/fs_mapr.h"
#endif

namespace fs = boost::filesystem;

namespace pert {

// Returns a pointer to a FileSystem appropriate for the URI scheme prefix or NULL
// on failure.
FileSystem* GetFilesystemForUri(const std::string& uri) {
	FileSystem* fs = NULL;
	std::string scheme, path, error_message;
	if (!ParseUri(uri, &scheme, &path, &error_message)) {
		LOG(WARNING) << error_message; // Don't exit... when called from python code may want to raise exception and not abort
		return fs;
	}

	if (scheme == "local") {
		fs = new LocalFileSystem();
	}
#ifdef PERT_USE_MAPR_FS
	else if (scheme == "maprfs") {
		fs = new MaprFileSystem();
	}
#endif
	else {
		LOG(ERROR) << "URI has unknown scheme: " << scheme;
	}

	return fs;
}

// A canonical url has this form: scheme:///path/to/file
std::string CanonicalizeUri(std::string uri) {
	std::string scheme, path, error_message;
	CHECK(pert::ParseUri(uri, &scheme, &path, &error_message))
			<< "invalid uri: " << uri << "\n" << error_message;
	std::string clean_uri = scheme + "://" + CanonicalizePath(path);
	return clean_uri;
}

// A canonical path
// always starts with /
// No multiple adjacent slashes
// No trailing slash
std::string CanonicalizePath(std::string path) {
	std::vector < std::string > tokens;
	boost::split(tokens, path, boost::is_any_of("/"), boost::token_compress_on);

	std::string canonical_path;
	//std::string prev_token;
	BOOST_FOREACH(std::string token, tokens) {
		if (token == ".")
			continue;
		if (token.empty())
			continue;
		canonical_path += "/" + token;
	}
	CHECK(!canonical_path.empty());
	return canonical_path;
}

std::string Uri(const std::string& scheme, const std::string& path) {
	return scheme + "://" + CanonicalizePath(path);
}

bool ParseUri(const std::string& uri, std::string* scheme, std::string* path,
		std::string* error_message) {
	CHECK(scheme);
	CHECK(path);

	// must not contain a space
	if (uri.find(" ") != std::string::npos) {
		if (error_message) {
			error_message->assign(
					"Uri may not contain spaces. Got this: " + uri);
		}
		return false;
	}

	// must have exactly one scheme delimiter
  std::string prefix = ":/";
  size_t pos = uri.find(prefix);
  if (pos == std::string::npos) {
    if (error_message) {
      error_message->assign(
          "missing <scheme>:/ part of a valid uri. Got this: " + uri);
    }
    return false;
  }
  size_t first_pos = uri.find(prefix);
  size_t last_pos = uri.rfind(prefix);
  if (first_pos != last_pos) {
    if (error_message) {
      error_message->assign("uri has multiple <scheme>:/ prefixes. Got this: " + uri);
    }
    return false;
  }

	// scheme prefix must not be empty
	*scheme = uri.substr(0, pos);
	if (scheme->size() == 0) {
		if (error_message) {
			error_message->assign(
					"missing <scheme>:/ part of a valid uri. Got this: " + uri);
		}
		return false;
	}

	CHECK_LE(pos+3, uri.size());
	*path = uri.substr(pos + prefix.size());

	// make sure the path we return starts with /
	if (path->empty() || path->at(0) != '/') {
		*path = "/" + *path;
	}

	*path = CanonicalizePath(*path);

	return true;
}

bool IsValidUri(const std::string& uri, const std::string& required_scheme) {
	std::string scheme, path;
	if (!ParseUri(uri, &scheme, &path)) {
		LOG(ERROR) << "URI is missing prefix <scheme>:// : " << uri;
		return false;
	}

	if (scheme != required_scheme) {
		LOG(ERROR) << "URI using wrong scheme. Given: " << scheme
				<< " but expected: " << required_scheme;
		return false;
	}
	return true;
}

bool IsValidUri(const std::string& uri) {
	std::string scheme, path;
	if (!ParseUri(uri, &scheme, &path)) {
		return false;
	}
	return true;
}

bool Exists(const std::string& uri) {
	boost::scoped_ptr<FileSystem> fs(GetFilesystemForUri(uri));
	CHECK(fs.get());
	return fs->Exists(uri);
}

bool IsFile(const std::string& uri) {
	boost::scoped_ptr<FileSystem> fs(GetFilesystemForUri(uri));
	CHECK(fs.get());
	return fs->IsFile(uri);
}

bool IsDirectory(const std::string& uri) {
	boost::scoped_ptr<FileSystem> fs(GetFilesystemForUri(uri));
	CHECK(fs.get());
	return fs->IsDirectory(uri);
}

bool MakeDirectory(const std::string& uri) {
  boost::scoped_ptr<FileSystem> fs(GetFilesystemForUri(uri));
  CHECK(fs.get());
  return fs->MakeDirectory(uri);
}


bool ListDirectory(const std::string& uri, std::vector<std::string>* uris) {
	boost::scoped_ptr<FileSystem> fs(GetFilesystemForUri(uri));
	CHECK(fs.get());
	return fs->ListDirectory(uri, uris);
}

bool Remove(const std::string& uri) {
	boost::scoped_ptr<FileSystem> fs(GetFilesystemForUri(uri));
	CHECK(fs.get());
	return fs->Remove(uri);
}

bool FileSize(const std::string& uri, uint64* size) {
  boost::scoped_ptr<FileSystem> fs(GetFilesystemForUri(uri));
  CHECK(fs.get());
  return fs->FileSize(uri, size);
}


// Returns seconds since last modified
bool ModifiedTimestamp(const std::string& uri, uint64* timestamp) {
  CHECK(timestamp);
	boost::scoped_ptr<FileSystem> fs(GetFilesystemForUri(uri));
	CHECK(fs.get());
	return fs->ModifiedTimestamp(uri, timestamp);
}


// Returns seconds since last modified
bool SetModifiedTimestamp(const std::string& uri, uint64 timestamp) {
  CHECK(timestamp);
  boost::scoped_ptr<FileSystem> fs(GetFilesystemForUri(uri));
  CHECK(fs.get());
  return fs->SetModifiedTimestamp(uri, timestamp);
}


bool ChunkSize(const std::string& uri, uint64* chunk_size_bytes){
  CHECK(chunk_size_bytes);
  boost::scoped_ptr<FileSystem> fs(GetFilesystemForUri(uri));
  CHECK(fs.get());
  return fs->ChunkSize(uri, chunk_size_bytes);
}

InputCodedBlockFile* OpenInput(const std::string& uri) {
	InputCodedBlockFile* file = NULL;
	boost::scoped_ptr<FileSystem> fs(GetFilesystemForUri(uri));
	if (fs) {
		CHECK(fs->Exists(uri)) << "Uri doesn't exist: " << uri;
		CHECK(fs->IsFile(uri)) << "Expected a file: " << uri;
		file = fs->OpenInput(uri);
	}
	return file;
}

OutputCodedBlockFile* OpenOutput(const std::string& uri, short replication, uint64 chunk_size) {
	OutputCodedBlockFile* file = NULL;
	boost::scoped_ptr<FileSystem> fs(GetFilesystemForUri(uri));
	CHECK(fs.get());
  file = fs->OpenOutput(uri, replication, chunk_size);
	return file;
}

bool DownloadUri(const std::string& uri, const std::string& local_path) {
	boost::scoped_ptr<FileSystem> fs(GetFilesystemForUri(uri));
	CHECK(fs.get());
	return fs->DownloadUri(uri, local_path);
}

bool CopyLocalToUri(const std::string& local_path,
		const std::string& dest_uri) {
	boost::scoped_ptr<FileSystem> fs(GetFilesystemForUri(dest_uri));
	CHECK(fs.get());
	return fs->CopyLocalToUri(local_path, dest_uri);
}


bool CopyUri(const std::string& src_uri, const std::string& dst_uri){
  if (!Exists(src_uri)){
    return false;
  }

  boost::scoped_ptr<FileSystem> src_fs(GetFilesystemForUri(src_uri));
  if (!src_fs.get()) {
    return false;
  }

  boost::scoped_ptr<FileSystem> dst_fs(GetFilesystemForUri(dst_uri));
  if (!dst_fs.get()) {
    return false;
  }

  uint64 desired_mod_time = 0;

  if (IsDirectory(src_uri)){
    //LOG(INFO) << "about to copy directory: " << src_uri;
    if (Exists(dst_uri)){
      CHECK(IsDirectory(dst_uri));
    }
    std::vector<std::string> contents;

    CHECK(ListDirectory(src_uri, &contents));
    CHECK_GT(contents.size(), 0);
    BOOST_FOREACH(const std::string& cur_src_uri, contents){
      std::string cur_dst_uri = dst_uri + "/" + fs::path(cur_src_uri).filename().string();
      CHECK(CopyUri(cur_src_uri, cur_dst_uri));
    }

    CHECK(ModifiedTimestamp(src_uri, &desired_mod_time));
  }
  else{ // is a file
    if (Exists(dst_uri)){
      CHECK(IsFile(dst_uri));
    }

    {
      boost::scoped_ptr<InputFile> input_file(src_fs->OpenInputFile(src_uri));
      if (!input_file.get()) {
        return false;
      }
      desired_mod_time = input_file->ModificationTime();
      CHECK_GT(desired_mod_time, 0);

      boost::scoped_ptr<OutputFile> output_file(dst_fs->OpenOutputFile(dst_uri, 0, 0));
      if (!output_file.get()) {
        return false;
      }

      uint32 buffer_size = 1048576; //
      char* buffer = new char[buffer_size];
      uint64 num_bytes_src_file = input_file->Size();
      uint64 num_bytes_copied = 0;
      while (num_bytes_copied < num_bytes_src_file){
        uint32 bytes_read = 0;
        CHECK(input_file->Read(buffer_size, buffer, &bytes_read));
        CHECK_LE(bytes_read, buffer_size);
        CHECK(output_file->Write(bytes_read, buffer));
        num_bytes_copied += bytes_read;
      }
      delete [] buffer;
    }

    dst_fs->SetModifiedTimestamp(dst_uri, desired_mod_time);
  }

  return true;
}

InputFile* OpenInputFile(const std::string& uri){
  boost::scoped_ptr<FileSystem> fs(GetFilesystemForUri(uri));
  if (!fs.get()) {
    return NULL;
  }
  return fs->OpenInputFile(uri);
}

OutputFile* OpenOutputFile(const std::string& uri, short replication, uint64 chunk_size, uint64 desired_modification_time){
  boost::scoped_ptr<FileSystem> fs(GetFilesystemForUri(uri));
  if (!fs.get()) {
    return NULL;
  }
  return fs->OpenOutputFile(uri, replication, chunk_size, desired_modification_time);
}


bool ComputeMd5DigestForUriSet(const std::vector<std::string>& input_uris, std::string* md5_as_hex){
  CHECK(md5_as_hex);
  if (input_uris.empty()){
    return false;
  }

  // sort so fingerprint is independent of the input uri ordering
  std::vector<std::string> uris = input_uris;
  std::sort(uris.begin(), uris.end());

  BOOST_FOREACH(std::string uri, uris){
    if (!Exists(uri)){
      LOG(INFO) << "uri does not exist: " << uri;
      return false;
    }
  }
  if (uris.size() == 1){
    std::string uri = uris[0];
    if (!ComputeMd5DigestForUri(uri, md5_as_hex)){
      LOG(INFO) << "Error getting md5sum for shard: " << uri;
      return false;
    }
  }
  else{
    Md5Digest md5_digest;
    BOOST_FOREACH(std::string uri, uris){
      std::string cur_md5;
      if (!ComputeMd5DigestForUri(uri, &cur_md5)){
        LOG(INFO) << "Error getting md5sum for shard: " << uri;
        return false;
      }
      md5_digest.Update(cur_md5.data(), cur_md5.size());
    }
    md5_as_hex->assign(md5_digest.GetAsHexString());
  }
  return true;
}

bool ComputeMd5DigestForUri(const std::string& uri, std::string* md5_as_hex){
  if (!Exists(uri)){
    return false;
  }

  if (!IsFile(uri)){
    return false; // can only compute digest for a file
  }

  boost::scoped_ptr<FileSystem> fs(GetFilesystemForUri(uri));
  if (!fs.get()) {
    return false;
  }

  boost::scoped_ptr<InputFile> input_file(fs->OpenInputFile(uri));
  if (!input_file.get()) {
    return false;
  }

  Md5Digest md5_digest;
  uint32 buffer_size = 1048576;
  char* buffer = new char[buffer_size];
  uint64 num_bytes_src_file = input_file->Size();
  uint64 num_bytes_hashed = 0;
  while (num_bytes_hashed < num_bytes_src_file){
    uint32 bytes_read = 0;
    CHECK(input_file->Read(buffer_size, buffer, &bytes_read));
    CHECK_LE(bytes_read, buffer_size);
    md5_digest.Update(buffer, bytes_read);
    num_bytes_hashed += bytes_read;
  }
  delete [] buffer;
  md5_as_hex->assign(md5_digest.GetAsHexString());

  return true;
}


/*
 std::string GetLocalCachePath(const std::string& uri){
 std::string scheme, path;
 if (!ParseUri(uri, &scheme, &path)) {
 LOG(FATAL) << "uri is missing schema prefix <scheme>:// - provided uri: " << uri;
 }

 return "/tmp/" + path;
 }

 bool GetCachedCopy(const std::string& uri, std::string* local_path_out, bool* download_performed_status){
 CHECK(local_path_out);
 std::string local_path = GetLocalCachePath(uri);

 bool is_stale = false;
 if (!fs::exists(local_path)){
 is_stale = true;
 LOG(INFO) << "File not yet cached: " << local_path;
 }
 else{
 uint64 src_last_write_time = ModifiedTimestamp(uri);
 uint64 cache_last_write_time = ModifiedTimestamp("local://" + local_path);
 //LOG(INFO) << "src_last_write_time: " << src_last_write_time;
 //LOG(INFO) << "cache_last_write_time: " << cache_last_write_time;
 if (src_last_write_time > cache_last_write_time){
 is_stale = true;
 LOG(INFO) << "Cached file is stale: " << local_path;

 fs::remove_all(local_path); // needed because download would fail if file still existed
 }
 }

 // Download the URI if stale
 if (is_stale){
 if (!DownloadUri(uri, local_path)){
 return false;
 }
 CHECK(fs::exists(local_path));
 }
 else{
 LOG(INFO) << "File already cached: " << local_path;
 }

 // Optional output for testing purposes
 if (download_performed_status){
 *download_performed_status = is_stale;
 }

 *local_path_out = local_path;
 return true;
 }
 */

} // close namespace
