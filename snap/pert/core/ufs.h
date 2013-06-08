// Universal File System
// API used by pert to access many different local and distributed file systems.
#pragma once

#include <vector>
#include <string>
#include "snap/pert/core/fs_base.h"

namespace pert {

class FileSystem;
FileSystem* GetFilesystemForUri(const std::string& uri);

// Removes and . in path and compresses out repeated /
std::string CanonicalizePath(std::string path);

// Removes and . in path and compresses out repeated /
std::string CanonicalizeUri(std::string uri);

// Constructs a canonical uri string given a scheme and path
std::string Uri(const std::string& scheme, const std::string& path);

// Extracts the scheme and path components from a uri (returns false on fail).
bool ParseUri(const std::string& uri, std::string* scheme, std::string* path, std::string* error_message = NULL);

// Returns true if the URI is valid and has the given scheme prefix.
bool IsValidUri(const std::string& uri, const std::string& scheme);

// Returns true if the URI is valid
bool IsValidUri(const std::string& uri);

// Returns true if the URI resolves to an existing file or dir.
bool Exists(const std::string& uri);

// Returns true if the URI resolves to an existing file.
bool IsFile(const std::string& uri);

// Returns true if the URI resolves to an existing directory.
bool IsDirectory(const std::string& uri);

bool MakeDirectory(const std::string& uri);

// If URI is a directory, copies directory contents to vector and returns true.
bool ListDirectory(const std::string& uri, std::vector<std::string>* uris);

// Returns true if the URI was succesfully removed.
bool Remove(const std::string& uri);

// Returns size of file in bytes
bool FileSize(const std::string& uri, uint64* size);

// Returns seconds since last modified
bool ModifiedTimestamp(const std::string& uri, uint64* timestamp);

bool SetModifiedTimestamp(const std::string& uri, uint64 timestamp);

// Get's the file's chunk size, or returns false if fs doesn't use chunks
bool ChunkSize(const std::string& uri, uint64* chunk_size_bytes);

// Returns pointer to a new InputCodedBlockFile (or NULL on failure).
class InputCodedBlockFile;
InputCodedBlockFile* OpenInput(const std::string& uri);

// Returns pointer to a new OutputCodedBlockFile (or NULL on failure).
class OutputCodedBlockFile;
OutputCodedBlockFile* OpenOutput(const std::string& uri, short replication = 0, uint64 chunk_size = 0);

// Downloads a URI to the local filesystem.
bool DownloadUri(const std::string& source_uri, const std::string& local_path);

// Uploads a local file to a URI
bool CopyLocalToUri(const std::string& local_path, const std::string& dest_uri);

bool CopyUri(const std::string& src_uri, const std::string& dest_uri);

// Returns pointer to a new InputFile (or NULL on failure).
class InputFile;
InputFile* OpenInputFile(const std::string& uri);

// Returns pointer to a new OutputFile (or NULL on failure).
class OutputFile;
OutputFile* OpenOutputFile(const std::string& uri, short replication = 0, uint64 chunk_size = 0, uint64 desired_modification_time = 0);

bool ComputeMd5DigestForUriSet(const std::vector<std::string>& uris, std::string* md5_as_hex);

bool ComputeMd5DigestForUri(const std::string& uri, std::string* md5_as_hex);

}
