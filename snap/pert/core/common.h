#pragma once

// NOTE: If you modify anything here, this creates an incompatible file format
// If this must be done, don't forget to modify PERT_FILE_VERSION so we can
// gracefully detect incompatible formats.

#define PERT_FILE_MAGIC        0x454c49465f54564bULL // ascii: KVT_FILE
#define PERT_FILE_VERSION      2
#define PERT_DATA_BLOCK_MAGIC  0x415441445f54564bULL // ascii: KVT_DATA
#define PERT_META_BLOCK_MAGIC  0x4154454d5f54564bULL // ascii: KVT_META

#define PERT_TAIL_SIZE 78 // must not change, based on proto size
#define PERT_COMPRESSED_BLOCK_HEADER_SIZE 18

#define PERT_PROTO_FORMAT_METADATA_NAME "value.proto_format"
#define PERT_BLOOM_FILTER_METADATA_NAME "value.bloom_filter"

