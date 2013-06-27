#include "iw/imagegraph.h"
#include "iw/util.h"
#include "snap/scul/scul.hpp"
#include "snap/pert/pert.h"
#include "iw/iw.pb.h"
#include "snap/google/base/stringprintf.h"
#include "snap/google/glog/logging.h"
#include "tide/tide.h"
#include <algorithm>    // std::max



namespace iw {

// Convert nfa (actually the log of nfa) to the range 0-1 such that unlikely matches get weight 0 and likely matches get weight closer to 1.
float NfaToWeight(float nfa){
  float weight = 0;
  if (nfa < 0){
    weight = 1.0 - pow(10, 0.01*nfa);
  }
  CHECK_GE(weight, 0.0);
  CHECK_LE(weight, 1.0);
  return weight;
}

bool CreateImageGraph2(const std::string& matches_uri, const std::string& photoid_uri, ImageGraph* ig){
  CHECK(ig);
  ig->Clear();


  // Ensure that the graph contains a vertex for every image... even if it was
  // not matched by pulling image ids from the tide dataset instead of the set
  // of matched images (which is generally a subset).
  pert::StringTableReader photoid_reader;
  CHECK(photoid_reader.Open(photoid_uri));

  scul::AutoIndexer<uint64> imageid_to_vertexid;
  string key, value;
  while (photoid_reader.Next(&key, &value)){
    uint64 image_id = KeyToUint64(key);
    imageid_to_vertexid.GetIndex(image_id);
  }
  photoid_reader.Close();

  pert::StringTableShardSetReader reader;
  CHECK(reader.Open(matches_uri));

  std::string image_pair_key;
  GeometricMatchResult match_result;
  uint64 image_a, image_b;


  int max_phase_id = 0;

  while (reader.NextProto(&image_pair_key, &match_result)){
    CHECK(match_result.IsInitialized());
    ParseImagePairKey(image_pair_key, &image_a, &image_b);
    uint32 vertex_a = imageid_to_vertexid.GetIndex(image_a);
    uint32 vertex_b = imageid_to_vertexid.GetIndex(image_b);
    BOOST_FOREACH(const GeometricMatch& match, match_result.matches()){
      ImageGraph_Edge* edge = ig->add_edges();
      edge->set_src(vertex_a);
      edge->set_dst(vertex_b);
      float weight = NfaToWeight(match.nfa());
      edge->set_weight(weight);
      edge->set_nfa(match.nfa());
      edge->set_phase(match_result.phase());
    }
    max_phase_id = std::max(max_phase_id, int(match_result.phase()));
  }

  ig->set_num_phases(max_phase_id + 1);

  CHECK_GT(ig->edges_size(), 0);
  BOOST_FOREACH(uint64 image_id, imageid_to_vertexid.GetElements()){
    ig->add_vertices()->set_image_id(image_id);
  }
  return true;
}

void SaveImageGraph(const ImageGraph& ig, const std::string& output_uri){
  pert::StringTableWriter writer;
  CHECK(writer.Open(output_uri, 1));

  // store edges
  for (int i = 0; i < ig.edges_size(); ++i) {
    string key = StringPrintf("e%010d", i);
    CHECK(writer.Add(key, ig.edges(i).SerializeAsString()));
  }

  // store vertices
  for (int i = 0; i < ig.vertices_size(); ++i) {
    string key = StringPrintf("v%010d", i);
    CHECK(writer.Add(key, ig.vertices(i).SerializeAsString()));
  }

  writer.AddMetadata("num_edges", StringPrintf("%d", ig.edges_size()));
  writer.AddMetadata("num_vertices", StringPrintf("%d", ig.vertices_size()));
  writer.AddMetadata("num_phases", StringPrintf("%d", ig.num_phases()));

  writer.Close();
}

bool LoadImageGraph(const std::string& uri, ImageGraph* ig){
  CHECK(ig);
  ig->Clear();
  pert::StringTableReader reader;
  CHECK(reader.Open(uri)) << "failed to open uri: " << uri;

  std::string tmp;
  CHECK(reader.GetMetadata("num_edges", &tmp)) << "this doesn't appear to be a ig uri:" << uri;
  uint64 num_edges = atol(tmp.c_str());

  CHECK(reader.GetMetadata("num_vertices", &tmp)) << "this doesn't appear to be a ig uri:" << uri;
  uint64 num_vertices = atol(tmp.c_str());

  //CHECK(reader.GetMetadata("num_phases", &tmp)) << "this doesn't appear to be a ig uri:" << uri;
  int num_phases = 1;
  if (!reader.GetMetadata("num_phases", &tmp)){
    LOG(INFO) << "this ig is missing phases info:" << uri;
    num_phases = atoi(tmp.c_str());
  }

  CHECK_EQ(reader.Entries(), num_edges + num_vertices);

  std::string key, value;

  // load edges
  for (int i = 0; i < num_edges; ++i) {
    reader.Next(&key, &value);
    CHECK_EQ(key[0], 'e');
    CHECK(ig->add_edges()->ParseFromString(value));
  }

  // load vertices
  for (int i = 0; i < num_vertices; ++i) {
    reader.Next(&key, &value);
    CHECK_EQ(key[0], 'v');
    CHECK(ig->add_vertices()->ParseFromString(value));
  }

  ig->set_num_phases(num_phases);
  return true;
}

} // close namespace iw
