#include "iw/imageregiongraph.h"
#include "iw/imagegraph.h"
#include "iw/util.h"
#include "snap/meanshift/meanshift.h"
#include "iw/iw.pb.h"
#include "snap/pert/prototable.h"
#include "snap/boost/foreach.hpp"
#include "snap/google/base/hashutils.h"
#include "snap/google/base/stringprintf.h"
#include "tide/tide.pb.h"
#include <algorithm>
#include "snap/opencv2/core/core.hpp"
#include "snap/progress/progress.hpp"

using namespace std;

namespace iw {

void SaveImageRegionGraph(const ImageRegionGraph& irg, const std::string& uri){
  pert::StringTableWriter writer;
  CHECK(writer.Open(uri, 1));

  // store edges
  for (int i = 0; i < irg.edge_size(); ++i) {
    string key = StringPrintf("e%010d", i);
    CHECK(writer.Add(key, irg.edge(i).SerializeAsString()));
  }

  // store vertices
  for (int i = 0; i < irg.vertex_size(); ++i) {
    string key = StringPrintf("v%010d", i);
    CHECK(writer.Add(key, irg.vertex(i).SerializeAsString()));
  }

  writer.AddMetadata("num_edges", StringPrintf("%d", irg.edge_size()));
  writer.AddMetadata("num_vertices", StringPrintf("%d", irg.vertex_size()));
  writer.Close();
}


bool LoadImageRegionGraph(const std::string& uri, ImageRegionGraph* irg){
  CHECK(irg);
  irg->Clear();
  pert::StringTableReader reader;
  CHECK(reader.Open(uri)) << "failed to open uri: " << uri;
  std::string tmp;
  CHECK(reader.GetMetadata("num_edges", &tmp)) << "this doesn't appear to be a irg uri:" << uri;
  uint64 num_edges = atol(tmp.c_str());
  CHECK(reader.GetMetadata("num_vertices", &tmp)) << "this doesn't appear to be a irg uri:" << uri;
  uint64 num_vertices = atol(tmp.c_str());
  CHECK_EQ(reader.Entries(), num_edges + num_vertices);
  std::string key, value;

  // load edges
  for (int i = 0; i < num_edges; ++i) {
    reader.Next(&key, &value);
    CHECK_EQ(key[0], 'e');
    CHECK(irg->add_edge()->ParseFromString(value));
  }

  // load vertices
  for (int i = 0; i < num_vertices; ++i) {
    reader.Next(&key, &value);
    CHECK_EQ(key[0], 'v');
    CHECK(irg->add_vertex()->ParseFromString(value));
  }
  return true;
}


double Area(const BoundingBox& bb){
  return (bb.y2() - bb.y1())*(bb.x2() - bb.x1());
}

double IntersectionArea(const BoundingBox& r1, const BoundingBox& r2){
  double y_max = std::min(r1.y2(), r2.y2());
  double y_min = std::max(r1.y1(), r2.y1());

  double x_max = std::min(r1.x2(), r2.x2());
  double x_min = std::max(r1.x1(), r2.x1());

  if (x_max < x_min){
    return 0;
  }
  if (y_max < y_min){
    return 0;
  }
  return (x_max-x_min)*(y_max-y_min);
}

double OverlapWeight(const BoundingBox& bb_a, const BoundingBox& bb_b){
  double intersection_area = IntersectionArea(bb_a, bb_b);
  double union_area = Area(bb_a) + Area(bb_b) - intersection_area;
  return intersection_area/union_area;
}

class BoundingBoxCluster {
public:
  std::vector<uint32> members;
  BoundingBox center;
};


class ImageRegionGraphBuilder {
public:
  void Run(const std::string& match_graph_uri, ImageRegionGraph* irg);
  void Run(const std::string& match_graph_uri, const std::string& tide_uri, ImageRegionGraph* irg);

protected:
  // methods
  ImageRegionGraph_Vertex* GetVertex(uint32 vertex_id);

  uint32 AddVertex(uint64 image_id, const BoundingBox& bb, ImageRegionGraph_Vertex_VertexType vertex_type);
  ImageRegionGraph_Edge* AddEdge(uint32 v1, uint32 v2, float weight);
  void AddEdgeWithOverlapWeight(uint32 vertex_id_a, uint32 vertex_id_b);

  void AddInterImageLinks(const std::string& match_results_filename);
  void AddIntraImageLinks();
  void ClusterBoundingBoxes(const std::vector<uint32>& vertex_ids, std::vector<BoundingBoxCluster>* clusters);
  void AddGroundTruthRegionLinks(const std::string& tide_objects_filename);

  bool SelectVertexWithLargestOverlapWeight(const BoundingBox& query_bb, const std::vector<uint32>& candidate_vertices, uint32* output_vertex_id, double* output_overlap_weight );

  // members
  ImageRegionGraph* irg_;
  uint32 next_vertex_id_;
  uint32 next_edge_id_;
  typedef std::map<uint64, std::vector<uint32> > ImageIdToVertexIdMap;
  ImageIdToVertexIdMap imageid_to_vertices_;
  ImageIdToVertexIdMap imageid_to_cluster_vertices_;

};


bool CreateImageRegionGraph(const std::string& matches_uri, const std::string& tide_uri,  ImageRegionGraph* irg){
  ImageRegionGraphBuilder builder;
  builder.Run(matches_uri, tide_uri, irg);
  return true;
}

class BoundingBoxBuilder {
public:
  BoundingBoxBuilder() : x_min(kuint32max), y_min(kuint32max), x_max(0), y_max(0) { }

  void Update(uint32 x, uint32 y) {
    x_min = std::min(x_min, x);
    y_min = std::min(y_min, y);
    x_max = std::max(x_max, x);
    y_max = std::max(y_max, y);
  }

  void CopyTo(BoundingBox* bb) {
    bb->set_x1(x_min);
    bb->set_y1(y_min);
    bb->set_x2(x_max);
    bb->set_y2(y_max);
  }

private:
  uint32 x_min, y_min, x_max, y_max;
};

/*
void GetBoundingBoxes(const GeometricMatch& match, BoundingBox* box_a,
    BoundingBox* box_b) {
  const ::google::protobuf::RepeatedPtrField< iw::Correspondence>& correspondences =
      match.correspondences();
  BoundingBoxBuilder a, b;
  for (int i = 0; i < correspondences.size(); ++i) {
    const Correspondence& c = correspondences.Get(i);
    a.Update(c.a().pos().x(), c.a().pos().y());
    b.Update(c.b().pos().x(), c.b().pos().y());
  }
  a.CopyTo(box_a);
  b.CopyTo(box_b);
}
*/


void GetBoundingBoxes(const GeometricMatch& match, BoundingBox* box_a,
    BoundingBox* box_b) {
  const ::google::protobuf::RepeatedPtrField< iw::Correspondence>& correspondences =
      match.correspondences();
  BoundingBoxBuilder a, b;
  for (int i = 0; i < correspondences.size(); ++i) {
    const Correspondence& c = correspondences.Get(i);
    float radius = c.a().radius();
    a.Update(c.a().pos().x()-radius, c.a().pos().y()-radius);
    a.Update(c.a().pos().x()-radius, c.a().pos().y()+radius);
    a.Update(c.a().pos().x()+radius, c.a().pos().y()-radius);
    a.Update(c.a().pos().x()+radius, c.a().pos().y()+radius);

    radius = c.b().radius();
    b.Update(c.b().pos().x()-radius, c.b().pos().y()-radius);
    b.Update(c.b().pos().x()-radius, c.b().pos().y()+radius);
    b.Update(c.b().pos().x()+radius, c.b().pos().y()-radius);
    b.Update(c.b().pos().x()+radius, c.b().pos().y()+radius);
  }
  a.CopyTo(box_a);
  b.CopyTo(box_b);
}

void ImageRegionGraphBuilder::Run(
    const std::string& match_graph_uri,
    ImageRegionGraph* irg) {
  CHECK(irg);
  irg_ = irg;
  irg_->Clear();

  next_vertex_id_ = 0;
  next_edge_id_ = 0;
  imageid_to_vertices_.clear();

  AddInterImageLinks(match_graph_uri);
  AddIntraImageLinks();
}

void ImageRegionGraphBuilder::Run(
    const std::string& match_graph_uri,
    const std::string& tide_uri,
    ImageRegionGraph* irg) {
  Run(match_graph_uri, irg);
  AddGroundTruthRegionLinks(tide_uri);
}

ImageRegionGraph_Vertex* ImageRegionGraphBuilder::GetVertex(uint32 vertex_id) {
  return irg_->mutable_vertex(vertex_id);
}

uint32 ImageRegionGraphBuilder::AddVertex(uint64 image_id,
    const BoundingBox& bb, ImageRegionGraph_Vertex_VertexType vertex_type) {

  uint32 vertex_id = next_vertex_id_++;
  imageid_to_vertices_[image_id].push_back(vertex_id);

  if (vertex_type == ImageRegionGraph_Vertex_VertexType_CLUSTER) {
    imageid_to_cluster_vertices_[image_id].push_back(vertex_id);
  }

  ImageRegionGraph_Vertex* vertex = irg_->add_vertex();

  vertex->set_image_id(image_id);
  vertex->set_type(vertex_type);
  vertex->mutable_bounding_box()->CopyFrom(bb);
  return vertex_id;
}

ImageRegionGraph_Edge* ImageRegionGraphBuilder::AddEdge(uint32 v1, uint32 v2,
    float weight) {
  ImageRegionGraph_Edge* edge = irg_->add_edge();
  next_edge_id_++;
  edge->set_weight(weight);
  edge->set_src(v1);
  edge->set_dst(v2);
  return edge;
}

void ImageRegionGraphBuilder::AddEdgeWithOverlapWeight(
    uint32 vertex_id_a, uint32 vertex_id_b) {
  const BoundingBox& roi_a = GetVertex(vertex_id_a)->bounding_box();
  const BoundingBox& roi_b = GetVertex(vertex_id_b)->bounding_box();

  int min_area = 5 * 5;

  if (Area(roi_a) < min_area || Area(roi_b) < min_area) {
    return;
  }

  double overlap_frac = OverlapWeight(roi_a, roi_b);
  if (overlap_frac > 0.0) {
    AddEdge(vertex_id_a, vertex_id_b, overlap_frac);
  }

}


void ImageRegionGraphBuilder::AddInterImageLinks(
    const std::string& match_graph_uri) {
  pert::StringTableShardSetReader reader;
  LOG(INFO) << "AddInterImageLinks";

  std::vector<std::string> shard_uris = pert::GetShardUris(match_graph_uri);
  CHECK(reader.Open(shard_uris)) << "can't open uri: " << match_graph_uri;
  LOG(INFO) << "opened uri: " << match_graph_uri;
  std::string key;

  uint64 image_a_id, image_b_id;
  //uint64 num_pairs = reader.Entries();
  uint64 pair_count = 0;
  GeometricMatchResult match_result;
  std::string value;
  progress::ProgressBar<int> progress_bar(reader.Entries());
  while (reader.Next(&key, &value)) {
    CHECK(match_result.ParseFromString(value));
    ParseImagePairKey(key, &image_a_id, &image_b_id);

    //LOG_EVERY_N(INFO, 100) << "progress: " << double(pair_count)/num_pairs;
    //LOG_EVERY_N(INFO, 10000) << "progress: " << pair_count;
    progress_bar.Increment();
    pair_count++;
    for (uint32 i = 0; i < match_result.matches_size(); ++i) {
      const GeometricMatch& match = match_result.matches(i);

      // reject any matches that are not high quality
      if (match.nfa() > -2.0){
        continue;
      }

      uint32 vertex_id_1, vertex_id_2;
      BoundingBox bb_1, bb_2;
      GetBoundingBoxes(match, &bb_1, &bb_2);
      vertex_id_1 = AddVertex(image_a_id, bb_1, ImageRegionGraph_Vertex_VertexType_MATCH);
      vertex_id_2 = AddVertex(image_b_id, bb_2, ImageRegionGraph_Vertex_VertexType_MATCH);
      float weight = NfaToWeight(match.nfa());
      AddEdge(vertex_id_1, vertex_id_2, weight);
    }
  }
}

void ImageRegionGraphBuilder::ClusterBoundingBoxes(
    const std::vector<uint32>& vertex_ids,
    std::vector<BoundingBoxCluster>* clusters) {
  CHECK(clusters);
  int num_bb = vertex_ids.size();
  cv::Mat data(num_bb, 4, CV_32F);
  cv::MatIterator_<float> it = data.begin<float>();
  for (uint32 i = 0; i < vertex_ids.size(); ++i) {
    const BoundingBox& bb = GetVertex(vertex_ids[i])->bounding_box();
    *it++ = bb.x1();
    *it++ = bb.y1();
    *it++ = bb.x2();
    *it++ = bb.y2();
  }

  cv::Mat centers;
  std::vector<uint32> labels;

  double bandwidth = 0;
  if (!meanshift::EstimateBandwidth(data, 0.5, 500, &bandwidth)) {
    bandwidth = 500;
  }

  meanshift::GenerateClusterLabels(data, bandwidth, &centers, &labels);
  int num_clusters = centers.rows;
  clusters->resize(num_clusters);

  //copy cluster centers to output
  for (int i = 0; i < num_clusters; ++i) {
    float* c = centers.ptr<float>(i); //get ptr to ith ro of centers matrix
    BoundingBox& bb = clusters->at(i).center;
    bb.set_x1(*c++);
    bb.set_y1(*c++);
    bb.set_x2(*c++);
    bb.set_y2(*c++);
  }

  CHECK_EQ(labels.size(), vertex_ids.size());
  // collect ids of cluster members for each cluster
  for (uint32 i = 0; i < labels.size(); ++i) {
    uint32 cluster_label = labels[i];
    clusters->at(cluster_label).members.push_back(vertex_ids[i]);
  }
}

void ImageRegionGraphBuilder::AddIntraImageLinks() {
  uint32 num_images = imageid_to_vertices_.size();
  uint32 images_processed = 0;
  LOG(INFO) << "AddIntraImageLinks";

  progress::ProgressBar<uint32> progress_bar(num_images);
  for (ImageIdToVertexIdMap::iterator iter = imageid_to_vertices_.begin();
      iter != imageid_to_vertices_.end(); ++iter) {
    uint64 image_id = iter->first;
    const std::vector<uint32>& image_region_ids = iter->second;
    std::vector<BoundingBoxCluster> clusters;
    ClusterBoundingBoxes(image_region_ids, &clusters);
    std::vector<uint32> cluster_region_vertex_ids;
    for (uint32 cluster_index = 0; cluster_index < clusters.size();
        ++cluster_index) {
      const BoundingBoxCluster& cluster = clusters[cluster_index];
      uint32 new_vertex_id = AddVertex(image_id, cluster.center,
          ImageRegionGraph_Vertex_VertexType_CLUSTER);
      cluster_region_vertex_ids.push_back(new_vertex_id);
      //add edges from new cluster region vertex to each related region (region in clusteR)
      for (uint32 i = 0; i < cluster.members.size(); ++i) {
        AddEdgeWithOverlapWeight(new_vertex_id, cluster.members[i]);
      }
    }

    // add overlap links between overlapping region clusters
    uint32 num_cluster_regions = cluster_region_vertex_ids.size();
    for (uint32 i = 0; i < num_cluster_regions; ++i) {
      uint32 vertex_id_a = cluster_region_vertex_ids[i];
      for (uint32 j = i + 1; j < num_cluster_regions; ++j) {
        uint32 vertex_id_b = cluster_region_vertex_ids[j];
        AddEdgeWithOverlapWeight(vertex_id_a, vertex_id_b);
      }
    }

    images_processed++;
    //double p = double(images_processed) / num_images;
    //LOG_EVERY_N(INFO, 100) << "progress: " << p;
    progress_bar.Increment();
  }
}

// Adds a link between a GT region and all cluster regions that overlap it
void ImageRegionGraphBuilder::AddGroundTruthRegionLinks(
    const std::string& tide_uri) {
  LOG(INFO) << "AddGroundTruthRegionLinks";
  pert::StringTableReader tide_reader;
  CHECK(tide_reader.Open(tide_uri)) << "failed to open uri: " << tide_uri;

  string key, value;
  tide::Object object;
  progress::ProgressBar<uint32> progress_bar(tide_reader.Entries());
  while (tide_reader.Next(&key, &value)) {
    CHECK(object.ParseFromString(value));
    progress_bar.Increment();
    for (int i = 0; i < object.photos_size(); ++i) {
      const tide::Photo& photo = object.photos(i);
      if (photo.label() == tide::POSITIVE) {
        // get list of all cluster regions in this image
        const std::vector<uint32>& cluster_regions =
            imageid_to_cluster_vertices_[photo.id()];

        // modify all cluster regions to CLUSTER_NONOVERLAP_GROUND_TRUTH type
        BOOST_FOREACH(uint32 r, cluster_regions) {
          irg_->mutable_vertex(r)->set_type(ImageRegionGraph_Vertex_VertexType_CLUSTER_NONOVERLAP_GROUND_TRUTH);
        }

        // for each GT region
        BOOST_FOREACH(const tide::BoundingBox& tide_bb, photo.regions()) {
          BoundingBox bb;
          bb.set_x1(tide_bb.x1());
          bb.set_y1(tide_bb.y1());
          bb.set_x2(tide_bb.x2());
          bb.set_y2(tide_bb.y2());


          //for (int j=0; j < photo.regions_size(); ++j){
          //const BoundingBox& bb = photo.regions(j);
          // Add it to BBIRG
          uint32 new_gt_vertex = AddVertex(photo.id(), bb, ImageRegionGraph_Vertex_VertexType_GROUND_TRUTH);
          BOOST_FOREACH(uint32 cluster_vertex, cluster_regions) {
            double overlap = OverlapWeight(
                irg_->vertex(cluster_vertex).bounding_box(), bb);
            if (overlap > 0) {
              AddEdge(new_gt_vertex, cluster_vertex, overlap);
              // mark the cluster region as overlapping at least one GT region
              irg_->mutable_vertex(cluster_vertex)->set_type(
                  ImageRegionGraph_Vertex_VertexType_CLUSTER_OVERLAP_GROUND_TRUTH);
            }
          }
        }
      }
    }
  }
}


bool CreateAndSaveImageRegionGraph(const std::string& matches_uri, const std::string& tide_uri, const std::string& output_uri){
  iw::ImageRegionGraph irg;
  if (!CreateImageRegionGraph(matches_uri, tide_uri, &irg)){
    return false;
  }
  SaveImageRegionGraph(irg, output_uri);
  return true;
}

} // close namespace iw
