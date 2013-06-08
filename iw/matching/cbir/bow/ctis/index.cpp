// Created: Apr 9, 2009
// Author: heathkh

#include <fstream>
#include "iw/matching/cbir/bow/ctis/index.h"
#include "snap/boost/foreach.hpp"
#include "snap/boost/progress.hpp"
#include "snap/google/glog/logging.h"
#include "snap/google/base/hashutils.h"
#include "snap/boost/filesystem.hpp"

namespace fs = boost::filesystem;
using namespace std;

namespace cbir {

//////////////////////////////////////////////////////////////////////////////
// CtisIndex
//////////////////////////////////////////////////////////////////////////////

CtisIndex::CtisIndex() : Index(), ready_(false) {

  weight_ = ivFile::WEIGHT_BIN;
  norm_ = ivFile::NORM_L1;
  dist_ = ivFile::DIST_L1;

}


CtisIndex::~CtisIndex() {
}


bool CtisIndex::Load(const std::string& filebase){

  string ivf_filename =  filebase + "/index.ivf";
  string ids_filename = filebase + "/index.ivfids";

  LOG(INFO) << "Loading inverted file image ids: " << ids_filename;
  image_ids_.clear();

  //write list of image ids to file (map index in inverted file to image id)
  ifstream image_ids_file(ids_filename.c_str(), ios::binary);
  CHECK(image_ids_file) << "can't open filename: " << ids_filename;

  uint32 image_count_;
  CHECK(image_ids_file.readsome((char*)&image_count_, sizeof(image_count_)) == sizeof(image_count_));
  image_ids_.resize(image_count_);
  uint64* image_id_iter = &image_ids_[0];
  for(uint64 i=0; i < image_count_; ++i){
    image_ids_file.read((char*)image_id_iter++, sizeof(uint64));
  }

  LOG(INFO) << "Loading inverted file : " << ivf_filename;
  CHECK(ivf_.load(ivf_filename.c_str())) << "failed to open file: " << ivf_filename;

  cout << "Computing stats..." << endl;
  ivf_.computeStats(weight_, norm_);

  CHECK_EQ(image_count_, Size());

  ready_ = true;

  return true;
}

bool CtisIndex::Save(const std::string& filebase){

  string ivf_filename =  filebase + "/index.ivf";
  string ids_filename = filebase + "/index.ivfids";

  // construct parent dir if needed
  std::string parent_path = fs::path(ids_filename).parent_path().string();
  if (not fs::exists(parent_path)){
    CHECK(fs::create_directories(parent_path));
  }

  //write list of image ids to file (map index in inverted file to image id)
  ofstream image_ids_file(ids_filename.c_str(), ios::binary);
  CHECK(image_ids_file) << "can't write to file: " << ids_filename;

  uint32 image_count = image_ids_.size();
  image_ids_file.write((char*)&image_count, sizeof(image_count));
  BOOST_FOREACH(uint64 image_id, image_ids_){
    image_ids_file.write((char*)&image_id, sizeof(uint64));
  }

  ivf_.save(ivf_filename);

  return true;
}

// Creates an index for num_docs images with given visual vocab size
void CtisIndex::StartCreate(uint32 visual_vocab_size, uint32 num_docs){
  ivf_.nwords = visual_vocab_size;
  ivf_.ndocs = num_docs;
  ivf_.words.resize(ivf_.nwords);
  ivf_.docs.resize(ivf_.ndocs);
}


bool CtisIndex::Add(uint64 image_id, const BagOfWords& bow ){
  CHECK_GT(ivf_.ndocs, 0) << "you must call CtisIndex::StartCreate before CtisIndex::Add()!";
  uint32 dl = image_ids_.size();
  image_ids_.push_back(image_id);

  CHECK_LE(image_ids_.size(), ivf_.ndocs) << "You added more docs than requested with Create";

  for (int i=0; i < bow.word_id_size(); ++i) {
    uint32 wl = bow.word_id(i);
    CHECK_LT(wl, ivf_.nwords);
    ivWord* w = &ivf_.words[wl]; // get word
    w->wf++; //increment word count
    ivWordDoc newdoc; //make a new wordDoc
    newdoc.doc = dl;
    //find position of this document id in the list
    ivWordDocIt wdit = lower_bound (w->docs.begin(), w->docs.end(), newdoc);

    //check if not found, then add it in the correct place
    if (wdit == w->docs.end() || newdoc<(*wdit)){
      wdit = w->docs.insert(wdit, newdoc);
      w->ndocs++;
    }
    wdit->count++;  //increment word count for this word's doc
    ivf_.docs[dl].ntokens++; //increment document count of tokens
  }


  return true;
}

void CtisIndex::EndCreate(){
  ivf_.computeStats(weight_, norm_);
  ready_ = true;
}

int CtisIndex::Size(){
  return ivf_.ndocs;
}



bool CtisIndex::Query(uint64 query_image_id, const BagOfWords& bag_of_words, int max_count, QueryResults* results){
  //CHECK(ready_) << "Must call Load() or use StartCreate... Add... EndCreate first";

  if (!ready_){
    EndCreate();
  }

  CHECK(results);
  ivNodeList scorelist;
  bool overlap_only = true;
  uint32* data = (uint32*)bag_of_words.word_id().data(); // TODO.. casting away const is bad... need to add const version of templates to CTIS
  ivSearchFileCustom(ivf_, data, bag_of_words.word_id_size(),
               weight_, norm_, dist_, overlap_only, max_count+1, scorelist);

  results->Clear();

  BOOST_FOREACH(const ivNode& node, scorelist){
    uint64 image_id = image_ids_[node.id];
    if (query_image_id == image_id){
      continue;
    }

    QueryResult* r = results->add_entries();
    r->set_image_id(image_id);
    r->set_score(node.val);
  }

  return true;
}


} // close namespace
