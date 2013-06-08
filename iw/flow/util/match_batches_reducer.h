#pragma once

//#include "hadoop/Pipes.hh"
#include "deluge/deluge.h"
#include "snap/google/glog/logging.h"
#include "snap/google/base/hashutils.h"
#include "iw/iw.pb.h"
#include "iw/matching/image_matcher.h"
#include "boost/timer/timer.hpp"

using boost::timer::cpu_timer;
using namespace std;

using iw::ImageFeatures;
using iw::MatchBatchMetadata;
using iw::GeometricMatchResult;
using iw::GeometricMatches;


class MatchBatchesReducer : public HadoopPipes::Reducer {
public:
  HadoopPipes::TaskContext::Counter* match_success_counter_;
  HadoopPipes::TaskContext::Counter* match_fail_counter_;
  HadoopPipes::TaskContext::Counter* match_attempt_counter_;
  HadoopPipes::TaskContext::Counter* match_time_counter_;
  HadoopPipes::TaskContext::Counter* match_batch_counter_;

  MatchBatchesReducer(HadoopPipes::TaskContext& context) : counters_(context, "iw") {
    JobInfo conf(context);
    match_success_counter_ = context.getCounter("iw", "match_success_counter");
    match_fail_counter_ = context.getCounter("iw", "match_fail_counter");
    match_attempt_counter_ = context.getCounter("iw", "match_attempt_counter");
    match_time_counter_ = context.getCounter("iw", "match_time_counter_millisec");
    match_batch_counter_ = context.getCounter("iw", "match_batch_counter");
    config_ = conf.GetProtoOrDie<iw::ImageMatcherConfig>("image_matcher_config_proto");
    CHECK(config_.IsInitialized());
    CHECK(config_.has_affine_acransac_params());
    matcher_.reset(CreateImageMatcherOrDie(config_));

    match_phase_ = conf.GetIntOrDie("match_phase");
  }

  void reduce(HadoopPipes::ReduceContext& context) {
    match_batch_.clear();
    MatchBatchMetadata metadata;

    const string& batch_name = context.getInputKey();
    while (context.nextValue()) {
      CHECK(metadata.ParseFromString(context.getInputValue()));
      CHECK_EQ(metadata.batch_name(), batch_name);
      CHECK_GT(metadata.batch_name().size(), 0);
      match_batch_.push_back(metadata);
    }

    if (match_batch_.empty()){
      return;
    }

    context.incrementCounter(match_batch_counter_, 1);

    // find primary image in match set
    int primary_image_index = -1;
    int primary_image_count = 0;
    for (uint32 i=0; i < match_batch_.size(); ++i){
      if (match_batch_[i].is_primary()){
        primary_image_index = i;
        primary_image_count++;
      }
    }

    // This could happen if the primary image had no features thus join failed and it's associated metadata record with primary = True never reached this point
    if(primary_image_count != 1){
      LOG(INFO) << "primary_image_count: " << primary_image_count;
      LOG(INFO) << "match_batch size: " << match_batch_.size();

      for (int i=0; i < match_batch_.size(); ++i){
        LOG(INFO) << "index: " << i << " --- " << match_batch_[i].DebugString();
      }

      LOG(INFO) << "*************** no primary candidate for set : " << match_batch_[0].batch_name();
      //LOG(FATAL) << "giving up";
      counters_.Increment("failed_match_batch");
      return;
    }


    const MatchBatchMetadata& primary_image = match_batch_[primary_image_index];

    // match primary image with each secondary image
    for (int i=0; i < match_batch_.size(); ++i){
      if (i == primary_image_index) continue;

      LOG(INFO) << "matching pair " << i << " of " << match_batch_.size();

      const MatchBatchMetadata& secondary_image = match_batch_[i];
      GeometricMatches matches;
      double match_time_seconds;
      matcher_->Run(primary_image.features(), secondary_image.features(), &matches, &match_time_seconds);
      LOG(INFO) << "Done matching pair in " << match_time_seconds << " sec";
      context.incrementCounter(match_time_counter_, match_time_seconds*1000.0);

      GeometricMatchResult match_result;
      match_result.set_image_a_id(primary_image.image_id());
      match_result.set_image_b_id(secondary_image.image_id());
      match_result.mutable_properties()->CopyFrom(secondary_image.properties());
      match_result.mutable_matches()->CopyFrom(matches.entries());
      match_result.set_phase(match_phase_);
      string match_key = Uint64ToKey(primary_image.image_id())+Uint64ToKey(secondary_image.image_id());

      // emit match results
      CHECK(match_result.IsInitialized());
      context.emit(match_key, match_result.SerializeAsString());
      if (match_result.matches_size()){
        context.incrementCounter(match_success_counter_, 1);
      }
      else{
        context.incrementCounter(match_fail_counter_, 1);
      }
      context.incrementCounter(match_attempt_counter_, 1);
    }
  }

private:
  std::vector<MatchBatchMetadata> match_batch_;
  iw::ImageMatcherConfig config_;
  uint32 match_phase_;
  boost::scoped_ptr<iw::ImageMatcher> matcher_;
  CounterGroup counters_;
};


