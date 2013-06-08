#include "snap/pert/core/ufs.h"
#include "deluge/debug.h"
#include "deluge/job.h"
#include "deluge/std.h"
#include "snap/google/glog/logging.h"
#include "snap/google/base/stringprintf.h"

#include "snap/boost/filesystem.hpp"
#include "snap/boost/bind.hpp"
#include "snap/boost/function.hpp"
#include "snap/boost/thread.hpp"
#include <iostream>
#include <fstream>
#include <google/profiler.h> // from gperftools
#include <sys/time.h>
#include <signal.h>

namespace fs =  boost::filesystem;
using namespace std;

namespace deluge {

CounterGroup::CounterGroup(HadoopPipes::TaskContext& context, const std::string& group_name) :
context_(context),
group_name_(group_name)
{

}

void CounterGroup::Increment(const std::string& name, int val){
  HadoopPipes::TaskContext::Counter* counter = NULL;
  CounterTable::iterator iter = counters_.find(name);
  if (iter == counters_.end()){
    counter = context_.getCounter(group_name_, name);
    counters_[name] =  counter;
  }
  else{
    counter = iter->second;
  }
  context_.incrementCounter(counter, val);
}


Timer::Timer(CounterGroup& counter_group, const std::string& name, Units units) :
counter_group_(counter_group),
name_(name),
units_(units) {

}

Timer::~Timer(){
  timer_.stop();
  boost::timer::cpu_times elapsed = timer_.elapsed();
  counter_group_.Increment(name_+"_wall_" + GetTimeUnits(), GetTimeValue(elapsed.wall) );
  counter_group_.Increment(name_+"_user_" + GetTimeUnits(), GetTimeValue(elapsed.user) );
  counter_group_.Increment(name_+"_sys_" + GetTimeUnits(), GetTimeValue(elapsed.system) );
  counter_group_.Increment(name_+"_count");
}

std::string Timer::GetTimeUnits(){
  std::string units;
  if (units_ == SECONDS){
	  units = "seconds";
  }
  else if (units_ == MILLISECONDS){
	  units = "milliseconds";
  }
  else{
    LOG(FATAL) << "unknown units: " << units_;
  }
  return units;
}

size_t Timer::GetTimeValue(boost::timer::nanosecond_type nanoseconds){
  size_t value = 0;
  if (units_ == SECONDS){
	  value = nanoseconds*1.0e-9;
  }
  else if (units_ == MILLISECONDS){
	  value = nanoseconds*1.0e-6;
  }
  else{
    LOG(FATAL) << "unknown units: " << units_;
  }
  return value;
}

// Profiler is disabled by default.  Must set key 'profiler' to 'on' to enable.
Profiler::Profiler(HadoopPipes::TaskContext& context) :  profile_timeout_sec_(0), enabled_(false) {
  JobInfo info(context);
  std::string profile_flag;

  pipes_binary_uri_ = info.GetStringOrDie("hadoop.pipes.executable");

  if (info.Get("profiler", &profile_flag)){
    if (profile_flag == "on"){
      enabled_ = true;
    }
    else if (profile_flag == "off"){
      enabled_ = false;
    }
    else{
      LOG(FATAL) << "expected profiler = {'on', 'off'} but got: " << profile_flag;
    }
  }

  if (enabled_){
    string task_string = info.GetStringOrDie("mapred.tip.id");
    std::string output_dir = info.GetStringOrDie("mapred.output.dir");
    output_dir = CleanupMaprPathFormat(output_dir);
    profile_output_base_uri_ = StringPrintf("maprfs:/%s/_PROFILE/%s/", output_dir.c_str(), task_string.c_str());
    local_profile_path_ = "/tmp/" + fs::unique_path().string();
    ProfilerStart(local_profile_path_.c_str());
    LOG(INFO) << "starting profiler";
    LOG(INFO) << "local_profile_path_: " << local_profile_path_;
    LOG(INFO) << "profile_output_base_uri: " << profile_output_base_uri_;


    if (info.Get("profile_timeout_sec", &profile_timeout_sec_)){
      LOG(INFO) << "profile_timeout_sec_: " << profile_timeout_sec_;
      boost::thread test_thread(boost::bind(&Profiler::SleepDumpKill, this));
    }
  };
}


Profiler::~Profiler(){
  if (!enabled_){
    return;
  }
  GenerateProfile();
}


void Profiler::SleepDumpKill(){
  sleep(profile_timeout_sec_);
  GenerateProfile();
  exit(0);
}

void Profiler::GenerateProfile(){
  LOG(INFO) << "GenerateProfile()";

  ProfilerStop();
  LOG(INFO) << "Profiler stopped...";
  ProfilerFlush();
  LOG(INFO) << "Profiler flushed...";

  // Copy to output uri
  CHECK(fs::exists(local_profile_path_));
  CHECK(fs::is_regular_file(local_profile_path_));
  CHECK(pert::CopyLocalToUri(local_profile_path_, profile_output_base_uri_ + "/profile.pprof" ));
  LOG(INFO) << "Profile copied to maprfs: " << profile_output_base_uri_;

  // write a convenience script to make it simple to view profiler output with one command
  std::string scheme, binary_path;
  CHECK(pert::ParseUri(pipes_binary_uri_, &scheme, &binary_path));

  std::string script;
  script += "#!/bin/bash\n";
  script += "MAPR_NFS_PREFIX=/mapr/my.cluster.com\n";
  script += "BINARY_PATH=" + binary_path + "\n";
  script += "pprof --callgrind $MAPR_NFS_PREFIX/$BINARY_PATH profile.pprof > profile.callgrind\n";
  script += "kcachegrind profile.callgrind\n";

  std::string local_script_path = "/tmp/" + fs::unique_path().string();

  std::ofstream script_file(local_script_path.c_str());
  script_file.write(script.data(), script.size());
  script_file.close();
  CHECK(pert::CopyLocalToUri(local_script_path, profile_output_base_uri_ + "/view_profile.sh"));
}


} // close namespace deluge
