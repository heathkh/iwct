#pragma once

#include "deluge/pipes/Pipes.hh"
#include "snap/boost/timer/timer.hpp"
#include <map>

namespace deluge {

class CounterGroup {
public:
  CounterGroup(HadoopPipes::TaskContext& context, const std::string& group_name);
  void Increment(const std::string& name, int val = 1);

private:
  HadoopPipes::TaskContext& context_;
  std::string group_name_;

  typedef std::map<std::string, HadoopPipes::TaskContext::Counter*> CounterTable;
  CounterTable counters_;
};

class Timer {
public:
  enum Units {SECONDS, MILLISECONDS};
  Timer(CounterGroup& counter_group, const std::string& name, Units units = SECONDS);
  ~Timer();

private:
  std::string GetTimeUnits();
  size_t GetTimeValue(boost::timer::nanosecond_type nanoseconds);

  CounterGroup& counter_group_;
  std::string name_;
  Units units_;
  boost::timer::cpu_timer timer_;
};


class Profiler {
public:

  Profiler(HadoopPipes::TaskContext& context);
  ~Profiler();

private:

  void GenerateProfile();
  void SleepDumpKill();
  //void HandleTimeoutSignal(int signal_num);
  std::string local_profile_path_;
  std::string profile_output_base_uri_;
  std::string pipes_binary_uri_;
  int profile_timeout_sec_;
  bool enabled_;
};

} // close namespace deluge
