// Copyright (c) 2013 Kyle Heath
// 20130213 - Renamed from ezProgressBar to progress, changed api, fixed bugs

// Copyright (C) 2011,2012 Remik Ziemlinski. See MIT-LICENSE.
//
// CHANGELOG
//
// v0.0.0 20110502 rsz Created.
// V1.0.0 20110522 rsz Extended to show eta with growing bar.
// v2.0.0 20110525 rsz Added time elapsed.
// v2.0.1 20111006 rsz Added default constructor value.

#pragma once

#include <iostream>
#include <stdio.h>
#include <sys/time.h> // for gettimeofday
#include <algorithm> // for std::max
#include <string>
#include <assert.h>

namespace progress {

#define SECONDS_TO_MICROSECONDS 1e6
#define MICROSECONDS_TO_SECONDS 1e-6

// One-line refreshing progress bar inspired by wget that shows ETA (time remaining).
// Example output:
// 90% [##################################################     ] ETA 12d 23h 56s
//
// The display will be updated at most once a second or up to max_refreshes
// times.  This guarantee makes it safe to use a progress bar for large
// operations where console IO on every item would slow computation.
template <typename Count>
class ProgressBar {

public:
  ProgressBar(Count task_complete_count, int max_refreshes = 1000, int max_refresh_per_second = 4) :
  task_complete_count_(task_complete_count),
  count_(0),
  next_refresh_count_(0),
  start_time_us_(0),
  last_refresh_time_us_(0),
  min_refresh_period_us_((1.0/max_refresh_per_second)*SECONDS_TO_MICROSECONDS),
  terminal_width_(80),
  max_refreshes_(max_refreshes) {
    Start();
	}
	
	void Increment(Count count_increment = Count(1)) {
	  assert(count_increment >= 0);
	  count_ += count_increment;
		Update(count_);
	};

	void Update(Count new_count) {
	  if (new_count < next_refresh_count_ || new_count > task_complete_count_){
	    return;
	  }
	  count_ = new_count;
	  next_refresh_count_ = std::min(count_ + refresh_increment_, task_complete_count_);
    RefreshDisplay();
	}

private:
	void Start() {
    start_time_us_ = GetTimeMicroseconds();
    refresh_increment_ = std::max(task_complete_count_/max_refreshes_, Count(1));
    next_refresh_count_ = 0;
    RefreshDisplay();
	}

	long long GetTimeMicroseconds() {
    timeval time;
    gettimeofday(&time, NULL);
    return time.tv_sec * 1000000ll + time.tv_usec;
  }



	std::string PrettyPrintDuration(double duration_seconds) {
	  const int sec_per_day = 86400;
	  const int sec_per_hour = 3600;
	  const int sec_per_min = 60;
	  int remaining_seconds = duration_seconds;
	  int days = remaining_seconds/sec_per_day;
	  remaining_seconds -= days*sec_per_day;
		int hours = remaining_seconds/sec_per_hour;
		remaining_seconds -= hours*sec_per_hour;
		int mins = remaining_seconds/sec_per_min;
		remaining_seconds -= mins*sec_per_min;
		char buf[8];
		std::string out;
		if (days >= 7){
		  out = "> 1 week";
		}
		else{
      if (days) {
        snprintf(buf, sizeof(buf), "%dd ", days);
        out += buf;
      }
      if (hours >= 1) {
        snprintf(buf, sizeof(buf), "%dh ", hours);
        out += buf;
      }
      if (mins >= 1) {
        snprintf(buf, sizeof(buf), "%dm ", mins);
        out += buf;
      }
      snprintf(buf, sizeof(buf), "%ds", remaining_seconds);
      out += buf;
		}
		return out;
	}
	
	// Update the display
	void RefreshDisplay() {
	  long long cur_time_us = GetTimeMicroseconds();
	  // don't allow refresh rate faster than once a second, unless we reach 100%
	  long long time_since_last_refresh_us = cur_time_us - last_refresh_time_us_;
	  if (time_since_last_refresh_us < min_refresh_period_us_ && count_ < task_complete_count_  ){
	    return;
	  }
    last_refresh_time_us_ = cur_time_us;
	  float fraction_done = float(count_)/task_complete_count_;
		char buf[5];
		snprintf(buf, sizeof(buf), "%3.0f%%", fraction_done*100.0);

		// Compute how many tics to display.
		int num_tics_max = terminal_width_ - 27;
		int num_tics = num_tics_max*fraction_done;
		std::string out(buf);
		out.append(" [");
		out.append(num_tics,'#');
		out.append(num_tics_max-num_tics,' ');
		out.append("] ");

		double elapsed_time = (cur_time_us - start_time_us_)*MICROSECONDS_TO_SECONDS;
		bool is_done = (fraction_done >= 1.0);

		if (is_done) {
		  // Print overall time
		  out.append("in ");
			out.append(PrettyPrintDuration(elapsed_time));
		}
		else {
		  // Print estimated remaining time
		  out.append("ETA ");
			if (fraction_done > 0.0){
			  double remaining_time = elapsed_time*(1.0-fraction_done)/fraction_done;
			  out.append(PrettyPrintDuration(remaining_time));
			}
		}

		// Pad with spaces to fill terminal width.
    if (out.size() < terminal_width_){
      out.append(terminal_width_-out.size(),' ');
      out.append("\r");
    }

    if (is_done){
      out.append("\n");
      out.append("\n");
    }

		std::cout << out;
		fflush(stdout);
	}

	Count task_complete_count_;
	Count count_;
	Count next_refresh_count_;
	Count refresh_increment_;
	long long start_time_us_;
	long long last_refresh_time_us_;
	long long min_refresh_period_us_;
	int terminal_width_;
  int max_refreshes_;
};



/*
//Display progress on two lines with rate update.
//
//Done |  Elapsed | Remaining | Processed | Unprocessed | Rate
// 90% | 23:45:56 |  23:45:56 |      1 MB |    1,299 MB | 124 MB/s
template <typename T>
class ProgressRate {
public:
  RateProgressBar(T _n=0, std::string units = "") : task_complete_count_(_n), count_(0), done(0), pct(0), terminal_width_(0), unitsWidth(0), units(units) {}
  void reset() { pct = 0; cur = 0; done = 0; }
  void start() {
    #ifdef WIN32
      assert(QueryPerformanceFrequency(&g_llFrequency) != 0);
    #endif
    startTime = osQueryPerfomance();
    prev_time_ = startTime;
    printHeader();
  }

  void commaNumber(T v, std::string & str) {
    std::ostringstream stm;
    if (v >= 1) {
      stm << (long long)v;
      str = stm.str();
      for(int i=str.size()-3; i>0; i-=3) str.insert(i, 1, ',');
    } else {
      stm << std::fixed << std::setprecision(3) << v;
      str = stm.str();
    }
  }

  // http://stackoverflow.com/questions/3283804/c-get-milliseconds-since-some-date
  long long osQueryPerfomance() {
  #ifdef WIN32
    LARGE_INTEGER llPerf = {0};
    QueryPerformanceCounter(&llPerf);
    return llPerf.QuadPart * 1000ll / ( g_llFrequency.QuadPart / 1000ll);
  #else
    struct timeval stTimeVal;
    gettimeofday(&stTimeVal, NULL);
    return stTimeVal.tv_sec * 1000000ll + stTimeVal.tv_usec;
  #endif
  }

  void printHeader() {
    //Done |  Elapsed | Remaining | Processed |  Unprocessed | Rate
    std::string out;
    out.reserve(80);
    out.assign("Done |  Elapsed | Remaining | ");
    // Compute how many max characters 'n' would take when formatted.
    std::string str;
    commaNumber(n, str);
    //int i;
    // Width of "123,456".
    int nWidth = str.size();
    unitsWidth = units.size();
    // Width of "123,456 MB".
    int nUnitsWidth = nWidth + unitsWidth + 1;
    // "Processed" is 9 chars.
    if (nUnitsWidth > 9)
      procColWidth = nUnitsWidth;
    else
      procColWidth = 9;

    if (nUnitsWidth > 11)
      unprocColWidth = nUnitsWidth;
    else
      unprocColWidth = 11;

    out.append(procColWidth-9, ' ');
    out.append("Processed | ");
    out.append(unprocColWidth-11, ' ');
    out.append("Unprocessed | Rate\n");

    printf("%s", out.c_str());
  }

  void secondsToString(unsigned int t, std::string & out) {
    unsigned int days = t/86400;
    unsigned int sec = t-days*86400;
    unsigned int hours = sec/3600+days*24;
    sec = t - hours*3600;
    sec = (sec > 0 ? sec : 0);
    unsigned int mins = sec/60;
    sec -= mins*60;
    char tmp[8];
    out.clear();
    sprintf(tmp, "%02u:", hours);
    out.append(tmp);

    sprintf(tmp, "%02u:", mins);
    out.append(tmp);

    sprintf(tmp, "%02u", sec);
    out.append(tmp);
  }

  void operator++() {
    update( cur++ );
  };

  T operator+=(const T delta) {
    cur += delta;
    update( cur );
    return cur;
  };

  void update(T newvalue) {
    // Nothing to Update if already maxed out.
    if (done) return;
    endTime = osQueryPerfomance();

    // Abort if at least 1 second didn't elapse, unless newvalue will get us to 100%.
    if ( ((endTime-prev_time_)/1000000.0 < 1.0) && (newvalue < n) ) return;
    prev_time_ = endTime;
    float dt = (endTime-startTime)/1000000.0;
    //if (dt < 1) return; // Was meant to avoid division by zero when time was in whole numbers.
    cur = newvalue;
    char pctstr[8];
    float Pct = ((double)cur)/n;
    sprintf(pctstr, "%3d%%", (int)(100*Pct));
    std::string out;
    out.reserve(80);
    out.append(pctstr);
    // Seconds.
    std::string tstr;
    out.append(" | ");
    secondsToString( (unsigned int)dt, tstr);
    out.append(tstr);
    int pad, newwidth;
    if (Pct >= 1.0) {
      // Print overall time and newline.
      out.append(" |  00:00:00 | ");
      commaNumber(n, tstr);
      pad = procColWidth-tstr.size()-unitsWidth-1;
      if (pad > 0) out.append(pad, ' ');
      out.append(tstr);
      out.append(" ");
      out.append(units);
      out.append(" | ");
      pad = unprocColWidth-1-unitsWidth-1;
      if (pad > 0) out.append(pad, ' ');
      out.append("0 ");
      out.append(units);
      out.append(" | ");
      commaNumber( (unsigned int)(n/dt), tstr);
      out.append(tstr);
      out.append(" ");
      out.append(units);
      out.append("/s");

      newwidth = out.size();
      if (newwidth < terminal_width_)
        out.append(terminal_width_-newwidth,' ');

      // Store length of this string so we know how much to blank out later.
      terminal_width_ = newwidth;
      out.append("\n");
      printf("%s", out.c_str());
      fflush(stdout);
      done = 1;
    } else {
      float eta=0.;
      if (Pct > 0.0) {
        eta = dt*(1.0-Pct)/Pct;
      }
      out.append(" |  ");
      secondsToString((unsigned int)eta, tstr);
      out.append(tstr);
      out.append(" | ");
      commaNumber(cur, tstr);
      pad = procColWidth-tstr.size()-unitsWidth-1;
      if (pad > 0) out.append(pad,' ');
      out.append(tstr);
      out.append(" ");
      out.append(units);
      out.append(" | ");
      commaNumber(n-cur, tstr);
      pad = unprocColWidth-tstr.size()-unitsWidth-1;
      if (pad > 0) out.append(pad,' ');
      out.append(tstr);
      out.append(" ");
      out.append(units);
      out.append(" | ");
      eta = cur/dt;
      if (eta > 1.0)
        commaNumber((unsigned int)eta, tstr);
      else {
        std::ostringstream stm;
        stm << eta;
        tstr = stm.str();
      }
      out.append(tstr);
      out.append(" ");
      out.append(units);
      out.append("/s");
      // Pad end with spaces to overwrite previous string that may have been longer.
      newwidth = out.size();
      if (newwidth < terminal_width_)
        out.append(terminal_width_-newwidth,' ');

      terminal_width_ = newwidth;
      out.append("\r");
      printf("%s", out.c_str());
      fflush(stdout);
    }
  }

private:

  T n;
  T cur;
  char done;
  unsigned short pct; // Stored as 0-1000, so 2.5% is encoded as 25.
  unsigned char terminal_width_; // Length of previously printed line we need to overwrite.
  unsigned int unitsWidth;
  unsigned int procColWidth;
  unsigned int unprocColWidth;
  std::string units; // Unit string to show for processed data.
  long long startTime, prev_time_, endTime;
  #ifdef WIN32
    LARGE_INTEGER g_llFrequency;
  #endif
};
*/

} // close namespace progress

