#pragma once
#include "iw/iw.pb.h"

namespace iw {

bool CreateImageRegionGraph(const std::string& matches_uri, const std::string& tide_uri, iw::ImageRegionGraph* irg);
void SaveImageRegionGraph(const iw::ImageRegionGraph& irg, const std::string& output_uri);
bool LoadImageRegionGraph(const std::string& input_uri, iw::ImageRegionGraph* irg);


bool CreateAndSaveImageRegionGraph(const std::string& matches_uri, const std::string& tide_uri, const std::string& output_uri);

}
