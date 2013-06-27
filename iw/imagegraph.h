#pragma once
#include <string>

namespace iw {

// forward declare pointer types instead of including header
class ImageGraph;

float NfaToWeight(float nfa);

bool CreateImageGraph2(const std::string& matches_uri, const std::string& photoid_uri, iw::ImageGraph* ig);
void SaveImageGraph(const iw::ImageGraph& ig, const std::string& output_uri);
bool LoadImageGraph(const std::string& input_uri, iw::ImageGraph* ig);

} // close namespace iw
