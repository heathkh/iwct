#pragma once
#include "iw/iw.pb.h"
#include "iw/matching/acransac_correspondences.h"
#include <string>


void LoadJpegImageOrDie(std::string filename, iw::JpegImage* image);

void SaveOrDie(const std::string& data, const std::string& filename);

std::string RenderCorrespondencesSVG(const iw::JpegImage& image_a,
                                     const iw::JpegImage& image_b,
                                     const iw::Correspondences& correspondences);

std::string RenderGeometricMatchesSVG(const iw::JpegImage& image_a,
                                      const iw::JpegImage& image_b,
                                      const iw::GeometricMatches& matches);

std::string RenderGeometricMatchesHTML(const iw::JpegImage& image_a,
                                      const iw::JpegImage& image_b,
                                      const iw::GeometricMatches& matches);

