#include "iw/matching/visutil.h"
#include "snap/google/base/stringprintf.h"
#include "snap/google/glog/logging.h"
#include "snap/base64/encode.h"
#include "snap/opencv2/highgui/highgui.hpp"
#include <fstream>
#include <vector>
#include <iterator>

using namespace std;
using namespace iw;

#define keypoint_scale 3.0f

void LoadJpegImageOrDie(string filename, JpegImage* image){
  CHECK(image);
  ifstream ifs(filename.c_str(), std::ios::binary);
  CHECK(ifs) << "can't open file: " << filename;
  vector<char> data((std::istreambuf_iterator<char>(ifs)), std::istreambuf_iterator<char>());
  cv::Mat img = cv::imdecode(data, 1);
  CHECK(!img.empty()) << "can't read image in file: " << filename;
  image->set_data(data.data(), data.size());
  image->set_width(img.cols);
  image->set_height(img.rows);
}

void SaveOrDie(const std::string& data, const std::string& filename){
  ofstream ofs(filename.c_str(), std::ios::binary);
  CHECK(ofs) << "can't open file for writing: " << filename;
  ofs.write(data.data(), data.size());
}


string Base64Encode(const string& in){
  std::istringstream in_stream(in);
  std::ostringstream out_stream;
  base64::encoder encoder;
  encoder.encode(in_stream, out_stream);
  return out_stream.str();
}

string ToJpegDataUri(const string& jpeg_data){
  string data_uri = "data:image/jpeg;base64,";
  data_uri += Base64Encode(jpeg_data);
  return data_uri;
}

std::string RenderCorrespondencesSVG(const JpegImage& image_a,
                                     const JpegImage& image_b,
                                     const iw::Correspondences& correspondences) {
  std::string out;
  int image2_offset_x = image_a.width();
  int image2_offset_y = 0;

  out += "<?xml version=\"1.0\" standalone=\"no\"?>\n";
  out += "<!DOCTYPE svg PUBLIC \"-//W3C//DTD SVG 1.1//EN\" \n";
  out += "\"http://www.w3.org/Graphics/SVG/1.1/DTD/svg11.dtd\">\n";
  out += "<svg width=\"100%\" height=\"100%\" version=\"1.1\" xmlns=\"http://www.w3.org/2000/svg\" xmlns:xlink=\"http://www.w3.org/1999/xlink\">\n";
  //out += "<circle cx=\"100\" cy=\"50\" r=\"40\" stroke=\"black\" stroke-width=\"2\" fill=\"red\"/>\n";
  out += "<g transform=\"scale(1.0)\" >";
  out += StringPrintf("<image x=\"%dpx\" y=\"%dpx\" width=\"%dpx\" height=\"%dpx\" xlink:href=\"%s\"> </image> \n", 0, 0, image_a.width(), image_a.height(), ToJpegDataUri(image_a.data()).c_str());
  out += StringPrintf("<image x=\"%dpx\" y=\"%dpx\" width=\"%dpx\" height=\"%dpx\" xlink:href=\"%s\"> </image> \n", image2_offset_x, image2_offset_y, image_b.width(), image_b.height(), ToJpegDataUri(image_b.data()).c_str());


  string color = "red";
  for (int index = 0; index < correspondences.size(); ++index){
    const Correspondence& c = correspondences.Get(index);
    float a_x = c.a().pos().x();
    float a_y = c.a().pos().y();
    float b_x = c.b().pos().x();
    float b_y = c.b().pos().y();

    std::string left_pt_id = StringPrintf("lp%d", index);
    std::string right_pt_id = StringPrintf("rp%d",index);

    out += StringPrintf("<circle id=\"%s\" cx=\"%f\" cy=\"%f\" r=\"3\" stroke=\"black\" stroke-width=\"0\" fill=\"%s\"/>\n", left_pt_id.c_str(), a_x, a_y, color.c_str());
    out += StringPrintf("<circle id=\"%s\" cx=\"%f\" cy=\"%f\" r=\"3\" stroke=\"black\" stroke-width=\"0\" fill=\"%s\"/>\n", right_pt_id.c_str(), b_x+image2_offset_x, b_y+image2_offset_y, color.c_str());

    out += StringPrintf("<circle id=\"%s_support\" cx=\"%f\" cy=\"%f\" r=\"%f\" stroke-width=\"1\" fill=\"none\" opacity=\"0.5\" stroke=\"yellow\">\n", left_pt_id.c_str(), a_x, a_y, c.a().radius(), color.c_str());
    out += StringPrintf("<set attributeName=\"opacity\" from=\"0.5\" to=\"1.0\" begin=\"%s.mouseover\" end=\"%s.mouseout\"/>", left_pt_id.c_str(), left_pt_id.c_str());
    out += StringPrintf("<set attributeName=\"opacity\" from=\"0.5\" to=\"1.0\" begin=\"%s.mouseover\" end=\"%s.mouseout\"/>", right_pt_id.c_str(), right_pt_id.c_str());
    out += "</circle>";

    out += StringPrintf("<circle id=\"%s_support\" cx=\"%f\" cy=\"%f\" r=\"%f\" stroke-width=\"1\" fill=\"none\" opacity=\"0.5\" stroke=\"yellow\">\n", right_pt_id.c_str(), b_x+image2_offset_x, b_y+image2_offset_y, c.b().radius(), color.c_str());
    out += StringPrintf("<set attributeName=\"opacity\" from=\"0.5\" to=\"1.0\" begin=\"%s.mouseover\" end=\"%s.mouseout\"/>", left_pt_id.c_str(), left_pt_id.c_str());
    out += StringPrintf("<set attributeName=\"opacity\" from=\"0.5\" to=\"1.0\" begin=\"%s.mouseover\" end=\"%s.mouseout\"/>", right_pt_id.c_str(), right_pt_id.c_str());
    out += "</circle>";

    out += StringPrintf("<line x1=\"%f\" y1=\"%f\" x2=\"%f\" y2=\"%f\" style=\"stroke:rgb(255,0,0);stroke-width:2\" visibility=\"hidden\">", a_x, a_y, b_x+image2_offset_x, b_y+image2_offset_y);
    out += StringPrintf("<set attributeName=\"visibility\" from=\"hidden\" to=\"visible\" begin=\"%s.mouseover\" end=\"%s.mouseout\"/>", left_pt_id.c_str(), left_pt_id.c_str());
    out += StringPrintf("<set attributeName=\"visibility\" from=\"hidden\" to=\"visible\" begin=\"%s.mouseover\" end=\"%s.mouseout\"/>", right_pt_id.c_str(), right_pt_id.c_str());
    out += "</line>";
  }


  out += "</g>";
  out += "</svg>";

  return out;
}

std::string RenderGeometricMatchesSVG(const iw::JpegImage& image_a,
                                      const iw::JpegImage& image_b,
                                      const iw::GeometricMatches& matches) {
  std::string out;
  int image2_offset_x = image_a.width();
  int image2_offset_y = 0;

  int svg_width = (image_a.width() + image_b.width())/2.0;
  int svg_height = std::max(image_a.height(), image_b.height())/2.0;

  out += "<?xml version=\"1.0\" standalone=\"no\"?>\n";
  out += "<!DOCTYPE svg PUBLIC \"-//W3C//DTD SVG 1.1//EN\" \n";
  out += "\"http://www.w3.org/Graphics/SVG/1.1/DTD/svg11.dtd\">\n";
  out += StringPrintf("<svg width=\"%d\" height=\"%d\" version=\"1.1\" xmlns=\"http://www.w3.org/2000/svg\" xmlns:xlink=\"http://www.w3.org/1999/xlink\">\n", svg_width, svg_height);
  //out += "<circle cx=\"100\" cy=\"50\" r=\"40\" stroke=\"black\" stroke-width=\"2\" fill=\"red\"/>\n";
  out += "<g transform=\"scale(0.5)\" >";
  out += StringPrintf("<image x=\"%dpx\" y=\"%dpx\" width=\"%dpx\" height=\"%dpx\" xlink:href=\"%s\"> </image> \n", 0, 0, image_a.width(), image_a.height(), ToJpegDataUri(image_a.data()).c_str());
  out += StringPrintf("<image x=\"%dpx\" y=\"%dpx\" width=\"%dpx\" height=\"%dpx\" xlink:href=\"%s\"> </image> \n", image2_offset_x, image2_offset_y, image_b.width(), image_b.height(), ToJpegDataUri(image_b.data()).c_str());


  string color = "red";
  for (int geo_match_index = 0 ; geo_match_index < matches.entries_size(); ++geo_match_index){
    const GeometricMatch& match = matches.entries(geo_match_index);
    for (int index = 0; index < match.correspondences_size(); ++index){
      const Correspondence& c = match.correspondences(index);
      float a_x = c.a().pos().x();
      float a_y = c.a().pos().y();
      float b_x = c.b().pos().x();
      float b_y = c.b().pos().y();

      std::string left_pt_id = StringPrintf("lp%d", index);
      std::string right_pt_id = StringPrintf("rp%d",index);

      out += StringPrintf("<circle id=\"%s\" cx=\"%f\" cy=\"%f\" r=\"3\" stroke=\"black\" stroke-width=\"0\" fill=\"%s\"/>\n", left_pt_id.c_str(), a_x, a_y, color.c_str());
      out += StringPrintf("<circle id=\"%s\" cx=\"%f\" cy=\"%f\" r=\"3\" stroke=\"black\" stroke-width=\"0\" fill=\"%s\"/>\n", right_pt_id.c_str(), b_x+image2_offset_x, b_y+image2_offset_y, color.c_str());

      out += StringPrintf("<circle id=\"%s_support\" cx=\"%f\" cy=\"%f\" r=\"%f\" stroke-width=\"5\" fill=\"none\" opacity=\"0.5\" stroke=\"yellow\" >\n", left_pt_id.c_str(), a_x, a_y, c.a().radius(), color.c_str());
      out += StringPrintf("<set attributeName=\"opacity\" from=\"0.5\" to=\"1.0\" begin=\"%s.mouseover\" end=\"%s.mouseout\"/>", left_pt_id.c_str(), left_pt_id.c_str());
      out += StringPrintf("<set attributeName=\"opacity\" from=\"0.5\" to=\"1.0\" begin=\"%s.mouseover\" end=\"%s.mouseout\"/>", right_pt_id.c_str(), right_pt_id.c_str());
      out += "</circle>";

      out += StringPrintf("<circle id=\"%s_support\" cx=\"%f\" cy=\"%f\" r=\"%f\" stroke-width=\"5\" fill=\"none\" opacity=\"0.5\" stroke=\"yellow\" >\n", right_pt_id.c_str(), b_x+image2_offset_x, b_y+image2_offset_y, c.b().radius(), color.c_str());
      out += StringPrintf("<set attributeName=\"opacity\" from=\"0.5\" to=\"1.0\" begin=\"%s.mouseover\" end=\"%s.mouseout\"/>", left_pt_id.c_str(), left_pt_id.c_str());
      out += StringPrintf("<set attributeName=\"opacity\" from=\"0.5\" to=\"1.0\" begin=\"%s.mouseover\" end=\"%s.mouseout\"/>", right_pt_id.c_str(), right_pt_id.c_str());
      out += "</circle>";

      out += StringPrintf("<line x1=\"%f\" y1=\"%f\" x2=\"%f\" y2=\"%f\" style=\"stroke:rgb(255,0,0);stroke-width:2\" visibility=\"hidden\">", a_x, a_y, b_x+image2_offset_x, b_y+image2_offset_y);
      out += StringPrintf("<set attributeName=\"visibility\" from=\"hidden\" to=\"visible\" begin=\"%s.mouseover\" end=\"%s.mouseout\"/>", left_pt_id.c_str(), left_pt_id.c_str());
      out += StringPrintf("<set attributeName=\"visibility\" from=\"hidden\" to=\"visible\" begin=\"%s.mouseover\" end=\"%s.mouseout\"/>", right_pt_id.c_str(), right_pt_id.c_str());
      out += "</line>";
    }
  }

  out += "</g>";
  out += "</svg>";

  return out;

}

std::string RenderGeometricMatchesHTML(const iw::JpegImage& image_a,
                                      const iw::JpegImage& image_b,
                                      const iw::GeometricMatches& matches){
  std::string out;
  out += "<html><body>";
  out += RenderGeometricMatchesSVG(image_a, image_b, matches);
  out += "<br>";
  if (matches.entries_size()){
    const iw::GeometricMatch& match =  matches.entries(0);
    out += StringPrintf("<h1>nfa=%f num_matches=%d precision=%f</h1>", match.nfa(), match.correspondences_size(), match.precision());
  }
  out += "</body></html>";
  return out;
}
