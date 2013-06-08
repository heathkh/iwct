#include "iw/matching/vgg/vgg_feature_extractor.h"
#include "snap/opencv2/highgui/highgui.hpp"
#include "snap/google/glog/logging.h"
#include "snap/boost/format.hpp"
#include "snap/google/base/stringprintf.h"
#include "snap/boost/filesystem.hpp"
#include "snap/google/glog/logging.h"
#include <sys/stat.h> // for chmod
#include <fstream>

namespace fs = boost::filesystem;

namespace iw {

VGGFeatureExtractor::VGGFeatureExtractor(FeatureType type, double threshold) {
  CHECK_GE(threshold, 0);
  if (type == HESSIAN){
    feature_flag_ = "-hesaff";
    threshold_flag_ = StringPrintf("-hesThres %f", threshold);
  }
  else if (type == HARRIS){
    feature_flag_ = "-haraff";
    threshold_flag_ = StringPrintf("-harThres %f", threshold);
  }
  else{
    LOG(FATAL) << "Invalid feature type";
  }

  // Create a unique work dir for thread safety
  char tmp_dir_template[] = "/tmp/KMFeatureExtractor_XXXXXX";
  CHECK_NOTNULL(mkdtemp(tmp_dir_template));
  temp_dir_ = tmp_dir_template;
  fs::create_directory(temp_dir_);
  bin_name_ = "/tmp/extract_features_64bit.ln";
  // Extract the VGG binary if needed
  //TODO(heathkh): There is a race condition here... multiple processes may be reading the file before it is completed... these processes will die... retries will succeed... ugly but it works for now.
  if (!fs::exists(bin_name_)){
    LOG(ERROR) << "downloading binary!!!";
    CHECK_EQ(system("cd /tmp; curl -O http://graphics.stanford.edu/~heathkh/webscratch/extract_features_64bit.ln"), 0); // Verify vgg binary works
    LOG(ERROR) << "changing permissions...";
    chmod(bin_name_.c_str(), S_IRUSR | S_IWUSR |  S_IXUSR | S_IRGRP | S_IWGRP | S_IXGRP | S_IROTH | S_IWOTH | S_IXOTH); // make it executable
    LOG(ERROR) << "testing binary...";
    CHECK_GT(system(bin_name_.c_str()), 0); // Verify vgg binary works
    LOG(ERROR) << "binary works!";
  }
  max_feature_count_ = 50000; // upper limit for sanity checking
}

VGGFeatureExtractor::~VGGFeatureExtractor(){
  LOG(INFO) << "removing temporary files...";
  fs::remove_all(temp_dir_);
}

bool VGGFeatureExtractor::ExtractFromData(const std::string& jpeg_image_data, iw::ImageFeatures* features){
  CHECK(features);
  features->Clear();

  std::vector<char> data(jpeg_image_data.begin(), jpeg_image_data.end());
  cv::Mat img = cv::imdecode(data, 0); // 0 for force load as grayscale
  CHECK_GT(img.cols, 0);
  CHECK_GT(img.rows, 0);
  features->set_width(img.cols);
  features->set_height(img.rows);
  if (img.empty()){
    LOG(WARNING)  << "Error decoding jpeg image data";
    return false;
  }
  std::string temp_image_filename = temp_dir_ + "/temp.png";
  if (!cv::imwrite(temp_image_filename, img)){
    LOG(WARNING)  << "Error writing file: " << temp_image_filename;
    return false;
  }
  std::string asciiDescriptorFilename = temp_dir_ + "/tmp_features.txt";
  fs::remove(asciiDescriptorFilename);
  std::string detectFeatureCommand =  boost::str(boost::format("%s %s -sift -i %s %s -o2 %s > /dev/null") % bin_name_ % feature_flag_ % temp_image_filename % threshold_flag_ % asciiDescriptorFilename);
  //LOG(INFO) << "detectFeatureCommand: " << detectFeatureCommand;
  CHECK_EQ(system(detectFeatureCommand.c_str()), 0);
  return ParseAsciiFormat(asciiDescriptorFilename, features);
}

class KeypointDescriptorPair {
public:
  double score;
  iw::FeatureKeypoint keypoint;
  iw::FeatureDescriptor descriptor;
};

// comparison, not case sensitive.
bool CompareScore(KeypointDescriptorPair* a, KeypointDescriptorPair* b) {
  if (a->score > b->score){
    return true;
  }
  return false;
}

bool VGGFeatureExtractor::ParseAsciiFormat(std::string filename,
                                           iw::ImageFeatures* features){
  std::fstream f(filename.c_str(), std::ios_base::in);
  if (!f.is_open()){
    LOG(WARNING) << "Can't open file: " << filename;
    return false;
  }
  int descLength = 0;
  int featCount = 0;
  f >> descLength;
  CHECK(descLength == 128);
  f >> featCount;
  std::vector<KeypointDescriptorPair*> kp_desc_pairs;
  // collect all features
  for (int i=0; i < featCount;++i){
    //read keypoint params
    //x y cornerness scale/patch_size angle object_index  point_type laplacian_value extremum_type mi11 mi12 mi21 mi22
    double x, y, cornerness, patch_size, alpha, objectIndex, pointType, laplacianValue, extremumType, mi11, mi12, mi21, mi22;
    f >> x >> y >> cornerness >> patch_size >> alpha >> objectIndex >> pointType >> laplacianValue >> extremumType >> mi11 >> mi12 >> mi21 >> mi22;
    KeypointDescriptorPair* kdp = new KeypointDescriptorPair();
    kdp->score = cornerness;
    kdp->keypoint.mutable_pos()->set_x(x);
    kdp->keypoint.mutable_pos()->set_y(y);
    kdp->keypoint.set_radius(patch_size*0.5); //it seems VGG reports the keypoint diameter (so convert to radius by scaling by 1/2)
    kdp->keypoint.set_angle(alpha);
    kdp->keypoint.mutable_iso()->set_m00(mi11);
    kdp->keypoint.mutable_iso()->set_m01(mi12);
    kdp->keypoint.mutable_iso()->set_m10(mi21);
    kdp->keypoint.mutable_iso()->set_m11(mi22);
    //kdp->keypoint.set_extremum_type((extremumType > 0));
    //read descriptor
    char desc_data[128];
    for (int j=0;j < 128; ++j){
      double d;
      f >> d;
      CHECK(d <= 255.0);
      desc_data[j] = (char)d;
    }
    kdp->descriptor.set_data(desc_data, 128);
    kp_desc_pairs.push_back(kdp);
  }
  //LOG(INFO) << featCount;
  // order by decreasing cornerness
  sort(kp_desc_pairs.begin(), kp_desc_pairs.end(), CompareScore);
  // keep only top features by cornerness score
  for (int i=0; i < max_feature_count_ && i < kp_desc_pairs.size(); ++i){
    KeypointDescriptorPair* kdp = kp_desc_pairs[i];
    //cout << "score: " << kdp->score << endl;
    features->add_keypoints()->CopyFrom(kdp->keypoint);
    features->add_descriptors()->CopyFrom(kdp->descriptor);
  }
  // free memory
  for (unsigned int i=0; i < kp_desc_pairs.size(); ++i){
    delete kp_desc_pairs[i];
  }
  f.close();
  return true;
}

} // close namespace iw
