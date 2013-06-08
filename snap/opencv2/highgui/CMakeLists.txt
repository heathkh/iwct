CMAKE_MINIMUM_REQUIRED(VERSION 2.6)

CPP_LIBRARY(
  NAME highgui  
  SOURCES  
    src/bitstrm.cpp
    #src/cap_android.cpp
    #src/cap_cmu.cpp
    #src/cap.cpp
    #src/cap_dc1394.cpp
    #src/cap_dc1394_v2.cpp
    #src/cap_dshow.cpp
    #src/cap_ffmpeg.cpp
    #src/cap_gstreamer.cpp
    #src/cap_images.cpp
    #src/cap_libv4l.cpp
    #src/cap_mil.cpp
    #src/cap_openni.cpp
    #src/cap_pvapi.cpp
    #src/cap_qt.cpp
    #src/cap_tyzx.cpp
    #src/cap_unicap.cpp
    #src/cap_v4l.cpp
    #src/cap_vfw.cpp
    #src/cap_ximea.cpp
    #src/cap_xine.cpp
    src/grfmt_base.cpp
    src/grfmt_bmp.cpp
    #src/grfmt_exr.cpp
    #src/grfmt_imageio.cpp
    #src/grfmt_jpeg2000.cpp
    src/grfmt_jpeg.cpp
    src/grfmt_png.cpp
    src/grfmt_pxm.cpp
    src/grfmt_sunras.cpp
    src/grfmt_tiff.cpp
    src/loadsave.cpp
    src/precomp.cpp
    src/utils.cpp
    #src/window_carbon.cpp
    src/window.cpp
    #src/window_gtk.cpp
    #src/window_QT.cpp
    #src/window_w32.cpp
  HEADERS   highgui.hpp
  PACKAGES  //snap/opencv2/core:core
            //snap/opencv2/imgproc:imgproc
            //snap/jpeg:jpeg
            //snap/png:png    
  LIB_TYPE
    STATIC
)
