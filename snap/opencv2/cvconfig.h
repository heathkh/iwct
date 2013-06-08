/* Define to one of `_getb67', `GETB67', `getb67' for Cray-2 and Cray-YMP
   systems. This function is required for `alloca.c' support on those systems.
   */
//#define  CRAY_STACKSEG_END

/* Define to 1 if using `alloca.c'. */
//#define C_ALLOCA

/* Define to 1 if you have `alloca', as a function or macro. */
//#define HAVE_ALLOCA 1

/* Define to 1 if you have <alloca.h> and it should be used (not on Ultrix).
   */
#define HAVE_ALLOCA_H 1

/* V4L capturing support */
//#define HAVE_CAMV4L

/* V4L2 capturing support */
//#define HAVE_CAMV4L2

/* V4L/V4L2 capturing support via libv4l */
//#define HAVE_LIBV4L

/* Carbon windowing environment */
//#define HAVE_CARBON

/* IEEE1394 capturing support */
//#define HAVE_DC1394

/* libdc1394 0.9.4 or 0.9.5 */
//#define HAVE_DC1394_095

/* IEEE1394 capturing support - libdc1394 v2.x */
//#define HAVE_DC1394_2

/* ffmpeg in Gentoo */
//#define HAVE_GENTOO_FFMPEG

/* FFMpeg video library */
//#define  HAVE_FFMPEG

/* FFMpeg version flag */
//#define  NEW_FFMPEG

/* ffmpeg's libswscale */
//#define  HAVE_FFMPEG_SWSCALE

/* GStreamer multimedia framework */
//#define  HAVE_GSTREAMER

/* GTK+ 2.0 Thread support */
//#define  HAVE_GTHREAD

/* GTK+ 2.x toolkit */
//#define  HAVE_GTK

/* OpenEXR codec */
//#define  HAVE_ILMIMF

/* Apple ImageIO Framework */
//#define  HAVE_IMAGEIO

/* Define to 1 if you have the <inttypes.h> header file. */
//#define  HAVE_INTTYPES_H 1

/* JPEG-2000 codec */
//#define  HAVE_JASPER

/* IJG JPEG codec */
#define  HAVE_JPEG

/* Define to 1 if you have the `dl' library (-ldl). */
//#define  HAVE_LIBDL 1

/* Define to 1 if you have the `gomp' library (-lgomp). */
//#define  HAVE_LIBGOMP 1

/* Define to 1 if you have the `m' library (-lm). */
//#define  HAVE_LIBM 1

/* libpng/png.h needs to be included */
//#define  HAVE_LIBPNG_PNG_H

/* Define to 1 if you have the `pthread' library (-lpthread). */
#define  HAVE_LIBPTHREAD 1

/* Define to 1 if you have the `lrint' function. */
//#define  HAVE_LRINT 1

/* PNG codec */
#define  HAVE_PNG

/* Define to 1 if you have the `png_get_valid' function. */
//#define  HAVE_PNG_GET_VALID 1

/* png.h needs to be included */
//#define  HAVE_PNG_H

/* Define to 1 if you have the `png_set_tRNS_to_alpha' function. */
//#define  HAVE_PNG_SET_TRNS_TO_ALPHA 1

/* QuickTime video libraries */
//#define  HAVE_QUICKTIME

/* AVFoundation video libraries */
//#define  HAVE_AVFOUNDATION

/* TIFF codec */
//#define  HAVE_TIFF

/* Unicap video capture library */
//#define  HAVE_UNICAP

/* Define to 1 if you have the <unistd.h> header file. */
#define  HAVE_UNISTD_H 1

/* Xine video library */
//#define  HAVE_XINE

/* OpenNI library */
//#define  HAVE_OPENNI

/* LZ77 compression/decompression library (used for PNG) */
//#define  HAVE_ZLIB

/* Intel Integrated Performance Primitives */
//#define  HAVE_IPP

/* OpenCV compiled as static or dynamic libs */
//#define  BUILD_SHARED_LIBS

/* Name of package */
#define  PACKAGE "opencv"

/* Define to the address where bug reports for this package should be sent. */
#define  PACKAGE_BUGREPORT "opencvlibrary-devel@lists.sourceforge.net"

/* Define to the full name of this package. */
#define  PACKAGE_NAME "opencv"

/* Define to the full name and version of this package. */
#define  PACKAGE_STRING "opencv 2.4.2"

/* Define to the one symbol short name of this package. */
#define  PACKAGE_TARNAME "opencv"

/* Define to the version of this package. */
#define  PACKAGE_VERSION "2.4.2"

/* If using the C implementation of alloca, define if you know the
   direction of stack growth for your system; otherwise it will be
   automatically deduced at runtime.
	STACK_DIRECTION > 0 => grows toward higher addresses
	STACK_DIRECTION < 0 => grows toward lower addresses
	STACK_DIRECTION = 0 => direction of growth unknown */
//#define  STACK_DIRECTION

/* Version number of package */
#define  VERSION "2.4.2"

/* Define to 1 if your processor stores words with the most significant byte
   first (like Motorola and SPARC, unlike Intel and VAX). */
//#define  WORDS_BIGENDIAN

/* Intel Threading Building Blocks */
//#define  HAVE_TBB

/* Eigen Matrix & Linear Algebra Library */
//#define  HAVE_EIGEN

/* NVidia Cuda Runtime API*/
//#define HAVE_CUDA

/* NVidia Cuda Fast Fourier Transform (FFT) API*/
//#define HAVE_CUFFT

/* NVidia Cuda Basic Linear Algebra Subprograms (BLAS) API*/
//#define HAVE_CUBLAS

/* Compile for 'real' NVIDIA GPU architectures */
#define CUDA_ARCH_BIN ""

/* Compile for 'virtual' NVIDIA PTX architectures */
#define CUDA_ARCH_PTX ""

/* NVIDIA GPU features are used */
#define CUDA_ARCH_FEATURES ""

/* Create PTX or BIN for 1.0 compute capability */
//#define CUDA_ARCH_BIN_OR_PTX_10

/* VideoInput library */
//#define HAVE_VIDEOINPUT

/* XIMEA camera support */
//#define HAVE_XIMEA

/* OpenGL support*/
//#define HAVE_OPENGL
