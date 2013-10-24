CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

PY_BINARY (
  NAME      add_distractors
  SOURCES   add_distractors.py   
)

PY_BINARY (
  NAME      pack_tide_images_to_pert
  SOURCES   pack_tide_images_to_pert.py   
)

PY_BINARY (
  NAME      extract_images_from_pert
  SOURCES   extract_images_from_pert.py   
)

PY_BINARY (
  NAME      extract_pos_images_from_pert
  SOURCES   extract_pos_images_from_pert.py   
)

PY_BINARY (
  NAME      upload_images_to_s3
  SOURCES   upload_images_to_s3.py   
)

PY_BINARY (
  NAME      correct_bb_for_crop
  SOURCES   correct_bb_for_crop.py   
)

PY_LIBRARY (
  NAME      crop
  SOURCES   crop.py   
)




