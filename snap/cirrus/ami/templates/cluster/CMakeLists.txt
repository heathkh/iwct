CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

LOCAL_RESOURCES(
  NAME      cluster
  FILES     install_mapr.sh
            master.yml
            worker.yml
            common.yml            
)



