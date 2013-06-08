CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

LOCAL_RESOURCES(
  NAME      cluster
  FILES     post_boot_patch.py 
            install_mapr.sh
            install_extra.sh
            mapred-site.xml
            start_node                    
            post_boot_patch.py   
            default_volume_config.sh
            config_ganglia_master.sh
            install_extra_master.sh
            install_extra_worker.sh
            hadoop-metrics.properties
            gmond.conf.template
            gmetad.conf.template
            mapred-site.xml.template
            libsnappy.so
)



