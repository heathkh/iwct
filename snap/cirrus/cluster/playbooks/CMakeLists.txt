CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

LOCAL_RESOURCES(
  NAME      templates  
  FILES     gmetad.conf.j2
            gmond.conf.j2
            mapred-site.yml
            mapred-site.xml.j2            
            master.yml
            worker.yml
            common.yml
            start_node             
              
)

