CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

PY_LIBRARY(
  NAME      boto
  SOURCES   auth_handler.py
            dynamodb
            iam
            roboto
            auth.py                 ec2                  __init__.py      route53
            beanstalk               ecs                  jsonresponse.py  s3
            cacerts                 elasticache          manage           sdb
            cloudformation          elastictranscoder    mashups          services
            cloudfront              emr                  mturk            ses
            cloudsearch             exception.py         mws              sns
            CMakeLists.txt          file                 plugin.py        sqs
            compat.py               fps                  provider.py      storage_uri.py
            connection.py           glacier              pyami            sts
            contrib                 gs                   rds              swf
            handler.py           regioninfo.py    utils.py
            datapipeline            https_connection.py  resultset.py     vpc
)

