#!/bin/bash

# Must be run on master node after maprcli command is available

# This script is a hack because I can't get the darn quotation escaping right
# to pass this jason bit through ssh... BASH!  
sudo maprcli config save -values '{"cldb.ganglia.cldb.metrics":"1"}'
sudo maprcli config save -values '{"cldb.ganglia.fileserver.metrics":"1"}'