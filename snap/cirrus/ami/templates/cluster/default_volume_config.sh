#!/bin/bash

# Must be run on master node after maprcli command is available

# Make all new volumes default to the /data topology which is setup to have about
# the same number of nodes in different zones for robustness to zone failure.

# This script is a hack because I can't get the darn quotation escaping right
# to pass this json bit through ssh... BASH!  
sudo maprcli config save -values "{\"cldb.default.volume.topology\":\"/data\"}"