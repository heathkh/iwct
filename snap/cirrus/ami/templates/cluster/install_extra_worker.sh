#!/bin/bash

function update_package_list() {    
    apt-get -y -q update  &>/dev/null
}

# Install a list of packages on ubuntu
function install_packages() {
    export DEBIAN_FRONTEND=noninteractive
    apt-get -y -q install $@  
}

function install_ganglia(){     
  install_packages 'sysvinit-utils' # needed for service command
  install_packages 'chkconfig'
  ln -s /usr/lib/insserv/insserv /sbin/insserv
  install_packages 'ganglia-monitor'
  
  # set ganglia to start on boot
  # causes an error... don't know why...   
  #sudo chkconfig --add ganglia-monitor  
}

update_package_list
install_ganglia
