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
  install_packages 'ganglia-monitor'
  install_packages 'gmetad'
  install_packages 'ganglia-webfrontend'
  install_packages 'sysvinit-utils' # needed for service command
  
  # place ganglia php files in apache dir  
  sudo ln -s /etc/ganglia-webfrontend/apache.conf /etc/apache2/sites-enabled/ganglia  
  sudo service ganglia-monitor restart
  sudo service gmetad restart
  sudo service apache2 restart
  
  # set ganglia to start on boot
  sudo apt-get install chkconfig
  ln -s /usr/lib/insserv/insserv /sbin/insserv
  sudo chkconfig --add gmetad
  sudo chkconfig --add ganglia-monitor
  sudo chkconfig --add apache2
    
}

update_package_list
install_ganglia
