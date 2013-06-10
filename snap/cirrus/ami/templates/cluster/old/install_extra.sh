#!/bin/bash

function update_package_list() {    
    apt-get -y -q update  &>/dev/null
}

# Install a list of packages on ubuntu
function install_packages() {    
    export DEBIAN_FRONTEND=noninteractive
    apt-get -y -q install $@  
}

function ensure_multiverse(){
  CODENAME=`lsb_release -sc`
  echo $CODENAME
  if [ "${CODENAME}" == "precise" ]  
  then 
    sudo perl -pi.orig -e   'next if /-backports/; s/^# (deb .* multiverse)$/$1/'   /etc/apt/sources.list
  elif [ "${CODENAME}" == "oneiric" ]
  then
    sudo perl -pi.orig -e   'next if /-backports/; s/^# (deb .* multiverse)$/$1/'   /etc/apt/sources.list
  elif [ "lucid" == "${CODENAME}" ] # 10.04 LTS Lucid
  then
    sudo perl -pi.orig -e   's/^(deb .* universe)$/$1 multiverse/'   /etc/apt/sources.list
  fi
  apt-get -y -q update &>/dev/null
}

function install_ec2tools(){    
  ensure_multiverse
  sudo apt-add-repository ppa:awstools-dev/awstools
  update_package_list
  install_packages 'ec2-ami-tools'
}

function install_ntp(){   
  install_packages 'ntp'    
}


update_package_list
install_ec2tools
install_ntp
