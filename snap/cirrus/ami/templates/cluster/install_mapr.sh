#!/bin/bash
# Copyright (c) 2009 & onwards. MapR Tech, Inc., All rights reserved

THIS_SCRIPT=`basename $0`
MAPR_PREFIX="mapr"
INSTALLSCRIPTS_DIR="/opt/install-$MAPR_PREFIX"
INSTALL_DIR=${MAPR_HOME:-/opt/$MAPR_PREFIX}
LOG_FILE="$INSTALL_DIR/logs/$THIS_SCRIPT.log"
lock="/tmp/$THIS_SCRIPT.lck"
pid=$$
MAPR_URL="http://package.$MAPR_PREFIX.com/releases"
MAPR_ECO_URL="http://package.$MAPR_PREFIX.com/releases/ecosystem"
TIMEOUT=1800
STATUS_WAIT_TIME=3
ETC_HOSTS="/etc/hosts"
ETC_ISSUE="/etc/issue"
ETC_REDHAT="/etc/redhat-release"
ETC_SUSE="/etc/SuSE-release"

# Ubuntu Package commands
DPKG_I="dpkg --force-all -i "
DPKG_Q="dpkg -l "
APT_GET="apt-get -qyf --force-yes "
APT_CACHE="apt-cache -qf "
APT_CACHE_DIR="/var/cache/apt/archives"
ETC_APT_SOURCES_DIR="/etc/apt/sources.list.d"

# RedHat/Centos Package commands
RPM_I="rpm --quiet --force --nosignature -i "
RPM_Q="rpm -qa"
YUM="yum -qy --nogpgcheck "
ETC_YUM_REPOS_DIR="/etc/yum.repos.d"

# SUSE Package commands
ZYPPER="zypper -nq --no-gpg-checks"
ETC_ZYPPER_REPOS_DIR="/etc/zypp/repos.d"
ZYPPER_CACHE_DIR="/var/cache/zypp/packages"

JAVA_VERSION_6="1.6"
JAVA_VERSION_7="1.7"
MIN_MAPR_VERSION=2
PORTS="5660 7221 7222 5181 9001 50030 50060 8443 60000 60010 60020 60030"
BAD_ETC_HOSTS_ENTRY="127.0.1.1"
MIN_OS_PARTITION_SIZE=10 # 10G
MIN_MEM_SIZE=4 # 4G
MIN_MFS_DISK_SIZE=8 # 8G
SWAPSIZE_FACTOR=2 # Swapsize >= 2* Memsize
DIVIDER=1000
MAX_PARALLEL_NODES=4
ssh_options="-q -o NumberOfPasswordPrompts=0 -o StrictHostKeyChecking=no "
PkgMsg="comma-separated list of packages to be installed on this node"
CldbMsg="comma-separated list of CLDBs for this cluster"
ZkMsg="comma-separated list of Zookeeper for this cluster"
DiskMsg="path to a valid disks file, required by the $MAPR_PREFIX-fileserver package"
ForceMsg="force install the product"
RolesMsg="path to a roles file having the list of nodes, packages and disks"
CldbMsg="comma-separated list of CLDBs for this cluster"
ZkMsg="comma-separated list of Zookeeper for this cluster"
RolesFormatMsg="#1) Roles file (-R) is required. Each line in the roles file should have the following format:
...
<ip/hostname> <comma-separated-list-of-packages> <comma-separated-list-of-disks>
...
#2) Comma-separated list of disks is required if $MAPR_PREFIX-fileserver dependent package is to be be installed."

ValidPkgs=("$MAPR_PREFIX-cascading" 
           "$MAPR_PREFIX-cldb"
           "$MAPR_PREFIX-client"
           "$MAPR_PREFIX-compat-suse"
           "$MAPR_PREFIX-core" 
           "$MAPR_PREFIX-emc"
           "$MAPR_PREFIX-fileserver"
           "$MAPR_PREFIX-flume"
           "$MAPR_PREFIX-hbase-internal"
           "$MAPR_PREFIX-hbase-master"
           "$MAPR_PREFIX-hbase-regionserver"
           "$MAPR_PREFIX-hcatalog"
           "$MAPR_PREFIX-hcatalog-server"
           "$MAPR_PREFIX-hive"
           "$MAPR_PREFIX-jobtracker"
           "$MAPR_PREFIX-metrics"
           "$MAPR_PREFIX-mahout"
           "$MAPR_PREFIX-nfs"
           "$MAPR_PREFIX-oozie-internal"
           "$MAPR_PREFIX-oozie"
           "$MAPR_PREFIX-pig"
           "$MAPR_PREFIX-single-node" 
           "$MAPR_PREFIX-sqoop"
           "$MAPR_PREFIX-tasktracker"
           "$MAPR_PREFIX-upgrade"
           "$MAPR_PREFIX-webserver"
           "$MAPR_PREFIX-whirr"
           "$MAPR_PREFIX-zk-internal"
           "$MAPR_PREFIX-zookeeper")

SupportedOS[0]="Red Hat.*release 5"
SupportedOS[1]="Red Hat.*release 6"
SupportedOS[2]="Centos release 5"
SupportedOS[3]="Centos release 6"
SupportedOS[4]="Ubuntu 9"
SupportedOS[5]="Ubuntu 10"
SupportedOS[6]="Ubuntu 11"
SupportedOS[7]="Ubuntu 12"
SupportedOS[8]="SUSE.*Server 11"

SupportedOSMsg="RedHat Enterprise 5+, Centos 5+, Ubuntu 9+ and SUSE Enterprise 11+"

extension="deb"
hasYum=0
hasZypper=0

if [ "`grep -i Ubuntu $ETC_ISSUE 2>/dev/null`" = "" -o -e $ETC_REDHAT -o -e $ETC_SUSE ]; then
  extension="rpm"  
  if [ "`which yum 2>/dev/null`" != "" ]; then
    hasYum=1
  elif [ "`which zypper 2>/dev/null`" != "" ]; then
    hasZypper=1
  fi
fi

nodeinstall_str=""
clusterinstall_str="$INSTALLSCRIPTS_DIR/$THIS_SCRIPT -t "
clientOnly=0
useEtcRepo=1
numErrs=0
errStr[0]=""
inputStr=""
hosts[0]=""
hosts_pacakges[0]=""
hosts_disks[0]=""
localHost=""
localIpList[0]=""
numNodes=0
failedNodes[0]=""
numFailedNodes=0
successfulNodes[0]=""
numSuccessfulNodes=0
packages=""
doExit=1
wroteRepoFile=0

#
# Parameters for this script
#
clusterName=""
version=""
pkg_dir=""
# TODO: Do we always format
diskFormat=" -F "
diskFile=""
TmpDiskFile="/tmp/$MAPR_PREFIX.disks.$pid"
cldbNodesList=""
zkNodesList=""
dry_run=0
force=0
isvm=0
startServices=1
num_parallel_nodes=2
username=`whoami`
prompt=1
baseurl=""
ecourl=""

#############################################
############## Common functions #############
#############################################

function KillAndExit() {
  # Explicitly send a signal to parent. This will prevent SSH hangs
  sleep 5 && kill $PPID &
  exit $1
}

# Check whether there is already same script is running or not.
# if there is already running then exit.
function LockForSingleInstance() {
  exec 7>$lock;

  # Mac has no flock
  which flock >/dev/null 2>&1
  if [ $? -eq 0 ]; then
    if ! flock -n -x 7 2>/dev/null; then
      echo "It appears $0 is already running. If not, remove $lock and retry. Exiting $$"
      if [ "$doExit" = "1" ]; then
        KillAndExit $1
      else
        return 1
      fi
    fi
  fi
  return 0
}

function UnLockForSingleInstance() {
  # Mac has no flock
  which flock >/dev/null 2>&1
  if [ $? -eq 0 ]; then
    flock -u 7 2>/dev/null;
  fi
  rm -f $lock
}

function CheckForRoot() {
  if [ "`id -u`" != "0" ]; then
    echo "Need to have root previleges to install MapR packages" >&4
    echo ""
    KillAndExit $1
  fi 
}

function UnLockExit() {
  CleanupRepoFile
  if [ -e $LOG_FILE ]; then
    grep "^\[" $LOG_FILE | cut -d ']' -f2 > $LOG_FILE.summary 2>/dev/null
    if [ "$1" != "" -a -e $INSTALL_DIR/logs/timed_wait.$pid.log ]; then
      cat $INSTALL_DIR/logs/timed_wait.$pid.log 2>/dev/null >> $LOG_FILE.summary
    fi
  fi
  UnLockForSingleInstance;
  echo "" >&3
  if [ "$doExit" = "1" ]; then
    KillAndExit $1
  else
    return 1
  fi
  return 0
}

function Exit() {
  echo "[`date`] $1"
  echo "     $1" >&4
  UnLockExit 1
}

function EchoDone() {
  echo "     Done" >&3
  echo "" >&3
}

# Runs a command. Kills the command, if it does not finish in a certain
# amount of time
# $*: command
function timed_wait()
{
  if [ "$1" = "" -o "`which $1 2>/dev/null`" = "" ]; then
     # The command doesn't exist
     return 1
  fi

  local bgwait=0
  local retVal=0

  ( $* >> $INSTALL_DIR/logs/timed_wait.$pid.log 2>&1 ) &
  local cmdpid=$!

  ( sleep $TIMEOUT ; if [ $cmdpid -ne 0 ]; then kill -9 $cmdpid; fi > /dev/null 2>&1 ) &
  local sleeppid=$!
  
  if [ "$bgwait" == "1" ]; then
    BGCMDPID=$cmdpid
    BGSLEEPPID=$sleeppid
  else
    wait $cmdpid > /dev/null 2>&1
    retVal=$?
    kill -0 $sleeppid > /dev/null 2>&1
    if [ $? -ne 0 ]; then
      echo "Command did not complete in $TIMEOUT seconds. Timed out"
      retVal=1
    fi
    pkill -9 -P $sleeppid > /dev/null 2>&1
  fi
  return $retVal
}

function Prompt() {
  inputStr=""
  while [ "$inputStr" = "" ]; do
    echo -n "     $1: " >&3
    read inputStr
  done
  inputStr=`echo $inputStr | sed -e 's/\n$//g'`
}

function ListValidPkgs() {
  echo "" >&3
  echo "     Valid MapR Packages:" >&3
  for v in ${ValidPkgs[*]}; do
    echo "       $v" >&3
  done
  echo "" >&3
}

function ListValidPkgsShort() {
  echo "" >&3
  echo "Valid MapR Packages:" >&3
  echo "--------------------" >&3

  local i=0
  for v in ${ValidPkgs[*]}; do
    printf '%-25s' "$v" >&3
    let "i++"
    if [ $i -eq 5 ]; then
      echo "" >&3
      i=0
    fi
  done
  echo "" >&3
}

#############################################
########### Node install functions ##########
#############################################

function WriteRepoFile()
{
  local ver=$1
  local ecourl=""

  if [ "$extension" = "deb" ]; then
    if [ "$baseurl" = "" ]; then
      baseurl="$MAPR_URL/v$ver/ubuntu/"
      ecourl="$MAPR_ECO_URL/ubuntu"
    fi
    mkdir -p $ETC_APT_SOURCES_DIR
    # Write default url
    echo "deb $baseurl $MAPR_PREFIX optional" > $ETC_APT_SOURCES_DIR/$MAPR_PREFIX.$pid.list
    # Append ecosystem url, only if user didn't specify a baseurl
    if [ "$ecourl" != "" ]; then
      echo "deb $ecourl binary/" >> $ETC_APT_SOURCES_DIR/$MAPR_PREFIX.$pid.list
    fi
    wroteRepoFile=1
    $APT_GET update 2>/dev/null
  else
    if [ "$baseurl" = "" ]; then
      baseurl="$MAPR_URL/v$ver/redhat"
      ecourl="$MAPR_ECO_URL/redhat"
    fi
    local dir=""
    if [ "$hasYum" != "0" ]; then
      dir=$ETC_YUM_REPOS_DIR
    elif [ "$hasZypper" != "0" ]; then
      dir=$ETC_ZYPPER_REPOS_DIR
      baseurl="$MAPR_URL/v$ver/suse"
      ecourl="$MAPR_ECO_URL/suse"
    fi
    mkdir -p $dir
    # Write default url
    echo "
[$MAPR_PREFIX]
name=MapR Technologies
baseurl=$baseurl
enabled=1
gpgcheck=0
protect=1" > $dir/$MAPR_PREFIX.$pid.repo
    # Write ecosystem url, only if user didn't specify a baseurl
    if [ "$ecourl" != "" ]; then
    echo "
[$MAPR_PREFIX-eco]
name=MapR Technologies
baseurl=$ecourl
enabled=1
gpgcheck=0
protect=1" > $dir/$MAPR_PREFIX-eco.$pid.repo
    fi
    wroteRepoFile=1
  fi
  return 0
}

function CleanupRepoFile() {
  if [ "$wroteReoFile" = "0" ]; then
    return
  fi

  if [ "$extension" = "deb" ]; then
    rm -f $ETC_APT_SOURCES_DIR/$MAPR_PREFIX.$pid.list
  else
    rm -f $dir/$MAPR_PREFIX.$pid.repo
    rm -f $dir/$MAPR_PREFIX-eco.$pid.repo
  fi
}

# Attempt to automatically install some required packages like pssh
# $1: command to test
# $2: Package name, usually same as $1
function CheckAndInstallPkgFromRepo() {
  local pkg=$1

  # Otherwise do a fresh install
  if [ "$extension" = "deb" ];then
    $PKG_TIMEOUTCMD $APT_GET install $pkg 2>/dev/null
  elif [ "$hasYum" != "0" ]; then
    $PKG_TIMEOUTCMD $YUM install $pkg 2>/dev/null
  elif [ "$hasZypper" != "0" ]; then
    $PKG_TIMEOUTCMD $ZYPPER install $pkg 2>/dev/null
  fi
  return $?
}

function CheckAndInstallPkgFromDir() {
  if [ "$pkg_dir" = "" ]; then
    return 1
  fi

  local pkg=$1

  if [ "$extension" = "deb" ]; then
    $PKG_TIMEOUTCMD $DPKG_I $pkg_dir/$pkg*.$extension 2>/dev/null
  else
    $PKG_TIMEOUTCMD $RPM_I $pkg_dir/$pkg*.$extension 2>/dev/null
  fi
  return $?
}

function CheckAndInstallPkg() {
  local cmd=$1
  local pkg=$2
  if [ "$pkg" = "" ]; then
    pkg=$1
  fi

  # If the command exists, we don't need to install
  if [ "`which $cmd 2>/dev/null`" != "" ]; then
    return 0
  fi

  CheckAndInstallPkgFromRepo $pkg
  if [ "$?" = "0" ]; then
    return 0
  fi

  if [ "$pkg_dir" != "" ]; then
    CheckAndInstallPkgFromDir $pkg
    return $?
  fi

  # Could not install
  return 1
}

function CheckJavaVersion() {
  # TODO: Download and install Java from:
  # http://javadl.sun.com/webapps/download/AutoDL?BundleId=63253
  
  install_java

  if [ -n "$JAVA_HOME" -a -d "$JAVA_HOME" ]; then
    if [ -f "$JAVA_HOME"/bin/java ]; then
      JAVA="$JAVA_HOME"/bin/java
    fi
  elif [ "$JAVA_HOME" = "" ]; then
    which java >/dev/null 2>&1
    if [ $? = 0 ]; then
      JAVA=java
    fi
  fi

  if [ "$JAVA" = "" ]; then
    errStr[$numErrs]="There is no java available either in default PATH: $PATH \
                 or through JAVA_HOME environment variable"
    let "numErrs++"
    return 1
  fi

  $JAVA -version >/dev/null 2>&1
  if [ $? -ne 0 ]; then
    errStr[$numErrs]="There is no java available either in default PATH: $PATH \
                 or through JAVA_HOME environment variable"
    let "numErrs++"
    return 1
  fi

  distOk=0
  verOk=0
  bitOk=0

  $JAVA -version 2>&1 | grep -E "^(Java|Open)" >/dev/null
  if [ $? -eq 0 ]; then
    distOk=1
  fi

  $JAVA -version 2>&1 | grep "version" | grep -E "($JAVA_VERSION_6|$JAVA_VERSION_7)" >/dev/null
  if [ $? -eq 0 ]; then
    verOk=1
  fi

  $JAVA -version 2>&1 | grep -i "64-Bit" >/dev/null
  if [ $? -eq 0 ]; then
    bitOk=1
  fi

  if [ $distOk -eq 0 -o $verOk -eq 0 -o $bitOk -eq 0 ]; then
    errStr[$numErrs]="It appears like Sun/Open Java 1.6.xx/1.7.xx (64-Bit) is not installed. Please install it, set the JAVA_HOME environment variable and retry." 
    let "numErrs++"
    return 1
  fi

  return 0
}


function install_java() {  
  target_dir=/usr/lib/jvm/java-6-sun

  if [ -e "$target_dir" ]; then
    echo "It appears java is already installed. Skipping java installation."
    echo "Move /usr/lib/jvm/java-6-sun out of the way if you want to reinstall"

    #nasty little hack... somewhere the string 'r e t u r n' gets replaced by exit
    turn=turn
    re$turn
  fi

  arch=$(uname -m)

  # Find out which .bin file to download
  #url=http://download.oracle.com/otn-pub/java/jdk/6u31-b04/jdk-6u31-linux-i586.bin
  # must host binary manually, because oracle website no longer works for downloads without cookies
  url=http://graphics.stanford.edu/~heathkh/mapr/scripts/whirr/sun/java/jdk-6u31-linux-i586.bin
      
  if [ "x86_64" == "$arch" ]; then
    #url=http://download.oracle.com/otn-pub/java/jdk/6u31-b04/jdk-6u31-linux-x64.bin        
    # must host binary manually, because oracle website no longer works for downloads without cookies
    url=http://graphics.stanford.edu/~heathkh/mapr/scripts/whirr/sun/java/jdk-6u31-linux-x64.bin
  fi

  tmpdir=$(mktemp -d)
  curl $url -L --silent --show-error --fail --connect-timeout 10 --max-time 600 --retry 5 -o $tmpdir/$(basename $url)

  (cd $tmpdir; sh $(basename $url) -noregister)
  mkdir -p $(dirname $target_dir)
  (cd $tmpdir; mv jdk1* $target_dir)
  rm -rf $tmpdir

  if which dpkg &> /dev/null; then
    update-alternatives --install /usr/bin/java java $target_dir/bin/java 17000
    update-alternatives --set java $target_dir/bin/java
  elif which rpm &> /dev/null; then
    alternatives --install /usr/bin/java java $target_dir/bin/java 17000
    alternatives --set java $target_dir/bin/java
  else
    # Assume there is no alternatives mechanism, create our own symlink
    ln -sf "$target_dir/bin/java" /usr/bin/java
  fi

  # Try to set JAVA_HOME in a number of commonly used locations
  export JAVA_HOME=$target_dir
  if [ -f /etc/profile ]; then
    echo export JAVA_HOME=$JAVA_HOME >> /etc/profile
  fi
  if [ -f /etc/bashrc ]; then
    echo export JAVA_HOME=$JAVA_HOME >> /etc/bashrc
  fi
  if [ -f /etc/skel/.bashrc ]; then
    echo export JAVA_HOME=$JAVA_HOME >> /etc/skel/.bashrc
  fi
  if [ -f "$DEFAULT_HOME/$NEW_USER" ]; then
    echo export JAVA_HOME=$JAVA_HOME >> $DEFAULT_HOME/$NEW_USER
  fi
}

function CheckOSVersion () {
  local badOS=1
  local file=""

  if [ -e $ETC_ISSUE ]; then
    file=$ETC_ISSUE
  elif [ -e $ETC_REDHAT ]; then
    file=$ETC_REDHAT
  elif [ -e $ETC_SUSE ]; then
    file=$ETC_SUSE
  fi

  if [ "$file" != "" -a -f $file ]; then
    local i=0
    while [ $i -lt ${#SupportedOS[*]} ]; do
      egrep -i "${SupportedOS[$i]}" $file >/dev/null 2>&1
      if [ "$?" = "0" ]; then
        badOS=0
        break
      fi
      let "i++"
    done
  fi

  if [ "$badOS" != "0" ]; then
    errStr[$numErrs]="It appears like this OS is not one of the MapR supported OS versions: $SupportedOSMsg"
    let "numErrs++"
    return 1
  fi

  #
  # Check if basic packages are available
  #
  if [ "$extension" = "deb" ]; then
    if [ "`which dpkg`" = "" ]; then
      errStr[$numErrs]="It appears like package 'dpkg' is not installed on this node. Please install that and retry."
      let "numErrs++"
      return 1
    fi
  else
    if [ "`which rpm`" = "" ]; then
      errStr[$numErrs]="It appears like package 'rpm' is not installed on this node. Please install that and retry."
      let "numErrs++"
      return 1
    fi
  fi

  return 0
}

function CheckDiskFile() {
  local ndisks=0
  local errStrDisk[0]=""
  local numErrsDisk=0
  local ret=0

  # Parse file and ensure each disk in the path is present in /dev/
  while read str; do
    # Skip over empty and commented lines
    if [ "$str" = "" -o "`echo $str | grep "^#"`" != "" ]; then
      continue
    fi
    arr=$(echo $str | tr ' ' '\n')
    for disk in $arr; do
      if [[ $disk != /dev/* ]]; then
         disk="/dev/$disk"
      fi
      if [ ! -e $disk ]; then
        errStrDisk[$numErrsDisk]="Invalid disk $disk specified in $diskFile"
        let "numErrsDisk++"
      else
        let "ndisks++"
        val=`cat /sys/block/${disk:5}/size 2>/dev/null`
        if [ "$val" != "" ]; then
          let "val=$val*512/($DIVIDER*$DIVIDER*$DIVIDER)"
          if [ $val -lt $MIN_MFS_DISK_SIZE ]; then
            errStrDisk[$numErrsDisk]="$disk specified in $diskFile has less than $MIN_MFS_DISK_SIZE Gig of space"
            let "numErrsDisk++"
          fi
        fi
      fi
    done
  done < $diskFile

  if [ $ndisks -eq 0 ]; then
    errStrDisk[$numErrsDisk]="No valid disks found in $diskFile"
    let "numErrsDisk++"
  fi

  if [ $numErrsDisk -ne 0 ]; then
    for ((i=0; i < $numErrsDisk; i++));do
       echo "[`date`] ${errStrDisk[$i]}"
       echo "     -> ${errStrDisk[$i]}" >&4
    done
    echo "" >&4
    return 1
  fi

  return 0
}

function CheckOSPartition() {
  val=`df -l -BG / 2>/dev/null | awk '{if(NR==1) for(i=1;i<=NF;i++) { if($i~/Available/) { colnum=i; break} } else print $colnum}' | sed -e 's/G$//g'`
  if [ "$val" != "" -a $val -lt $MIN_OS_PARTITION_SIZE ]; then
    errStr[$numErrs]="It appears like the OS partition has less than $MIN_OS_PARTITION_SIZE Gig of space. Free up some space in the OS partition and retry."
    let "numErrs++"
    return 1
  fi
  return 0
}

function CheckPorts() {
  for port in $PORTS
  do
    netstat -an | grep ":$port " | grep "LISTEN " > /dev/null 2>&1
    if [ $? -eq 0 ]; then
      errStr[$numErrs]="It appears like port '$port' is not free. MapR requires this port for one of its processes. Free the port and retry."
      let "numErrs++"
      return 1
    fi
  done
  return 0
}

function CheckMemoryAndSwap() {
  # First check the memory
  memsize=`cat /proc/meminfo 2>/dev/null | grep MemTotal | awk '{print $2}'`
  if [ "$memsize" != "" ]; then
    let "memsize=$memsize/($DIVIDER*$DIVIDER)"
    if [ $memsize -lt $MIN_MEM_SIZE ] ; then
      errStr[$numErrs]="It appears like this system has less than $MIN_MEM_SIZE Gig of memory."
      let "numErrs++"
    fi
    let "val=$memsize*$SWAPSIZE_FACTOR"

    # Next check the Swap size
    swapsize=`cat /proc/swaps 2>/dev/null | awk '{sum=0; if(NR==1) for(i=1;i<=NF;i++) { if($i~/Size/) { colnum=i; break} } else sum += $colnum}  END {print sum}'`
    if [ "$swapsize" != "" ];then
      let "swapsize=$swapsize/($DIVIDER*$DIVIDER)"
      if [ $swapsize -lt $val ]; then
        errStr[$numErrs]="It appears like the swap size ($swapsize GB) is less than $SWAPSIZE_FACTOR * memory ($val GB)"
        let "numErrs++"
      fi
    fi
  fi
  return 0
}

function CheckDNS() {
  if [ "`which hostname`" != "" ]; then
    val=`hostname -f 2>/dev/null`
    if [ $? -ne 0 -o "$val" = "" ]; then
      errStr[$numErrs]="It appears like DNS is misconfigured. Please configure DNS correctly and ensure the command hostname -f returns a valid hostname."
      let "numErrs++"
      return 1
    fi
  fi
  return 0
}

function CheckEtcHosts() {
  if [ "`grep $BAD_ETC_HOSTS_ENTRY $ETC_HOSTS 2>/dev/null`" != "" ]; then
    errStr[$numErrs]="It appears like you have a bad entry $BAD_ETC_HOSTS_ENTRY in $ETC_HOSTS. Remove the bad entry and retry."
    let "numErrs++"
    return 1
  fi
  return 0
}

function CheckIrqBalance() {
  # Check if irqbalance is not installed
  if [ "$extension" = "deb" ]; then
    val=`$DPKG_Q 2>/dev/null | grep irqbalance`
   else
    val=`$RPM_Q 2>/dev/null | grep irqbalance`
  fi

  if [ "$val" = "" ]; then
    # Ubuntu 10.10 is one exception
    if [ -e $ETC_ISSUE -a "`grep -i Ubuntu 10.10 $ETC_ISSUE >/dev/null 2>&1`" != "" ]; then
       return 0
    fi

    # Attempt to install irqbalance
    if [ "$extension" = "deb" ]; then
      timed_wait $APT_GET install irqbalance 2>/dev/null
      if [ $? -eq 0 ]; then
        val=`$DPKG_Q 2>/dev/null | grep irqbalance`
      fi
    elif [ "$hasYum" != "0" ]; then
      timed_wait $YUM install irqbalance 2>/dev/null
      if [ $? -eq 0 ]; then
        val=`$RPM_Q 2>/dev/null | grep irqbalance`
      fi
    elif [ "$hasZypper" != "0" ]; then
      timed_wait $ZYPPER install irqbalance 2>/dev/null
      if [ $? -eq 0 ]; then
        val=`$RPM_Q 2>/dev/null | grep irqbalance`
      fi
    fi
    if [ "$val" != "" ]; then
      return 0
    fi

    # Failed to install irqbalance. Let the user do this
    errStr[$numErrs]="It appears like irqbalance package is not installed. Install irqbalance and retry."
    let "numErrs++"
    return 1
  elif [ -e $ETC_ISSUE -a "`grep -i Ubuntu 10.10 $ETC_ISSUE >/dev/null 2>&1`" != "" ]; then
    errStr[$numErrs]="It appears like irqbalance package is installed, which causes problems on Ubuntu 10.10. Uninstall irqbalance and retry."
    let "numErrs++"
    return 1
  fi
  return 0
}

function PreTest() {
  echo "[`date`] - Running pre-requisite checks ---"
  echo " --- Running pre-requisite checks ---" >&3

  CheckOSVersion
  CheckJavaVersion

  # If Client-only package, we don't need to do further checks
  if [ $clientOnly -eq 0 ]; then
    if [ "$diskFile" != "" ]; then
      CheckDiskFile
    fi

    #CheckOSPartition
    CheckPorts
    #CheckMemoryAndSwap
    CheckDNS
    CheckEtcHosts
    CheckIrqBalance
  fi

  if [ $numErrs -ne 0 ]; then
    echo "" >&4
    echo "     Issues detected:" >&4
    echo "     ----------------" >&4
    for ((i=0; i < $numErrs; i++));do
       let "j=$i+1"
       echo "[`date`] ${errStr[$i]}"
       echo "     #$j) ${errStr[$i]}" >&4
    done
    echo "" >&4

    if [ "$prompt" = "0" ]; then
      echo "[`date`]    Use -f (force) option to $ForceMsg"
      echo "     Use -f (force) option to $ForceMsg" >&4
      UnLockExit 1
    else
      Prompt "Do you want to $ForceMsg (y/n) ?"

      case "$inputStr" in
        "y"|"ye"|"yes" )
          force=1
          numErrs=0
          ;;
  
        * )
          force=0
          echo "[`date`]    Use -f (force) option to $ForceMsg"
          echo "     Use -f (force) option to $ForceMsg" >&4
          UnLockExit 1
          ;;
      esac
    fi
  fi
  EchoDone
}

function SimulateInstall() {

  if [ "$extension" = "deb" ]; then
    # Ubuntu
    if [ "$pkg_dir" != "" ]; then
      for p in $packages; do
        val=`find $pkg_dir -name "$p*.$extension" 2>/dev/null | grep -v nonstrip | head -1 | sed -e s'/\n//g'`
        nodeinstall_str="$nodeinstall_str $val"
      done
      # simulate upgrade
      echo "[`date`]   --- Simulating installation of packages ---" 
      echo " --- Simulating installation of packages ---" >&3
      timed_wait $DPKG_I --dry-run $nodeinstall_str
    else
      for p in $packages; do
        nodeinstall_str="$nodeinstall_str $p=$version*"
      done
      # First download packages
      if [ ! -e $APT_CACHE_DIR/$MAPR_PREFIX*$version.*.$extension ]; then
        echo "[`date`] - Downloading packages from repository to local cache ---"
        echo " --- Downloading packages from repository to local cache ---" >&3
        timed_wait $APT_GET -d install $nodeinstall_str
        if [ $? -ne 0 ]; then
          Exit "Failed to download some packages"
        fi
        EchoDone
      fi
  
      # Second simulate upgrade (NOTE NOTE: Putting a timeout here seems to hang)
      echo "[`date`] - Simulating installation of packages ---"
      echo " --- Simulating installation of packages ---" >&3
      # (NOTE NOTE: Putting a timeout here seems to hang)
      $DPKG_I --dry-run $APT_CACHE_DIR/*.$extension
    fi
  else
    # Redhat/Centos/SUSE
    if [ "$pkg_dir" != "" ]; then
      for p in $packages; do
        val=`find $pkg_dir -name "$p*.$extension" 2>/dev/null | grep -v nonstrip | head -1 | sed -e s'/\n//g'`
        nodeinstall_str="$nodeinstall_str $val"
      done
      echo "[`date`] - Simulating installation of packages ---"
      echo " --- Simulating installation of packages ---" >&3
      # simulate upgrade
      timed_wait $RPM_I --test $nodeinstall_str
    else
      for p in $packages; do
        if [ "$version" != "" ]; then
          nodeinstall_str="$nodeinstall_str $p=$version"
        else
          nodeinstall_str="$nodeinstall_str $p"
        fi
      done

      # NOTE: yum doesn't have a test/download only option

      if [ "$hasZypper" != "0" ]; then
        echo "[`date`] - Downloading packages from repository to local cache ---"
        echo " --- Downloading packages from repository to local cache ---" >&3
        timed_wait $ZYPPER install -d $nodeinstall_str
        if [ $? -ne 0 ]; then
          Exit "Failed to download some packages"
        fi
        EchoDone

        # Second simulate upgrade
        echo "[`date`] - Simulating installation of packages ---"
        echo " --- Simulating installation of packages ---" >&3
        timed_wait $RPM_I --test `find $ZYPPER_CACHE_DIR -name $MAPR_PREFIX*$version*.$extension`
      fi
    fi
  fi
  
  if [ $? -ne 0 ]; then
    Exit "Install simulation failed"
  fi
  EchoDone
}


function ActualInstall() {
  echo "[`date`] - Installing packages ---"
  echo " --- Installing packages ---" >&3
  local newpackages=""
  
  # Writethe hostname file just in case configure.sh doesn't
  hostname -f > $INSTALL_DIR/hostname

  if [ "$extension" = "deb" ]; then
    if [ "$pkg_dir" != "" ]; then
      timed_wait $DPKG_I $nodeinstall_str
    else
      # (NOTE NOTE: Putting a timeout here seems to hang)
      $DPKG_I /var/cache/apt/archives/*.$extension
    fi

    if [ $? -ne 0 ]; then
      Exit "Failed to install MapR packages"
    fi
    timed_wait $APT_GET install 2>/dev/null
    newpackages=`$DPKG_Q | grep "$MAPR_PREFIX-" | grep -v install | grep "^ii " | awk '{print $2}' | sort -u`
  else
    if [ "$pkg_dir" != "" ]; then
      timed_wait $RPM_I $nodeinstall_str
    elif [ "$hasYum" != "0" ]; then
      timed_wait $YUM install $nodeinstall_str
    elif [ "$hasZypper" != "0" ]; then
      timed_wait $RPM_I `find $ZYPPER_CACHE_DIR -name $MAPR_PREFIX*$version*.$extension`
    fi
  
    if [ $? -ne 0 ]; then
      Exit "Failed to install MapR packages"
    fi

    # On SUSE, install mapr-compat-suse, if it wasn't already installed
    if [ "$hasZypper" != "0" -a "`$RPM_Q 2>/dev/null | grep $MAPR_PREFIX-compat-suse`" = "" ]; then
      if [ "$pkg_dir" != "" ]; then
        if [ -f $pkg_dir/$MAPR_PREFIX-compat-suse*.$extension ]; then
          timed_wait $RPM_I $pkg_dir/$MAPR_PREFIX-compat-suse-*
        fi
      else
        timed_wait $ZYPPER install $MAPR_PREFIX-compat-suse
      fi
    fi

    newpackages=`$RPM_Q --queryformat '%{name}\n' | grep "$MAPR_PREFIX-" | grep -v install | sort -u`
  fi
  
  for p in $packages; do
    if [ "`echo $newpackages | grep $p`" = "" ]; then
      errStr[$numErrs]="Failed to install $p"
      let "numErrs++"
    fi
  done
  
  
  if [ $numErrs -ne 0 ]; then
    echo "     Issues detected:" >&4
    echo "     ----------------" >&4
    for ((i=0; i < $numErrs; i++));do
       let "j=$i+1"
       echo "[`date`] ${errStr[$i]}"
       echo "     #$j) ${errStr[$i]}" >&4
    done
    UnLockExit 1
  fi
  EchoDone

  grep "^\[" $LOG_FILE | cut -d ']' -f2 > $LOG_FILE.summary
}

function CheckRepoAndVersion() {
  local val=""
  if [ "$extension" = "deb" ]; then
    $APT_GET clean 2>/dev/null
    $APT_GET update 2>/dev/null
    val=`$APT_CACHE show "$MAPR_PREFIX-core" 2>/dev/null | grep "Version:" | grep "$version" | sort -u | tail -1`
  elif [ "$hasYum" != "0" ]; then
    $YUM clean all
    val=`$YUM list -q "$MAPR_PREFIX-core" 2>/dev/null | grep "$version" | sort -u | tail -1`
  elif [ "$hasZypper" != "0" ]; then
    $ZYPPER clean
    $ZYPPER refresh
    val=`$ZYPPER info "$MAPR_PREFIX-core" 2>/dev/null | grep "Version:" | grep "$version" | sort -u | tail -1`
  fi

  # If we did not find repo, create a generic repo file and attempt to use that
  if [ $? -ne 0 -o "$val" = "" ]; then
    WriteRepoFile $version
    if [ "$extension" = "deb" ]; then
      val=`$APT_CACHE show "$MAPR_PREFIX-core" 2>/dev/null | grep "Version:" | grep "$version" | sort -u | tail -1`
    elif [ "$hasYum" != "0" ]; then
      val=`$YUM list -q "$MAPR_PREFIX-core" 2>/dev/null | grep "$version" | sort -u | tail -1`
    elif [ "$hasZypper" != "0" ]; then
      val=`$ZYPPER info "$MAPR_PREFIX-core" 2>/dev/null | grep "Version:" | grep "$version" | sort -u | tail -1`
    fi

    # Still not found, exit
    if [ $? -ne 0 -o "$val" = "" ]; then
      errStr[$numErrs]="Could not find MapR packages with version $version in the repository. Check if the repository is correctly configured, and version $version is available"
      let "numErrs++"
      return 1
    fi
  fi

  version=`echo $val | awk '{print $2}' | sed -e s/-[0-9]$//g`
  local iv=`echo $version 2>/dev/null | awk -F. '{print $1}'`
  if [ $iv -lt $MIN_MAPR_VERSION ]; then
    errStr[$numErrs]="It is recommended to install MapR $MIN_MAPR_VERSION or higher"
    let "numErrs++"
  fi

  # Check if each package is available in repo
  for p in $packages; do
    if [ "$extension" = "deb" ]; then
      val=`$APT_CACHE show "$p" 2>/dev/null | grep "Version:" | grep "$version" | sort -u | tail -1`
    elif [ "$hasYum" != "0" ]; then
      val=`$YUM list "p" 2>/dev/null | grep "$version" | sort -u | tail -1`
    elif [ "$hasZypper" != "0" ]; then
      val=`$ZYPPER info "$p" 2>/dev/null | grep "Version:" | grep "$version" | sort -u | tail -1`
    fi

    if [ $? -ne 0 -o "$val" = "" ]; then
      errStr[$numErrs]="Package $p not found in repository."
      let "numErrs++"
    fi
  done
}

function CheckRepoLatest() {
  if [ "$extension" = "deb" ]; then
    $APT_GET clean 
    $APT_GET update
    val=`$APT_CACHE show "$MAPR_PREFIX-core" 2>/dev/null`
  elif [ "$hasYum" != "0" ]; then
    $YUM clean all
    val=`$YUM list -q "$MAPR_PREFIX-core" 2>/dev/null`
  elif [ "$hasZypper" != "0" ]; then
    $ZYPPER clean
    $ZYPPER refresh
    val=`$ZYPPER search "$MAPR_PREFIX-core" 2>/dev/null`
  fi

  # If we did not find repo, create a generic repo file and attempt to use that
  if [ $? -ne 0 -o "$val" = "" ]; then
    val=""
    timed_wait wget -q --no-glob -O /tmp/$pid.html $MAPR_URL 2>/dev/null
    if [ $? -eq 0 -a -f /tmp/$pid.html ]; then
      val=`grep "^<tr>" /tmp/$pid.html 2>/dev/null | grep -v "Parent Directory" | grep "a href=\"v" | sed -e 's/^[^0-9]*//g' | awk -F/ '{print $1}' | sort -u | tail -1`
    fi

    if [ "$val" = "" ]; then
      errStr[$numErrs]="Could not get the latest version of MapR in repository. Retry by specifying a version (-v)"
      let "numErrs++"
      return 1
    fi

    WriteRepoFile $val

    if [ "$extension" = "deb" ]; then
      val=`$APT_CACHE show "$MAPR_PREFIX-core" 2>/dev/null`
    elif [ "$hasYum" != "0" ]; then
      val=`$YUM list -q "$MAPR_PREFIX-core" 2>/dev/null`
    elif [ "$hasZypper" != "0" ]; then
      val=`$ZYPPER search "$MAPR_PREFIX-core" 2>/dev/null`
    fi

    if [ $? -ne 0 -o "$val" = "" ]; then
      errStr[$numErrs]="Could not find MapR packages in the repository. Configure the repository correctly and retry."
      let "numErrs++"
      return 1
    fi
  #else
    # TODO: Make sure repo has > 2.0
  fi

  # Check if each package is available in repo
  for p in $packages; do
    if [ "$extension" = "deb" ]; then
      val=`$APT_CACHE show "$p" 2>/dev/null | grep "Version:" | sort -u | tail -1`
    elif [ "$hasYum" != "0" ]; then
      val=`$YUM list "$p" 2>/dev/null | sort -u | tail -1`
    elif [ "$hasZypper" != "0" ]; then
      val=`$ZYPPER search "$p" 2>/dev/null | sort -u | tail -1`
    fi

    if [ $? -ne 0 -o "$val" = "" ]; then
      errStr[$numErrs]="Package $p not found in repository."
      let "numErrs++"
    fi
  done
}

function CheckRepo() {
  echo "[`date`] - Checking repository for packages ---"
  echo " --- Checking repository for packages ---" >&3

  # First check the Repo
  if [ "$version" != "" ]; then
    CheckRepoAndVersion
  else
    CheckRepoLatest
  fi

  if [ $numErrs -ne 0 ]; then
    echo "     Issues detected:" >&4
    echo "     ----------------" >&4
    for ((i=0; i < $numErrs; i++));do
       let "j=$i+1"
       echo "[`date`] ${errStr[$i]}"
       echo "     #$j) ${errStr[$i]}" >&4
    done
    UnLockExit 1
  fi
  EchoDone

}

function CheckPkgs() {
  local val=0
  local tmp_packages=""

  for p in $packages; do
    local newp=$p

    # Adjust package name if it doesn't begin with $MAPR_PREFIX-
    if [ "`echo $p | grep ^$MAPR_PREFIX-`" = "" ]; then
      newp="$MAPR_PREFIX-$p"
    fi
    tmp_packages=$tmp_packages" $newp"

    local found=0
    for v in ${ValidPkgs[*]}; do
      if [ "$newp" = "$v" ]; then
        found=1
        break
      fi
    done

    if [ $found -eq 0 ]; then
      errStr[$numErrs]="Invalid package $p"
      let "numErrs++"
      val=1
      continue
    fi

    
    # Check if packages are in directory
    if [ "$pkg_dir" != "" -a "$version" = "" ]; then
      if [ ! -f $pkg_dir/$newp*.$extension ]; then
        errStr[$numErrs]="Could not find package $p in $pkg_dir"
        let "numErrs++"
        val=1
        continue
      fi
    fi

    # If mapr-zk-internal is being installed, check for nc/netcat dependency
    if [ "$newp" = "$MAPR_PREFIX-zk-internal" -o "$newp" = "$MAPR_PREFIX-zookeeper" ]; then
      CheckAndInstallPkg nc
      if [ "$?" != "0" ]; then
        CheckAndInstallPkg netcat
        if [ "$?" != "0" ]; then
          errStr[$numErrs]="It appears like package nc or netcat is not installed. It is required for zookeeper"
          let "numErrs++"
          val=1
          continue
        fi
      fi
    fi

  done
  packages=$tmp_packages

  # TODO: If installing from directory, pick up the dependencies too if not already specified
  # If installing from repo, the dependencies are automatically installed.
  #if [ "$pkg_dir" != "" ]; then
  #  tmp_packages=""
  #  for p in $packages; do
  #    local newp=$p
  #  done
  #fi

  return $val
}

function CheckSingleNodeOptions() {
  echo "[`date`] - Checking command-line options ---"
  echo "" >&3
  echo " --- Checking command-line options ---" >&3

  if [ "$diskFile" = "" -a -f $TmpDiskFile ]; then
    diskFile=$TmpDiskFile
  fi

  if [ "$packages" = "" ]; then
    if [ "$prompt" = "0" ]; then
      errStr[$numErrs]="Need the $PkgMsg (-P)"
      let "numErrs++"
    else
      while [ 1 ]; do
        Prompt "Enter a $PkgMsg (Type help to list packages)"
        packages=`echo $inputStr | sed -e 's/,/ /g'`
        case "$packages" in
          "h"|"he"|"hel"|"help"|"l"|"li"|"lis"|"list")
            ListValidPkgs
            ;;
  
          * )
            CheckPkgs
            if [ $? -eq 0 ]; then
              break
            else
              if [ $numErrs -ne 0 ]; then
                echo "     Issues detected:" >&4
                echo "     ----------------" >&4
                for ((i=0; i < $numErrs; i++));do
                  let "j=$i+1"
                  echo "[`date`] ${errStr[$i]}"
                  echo "     #$j) ${errStr[$i]}" >&4
                done
              fi
              numErrs=0
              ListValidPkgs
            fi
            ;;
        esac
      done
    fi
  else
    CheckPkgs
  fi
 
  if [ "$pkg_dir" != "" -a "$version" != "" ]; then
    errStr[$numErrs]="Can specify either the install version (-v) or package directory (-p), not both"
    let "numErrs++"
  fi
  
  if [ $numErrs -ne 0 ]; then
    echo "     Issues detected:" >&4
    echo "     ----------------" >&4
    for ((i=0; i < $numErrs; i++));do
       let "j=$i+1"
       echo "[`date`] ${errStr[$i]}"
       echo "     #$j) ${errStr[$i]}" >&4
    done
    Usage 1
  fi
  EchoDone
}

function InstallSingleNode() {
  #
  # Exit if there is already another instance of this script running
  #
  LockForSingleInstance;
  if [ "$?" != "0" ]; then
    UnLockForSingleInstance;
    return $?
  fi

  if [ "$pkg_dir" = "" -a "$hasZypper" != "0" ]; then
    $ZYPPER cleanlocks 2>/dev/null
    $ZYPPER locks 2>/dev/null
    ret=$?
    if [ "$ret" != "0" ]; then
      echo "[`date`] Unable to acquire zypper lock for installation"
      echo "Unable to acquire zypper lock for installation" >&4
      UnLockForSingleInstance;
      return $ret
    fi
  fi

  #
  # Check if required options are specified.
  #
  CheckSingleNodeOptions
  if [ "$?" != "0" ]; then
    UnLockForSingleInstance;
    return $?
  fi
  
  #
  # Check if the repository is configured
  #
  if [ "$pkg_dir" = "" ]; then
    CheckRepo
    if [ "$?" != "0" ]; then
      UnLockForSingleInstance;
      return $?
    fi
  fi
  
  #
  # Run PreTest
  #
  if [ $force -eq 0 ]; then
    PreTest
    if [ "$?" != "0" ]; then
      UnLockForSingleInstance;
      return $?
    fi
  fi
  
  # Prevent interrupts
  trap '' 1 2 3 15

  #
  # Simulate install
  #
  SimulateInstall
  if [ "$?" != "0" ]; then
    UnLockForSingleInstance;
    return $?
  fi
  
  if [ $dry_run -eq 1 ]; then
    echo "[`date`] Successfully completed dry-run of MapR installation"
    echo "Successfully completed dry-run of MapR installation" >&3
    UnLockExit 0
  fi
  
  #
  # Do the actual install
  #
  ActualInstall
  if [ "$?" != "0" ]; then
    UnLockForSingleInstance;
    return $?
  fi
  
  echo "[`date`] Successfully installed and configured MapR on this node"
  echo "Successfully installed and configured MapR on this node" >&3
  UnLockForSingleInstance;
}

#############################################
######### Cluster install functions #########
#############################################

function SSHTest() {
  local node=$1
  if [ "$node" = "$localHost" ]; then
    mkdir -p $INSTALLSCRIPTS_DIR 2>/dev/null
    if [ "`pwd`" != $INSTALLSCRIPTS_DIR ]; then
      cp -f $THIS_SCRIPT $INSTALLSCRIPTS_DIR 2>/dev/null
    fi
    return 0
  fi

  local i=0
  while [ $i -lt ${#localIpList[*]} ]; do
    local n=${localIpList[$i]}
    if [ "$n" = "$node" ]; then
      mkdir -p $INSTALLSCRIPTS_DIR 2>/dev/null
      if [ "`pwd`" != $INSTALLSCRIPTS_DIR ]; then
        cp -f $THIS_SCRIPT $INSTALLSCRIPTS_DIR 2>/dev/null
      fi
      return 0
    fi
    let "i++"
  done

  echo "[`date`] Testing ssh to $node"
  ssh $ssh_options $username@$node "mkdir -p $INSTALLSCRIPTS_DIR"

  local ret=$?
  if [ $ret -ne 0 ]; then
    echo "" >&3
    echo "     ssh failed for $node, skipping install on $node" >&3
    echo "     Ensure $node is reachable, has passwordless ssh setup for user $username" >&3
  else
    scp $ssh_options $THIS_SCRIPT $username@$node:$INSTALLSCRIPTS_DIR
  fi
  return $ret
}

function SCPTest() {
  local node=$1
  if [ "do_ssh" = "0" -o "$node" = "$localHost" ]; then
    return 0
  fi

  local i=0
  while [ $i -lt ${#localIpList[*]} ]; do
    local n=${localIpList[$i]}
    if [ "$n" = "$node" ]; then
      return 0
    fi
    let "i++"
  done

  ssh $ssh_options $username@$node "mkdir -p $pkg_dir"
  echo "[`date`] Copying packages to $node"
  echo "------ Copying packages to $node" >&3
  scp $ssh_options $pkg_dir/$MAPR_PREFIX-* $username@$node:$pkg_dir
  return $?
}

function PrintSuccessOrFailure() {
  local node=$1
  local ret=$2
  local isLocalHost=$3

  if [ "$isLocalHost" = "1" ]; then
    if [ -e $LOG_FILE.1.summary ]; then >&3
      sed -e 's/^/    /' $LOG_FILE.1.summary 2>/dev/null >&3
    fi
  else
    rm -f /tmp/$THIS_SCRIPT.log.summary
    scp $ssh_options $username@$node:$LOG_FILE.summary /tmp/$THIS_SCRIPT.log.summary 2>/dev/null
    if [ "$?" = "0" -a -e /tmp/$THIS_SCRIPT.log.summary ]; then
      sed -e 's/^/    /' /tmp/$THIS_SCRIPT.log.summary 2>/dev/null >&3
    fi
    rm -f /tmp/$THIS_SCRIPT.log.summary
  fi

  if [ $ret -ne 0 ]; then
    echo "[`date`] Failed to install on node $node"
    echo "     Failed to install on node: $node" >&3
    failedNodes[$numFailedNodes]=$node
    let "numFailedNodes++"
  else
    echo "[`date`] Successfully installed on node $node"
    succesfulNodes[$numSuccessfulNodes]=$node
    let "numSuccessfulNodes++"
  fi
}

function InstallSingleNodeViaSSH() {
  local node=$1
  local i=$2
  local install_str="$clusterinstall_str -P ${hosts_packages[$i]}"
  if [ "${hosts_disks[$i]}" != "" ]; then
    install_str="$install_str -x ${hosts_disks[$i]}"
  fi

  local isLocalHost=0
  if [ "$node" = "$localHost" ]; then
    isLocalHost=1
  else
    local j=0
    while [ $j -lt ${#localIpList[*]} ]; do
      local n=${localIpList[$j]}
      if [ "$n" = "$node" ]; then
        isLocalHost=1
        break
      fi
      let "j++"
    done
  fi

  # Safety check
  if [ "$do_ssh" = "0" -a "$isLocalHost" = "0" ]; then
    return 0
  fi

  echo "[`date`] ------ Installing MapR on node: $node ------"
  echo "" >&3
  echo " --- Installing MapR on node: $node ---" >&3

  if [ "$isLocalHost" = "1" ]; then
    # Running on localnode, instead of spawning same script, pretend like local install
    #packages=`echo ${hosts_packages[$i]} | sed -e 's/,/ /g'`
    #echo ${hosts_disks[$i]} | sed -e 's/,/\n/g' > $TmpDiskFile
    #prompt=0
    #doExit=0
    #InstallSingleNode
    echo "     For install status, refer to $node:{$LOG_FILE.1, $LOG_FILE.1.summary}" >&3
    install_str="$install_str -l $LOG_FILE.1"
    timed_wait $install_str
  else
    echo "     For install status, refer to $node:{$LOG_FILE, $LOG_FILE.summary}" >&3
    timed_wait ssh $ssh_options $username@$node $install_str
  fi

  local ret=$?
  PrintSuccessOrFailure $node $ret $isLocalHost
  return $ret
}

function InstallMultipleNodes() {
  local i=0

  #
  # Check if required options are specified.
  #
  CheckClusterOptions

  # 
  # Parse Roles file
  #
  ParseRolesFile

  # Prevent interrupts
  trap '' 1 2 3 15

  #
  # Do the install
  #
  for node in ${hosts[*]}; do
    SSHTest $node
    if [ $? -ne 0 ]; then
      continue
    fi

    InstallSingleNodeViaSSH $node $i
    let "i++"
  done

  #
  # Print Summary report
  #
  InstallReport
}

function InstallReport() {
  # Print report
  echo "" >&3
  echo " === Install Report === " >&3
  echo "" >&3
  if [ $numFailedNodes -gt 0 ]; then
    echo "     Install failed on the following nodes:" >&3
    local i=0
    while [ $i -lt $numFailedNodes ]; do
       echo "     ${failedNodes[$i]}" >&3
       let "i++"
    done
    echo "" >&3
  fi
  if [ $numSuccessfulNodes -gt 0 ]; then
    echo "     Install succeeded on the following nodes:" >&3
    local j=0
    while [ $j -lt $numSuccessfulNodes ]; do
       echo "     ${succesfulNodes[$j]}" >&3
       let "j++"
    done
  fi
}

function CheckClusterOptions() {
  echo "[`date`] - Checking command-line options ---"
  echo "" >&3
  echo " --- Checking command-line options" >&3

  if [ "$roles_file" = "" ]; then
    # errStr[$numErrs]="Need the roles file (-R) for this node"
    # let "numErrs++"
    while [ 1 ]; do
      echo "$RolesFormatMsg" >&3
      echo "" >&3
      Prompt "Enter the $RolesMsg "
      roles_file=$inputStr
      if [ ! -f $roles_file ]; then
        # errStr[$numErrs]="File $roles_file does not exist"
        # let "numErrs++"
        echo "      $roles_file is not a valid roles file" >&4
      else
        break
      fi
    done
  fi

  if [ "$pkg_dir" != "" -a "$version" != "" ]; then
    errStr[$numErrs]="Can specify either the install version (-v) or package directory (-p), not both"
    let "numErrs++"
  fi

  if [ $numErrs -ne 0 ]; then
    echo "     Issues detected:" >&4
    echo "     ----------------" >&4
    for ((i=0; i < $numErrs; i++));do
       let "j=$i+1"
       echo "[`date`] ${errStr[$i]}"
       echo "     #$j) ${errStr[$i]}" >&4
    done
    Usage 1
  fi
  EchoDone
}

function NeedDisksFile() {
  local pkg=$1

  if [ "$pkg" = "$MAPR_PREFIX-fileserver" ]; then
    return 1
  fi

  if [ "$extension" = "deb" ]; then
    if [ "$pkg_dir" != "" ]; then
      dpkg-deb -I $pkg_dir/$pkg-*.$extension 2>/dev/null | grep -i depend | grep "$MAPR_PREFIX-fileserver"
      if [ "$?" = "0" ]; then
        return 1
      fi
    else
      $APT_CACHE depends $pkg 2>/dev/null | grep "$MAPR_PREFIX-fileserver"
      if [ "$?" = "0" ]; then
        return 1
      fi
    fi
  else
    if [ "$pkg_dir" != "" ]; then
      rpm -qR -p $pkg_dir/$pkg-*.$extension 2>/dev/null | grep "$MAPR_PREFIX-fileserver"
    elif [ "$hasYum" != "0" ]; then
      $YUM deplist $pkg 2>/dev/null | grep -i depend | grep "$MAPR_PREFIX-fileserver"
    elif [ "$hasZypper" != "0" ]; then
      $ZYPPER info --requires $pkg 2>/dev/null | grep "$MAPR_PREFIX-fileserver"
    fi
    if [ "$?" = "0" ]; then
      return 1
    fi
  fi

  return 0
}

function ParseRolesFile() {
  local ip=""
  local pkgs=""
  local disks=""
  local i=0

  echo " --- Parsing roles file " >&3

  while read ip pkgs disks; do
    # Ignore invalid lines
    if [ "$ip" = "" -o "$pkgs" = "" ]; then
      continue
    fi

    # Ignore commented lines
    if [[ "$ip" =~ \#.* ]]; then
      continue
    fi

    local packages[0]=""
    local badentry=0
    local nodiskfile=0
    local cldb=""
    local zk=""

    packages=(`echo $pkgs | sed -e 's/,/ /g'`)

    # More sanity check on the packages
    for p in ${packages[*]}; do
      local newp=$p

      # Adjust package name if it doesn't begin with mapr-
      if [ "`echo $p | grep ^$MAPR_PREFIX`" = "" ]; then
        newp="$MAPR_PREFIX-$p"
      fi

      local found=0
      for v in ${ValidPkgs[*]}; do
        if [ "$newp" = "$v" ]; then
          found=1
          break
        fi
      done

      if [ "$found" = "0" ]; then
        errStr[$numErrs]="Invalid package $p for node: $ip"
        let "numErrs++"
        badentry=1
        continue
      fi

      # If fileserver or a dependent package is specified and diskFile is empty, throw an error
      if [ "$disks" = "" -a "$nodiskfile" = "0" ]; then
        NeedDisksFile $p
        if [ "$?" != "0" ]; then               
          errStr[$numErrs]="Need a valid comma-separated list of disks for node: $ip since $MAPR_PREFIX-fileserver or a dependent package is to be installed"
          let "numErrs++"
          badentry=1
          nodiskfile=1
          continue
        fi
      fi

      if [ "$newp" = "$MAPR_PREFIX-cldb" ]; then
        cldb="$ip"
      elif [ "$newp" = "$MAPR_PREFIX-zookeeper" ]; then
        zk="$ip"
      fi
    done

    if [ "$badentry" = "0" ]; then
      hosts[$i]=$ip
      hosts_packages[$i]=$pkgs

      # TODO: How to handle comma-separated list of disks
      hosts_disks[$i]=$disks

      if [ "$cldb" != "" ]; then
        cldbNodesList="$cldbNodesList,$cldb"
      fi
      if [ "$zk" != "" ]; then
        zkNodesList="$zkNodesList,$zk"
      fi
      let "i++"
    fi
  done < $roles_file

  if [ $numErrs -ne 0 ]; then
    echo "     Issues detected:" >&4
    echo "     ----------------" >&4
    for ((i=0; i < $numErrs; i++));do
       let "j=$i+1"
       echo "[`date`] ${errStr[$i]}"
       echo "     #$j) ${errStr[$i]}" >&4
    done
    Usage 1
  fi

  numNodes=$i
  if [ "$numNodes" = "0" ]; then
    echo "     No valid entries found in roles file" >&4
    UnLockExit $1
  fi

  cldbNodesList=${cldbNodesList/^//}
  zkNodesList=${zkNodesList/^//}

  if [ "$cldbNodesList" = "" ]; then
    # errStr[$numErrs]="Need the list of CLDBs (-C) for this cluster"
    # let "numErrs++"
    echo "     Did not find any valid CLDB nodes in $roles_file" >&3
    Prompt "Enter a $CldbMsg"
    cldbNodesList=$inputStr
  fi
  clusterinstall_str="$clusterinstall_str -C $cldbNodesList "

  if [ "$zkNodesList" = "" ]; then
    # errStr[$numErrs]="Need the list of Zookeepers (-Z) for this cluster"
    # let "numErrs++"
    echo "     Did not find any valid Zookeeper nodes in $roles_file" >&3
    Prompt "Enter a $ZkMsg"
    zkNodesList=$inputStr
  fi
  clusterinstall_str="$clusterinstall_str -Z $zkNodesList "
  EchoDone
}

function SetupLog() {
  clear
  mkdir -p $INSTALL_DIR/logs 2>/dev/null

  touch ${LOG_FILE} 2>/dev/null
  if [ "$?" = "0" ]; then
    echo "Logging to ${LOG_FILE}, ${LOG_FILE}.summary"
    echo ""
    exec >${LOG_FILE} 2>&1
    set -x
  fi

  if [ "$*" != "" ]; then
    echo "Command: $*"
  fi
}



#############################################
############# Usage() and main() ############
#############################################

function ShortUsage() {
  echo "" >&4
  echo "Type $0 -h to see the usage" >&4
  UnLockExit 1
}

function Usage() {
  echo "" >&4
  echo "USAGE: $0 -[RPCZDFNfpvsnuh] [--isvm]
  [ -R <$RolesMsg> ]
    or
  [ -P <$PkgMsg> ]

  [ -C <$CldbMsg> ]
  [ -Z <$ZkMsg> ]
  [ -D <$DiskMsg> ]
  [ -F to format disks ]
  [ -N <cluster name> ]
  [ -f to avoid pre-requisite checks and force install ]
  [ -p <directory containing MapR packages, instead of using repository> ]
  [ -s Don't start MapR services on nodes ]
  [ -r URL for repository (eg: $MAPR_URL/v2.0.0/ubuntu/) ]
  [ -u username for SSH ]
  [ --isvm if installing MapR on VMs ]
  [ -h help ]

To install MapR on a cluster:
=============================
$RolesFormatMsg
#3) If no CLDB or Zookeeper role is specified in the roles file, use -C and -Z options 
to specify the list of CLDBs and Zookeepers respectively for this cluster.

To install MapR on a single node:
=================================
#1) Packages (-P), list of CLDBs (-C), list of Zookeepers (-Z) are required.
#2) Disks file (-D) is required if $MAPR_PREFIX-fileserver or a dependent package is installed." >&4

ListValidPkgsShort
UnLockExit $1
}

#
# Save stdout and stderr
#
cmd=$*
exec 3>&1
exec 4>&2

#
# Parse all options
#

while [ "${1:0:1}" = "-" ]; do
  case "$1" in
    
    "-P" )
      packages=$2
      if [ "$packages" = "" ]; then
        errStr[$numErrs]="Invalid packages specified with -P option"
        let "numErrs++"
        shift
        continue
      fi
      shift 2
      packages=`echo $packages | sed -e 's/,/ /g'`
      ;;

    * )
      errStr[$numErrs]="One or more invalid options specified"
      let "numErrs++"
      shift
      continue
      ;;
  esac
done

#
# If any options were invalid, display and exit
#
if [ $numErrs -ne 0 ]; then
  echo "Issues detected:" >&4
  echo "================" >&4
  for ((i=0; i < $numErrs; i++));do
     let "j=$i+1"
     echo "#$j) ${errStr[$i]}" >&4
  done
  Usage 1
fi

# 
# Check if running as Root
#
CheckForRoot

#
# Log to maprinstall.log
#
SetupLog $cmd

if [ -e $INSTALL_DIR/hostname ]; then
  localHost=`head -1 $INSTALL_DIR/hostname 2>/dev/null`
fi
if [ "$localHost" = "" ]; then
  localHost=`hostname -f 2>/dev/null`
fi

if [ "`which ifconfig`" != "" ]; then
  localIpList=(`ifconfig 2>/dev/null | grep "inet addr:" | awk -F: '{print $2}' | awk '{print $1}' | tr '\n' ' '`)
else
  localIpList=(`ip -f inet addr show 2>/dev/null | grep inet | awk '{print $2}' | cut -d'/' -f2 | tr '\n' ' '`)
fi

ret=0
InstallSingleNode

UnLockExit $ret