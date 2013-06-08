CMAKE_MINIMUM_REQUIRED(VERSION 2.6)

PY_LIBRARY(
  NAME      library
  SOURCES   add_host        easy_install  hg          pip              subversion
            apt             ec2           ini_file    pkgin            supervisorctl
            apt_key         ec2_facts     lineinfile  postgresql_db    svr4pkg
            apt_repository  facter        mail        postgresql_user  sysctl
            assemble        fail          mount       raw              template
            async_status    fetch         mysql_db    script           user
            async_wrapper   file          mysql_user  seboolean        virt
            authorized_key  fireball      nagios      selinux          wait_for
            command         get_url       ohai        service          yum
            copy            git           pacman      setup
            cron            group         pause       shell
            debug           group_by      ping        slurp    
)

