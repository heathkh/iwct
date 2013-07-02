.. _how_to_transfer_files:

************************************************************
How to transfer files to and from IWCT Workstations
************************************************************

Connect To Server
=================

Use the "Connect To Server" feature to easily drag and drop files from a remote server (via ssh, ftp, windows share).  To do this, click **Places** in the bar at the top and click **Connect To Server...** from the drop down menu.


SCP
===

Example to copy a file 

.. code:: bash
  
   scp  username1@source_host:directory1/filename1 username2@destination_host:directory2/filename2
   
   
Example to copy a directory recursively

.. code:: bash
  
   scp -r username1@source_host:directory1 username2@destination_host:directory2
   
.. note:: 
   
   Use the  **Options** menu in the :ref:`IWCT workstation consle <launch_workstation>` to generate the SSH or SCP command to copy files to / from your workstation.




