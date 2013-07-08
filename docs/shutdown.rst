.. _shutdown:

********
Shutdown
********

When the you are finished running the image webs pipeline, don't forget to terminate the cluster.  Otherwise, you may end up with a LARGE bill.


.. code-block:: bash

   cirrus_cluster_cli destroy
      
.. warning::

  All files on the maprfs:// file system will be lost, be sure any data you wish to retain is copied to the IWCT workstation.


When you are finished using the workstation, click the "Turn Off" button in the IWCT Console.  When your workstation is shut-off, you are not charged for instance-hours but continue to pay for retaining the instance storage (on an EBS volume).  You can later return to the IWCT Console and click "Turn On" to start working where you left off.  When you are done with the workstation and no longer wish to retain the storage, Open the **Options** menu and click **Destroy** to delete the workstation and storage.

.. warning::
   You are responsible for any charges you accrue for use of AWS resources.  When you complete a work session, always login to your `AWS management console <https://console.aws.amazon.com/ec2/v2/>`_ and check that your workstation instances are stopped and your cluster instances terminated to avoid accidently wasting resources.          
 

