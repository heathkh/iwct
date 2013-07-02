.. _prep_dataset:

***************
Prepare dataset
***************

.. note::

  Perform the following steps from your *IWCT Workstation* (i.e. :ref:`connect using the NX client <launch_workstation>`).  You may want to open this page in a browser running on the IWCT Workstation for convenience.


You can either use an default example image dataset or provide your own image files.


Optional - Prepare your own dataset
-----------------------------------

To use your own image collection, you'll need to place jpeg format image files in an archive file (tar or zip).  Any directory structure is fine, non-JPEG files will be ignored.  
  

.. note:: 

  For convenience, place your .zip or .tar file on a webserver so you can easily download it to your workstation using in the next step.   

Import Data
-----------

Open a terminal (right click on desktop and select **Open in terminal** from the context menu) and enter the following command.

.. code-block:: bash

   iwct_import_dataset

At the prompt ``Please enter url to dataset archive:``, provide the path to your tar or zip image archive in URL format (or use the URL for the example dataset). 


.. note::

   If your file is copied locally on the workstation use the URL prefix file:// instead of http://.



At the prompt ``Please enter name for dataset (no spaces):``, provide a name for your dataset (no spaces - only letters, numbers, underscores).

This creates a new dataset directory :file:`/home/ubuntu/Desktop/datasets/{dataset_name}` containing a PERT file :file:`imageid_to_image.pert` containing your dataset images. 






