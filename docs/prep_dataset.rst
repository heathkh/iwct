.. _prep_dataset:

***************
Prepare dataset
***************

.. note::

  The following instructions should be completed from your IWCT Workstation (i.e. using the NX client as described in the :ref:`Setup Section <setup>`. Please open this page in a browser running in the IWCT Workstation for convenience.


You can either use an example image dataset or your own image files.


Optional - Prepare your own dataset
-----------------------------------

If you plan to use your own image collection, you'll need to place the images in an archive file (tar or zip).  The import tool will recursively search directories for all jpeg image files.

-- note:: 

  For convenience you can place your dataset archive file on a webserver so you can easily download it to your workstation using a URL in the next step.  Otherwise, you'll need to transfer it via scp to the workstation.
  
.. note::

  Use the "Connect To Server" feature to easily copy files to and from your IWCT Workstation.  To do this, click "Places" in the bar at the top and click "Connect To Server..." from the drop down menu.


Import Data
-----------

Open a terminal (right click on desktop and select "Open in terminal" from the context menu) and enter the following command.

.. code-block:: bash

   iwct_import_dataset

At the prompt ```Please enter url to dataset archive: ```, provide the url to your tar or zip image archive.
At the prompt ```Please enter name for dataset (no spaces): ```, provide a name for your dataset (letters, numbers, underscore are ok, but no spaces).

This creates a new directory /home/ubuntu/Desktop/datasets/<dataset_name> for your new dataset.






