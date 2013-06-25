.. _prep_dataset:

***************
Prepare dataset
***************

.. note::

  The following instructions should be completed from your IWCT Workstation (i.e. using the NX client as described in the :ref:`Setup Section <setup>`. Please open this page in a browser running in the IWCT Workstation for convenience.


You can either use an example image dataset (Option A), or  an archive (tar, zip) of your own image files (Option B).


Option A - Download example data
--------------------------------

To fetch an example dataset, open a terminal and enter

.. code-block:: bash

   cd ~/Desktop
   wget -r http://graphics.stanford.edu/tide/releases/tide-example.tar
   tar xvf tide-example.tar


Option B - Use your own data
----------------------------

To use your own dataset, first pack your data into a archive file (tar, zip) and copy it to your workstation desktop.  Let's assume your archive is named my_photos.zip and stored on the desktop.  Open a terminal (right click on desktop and select "Open in terminal" from the context menu) and enter the following command.

.. code-block:: bash

   iwct_pack_images my_photos.zip

This will create a new folder in the same directory named my_photos_dataset.

.. note::

  Use the "Connect To Server" feature to easily copy files to and from your IWCT Workstation.  Open the File Manager and click "Connect To Server".


