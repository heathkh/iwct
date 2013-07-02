####################################
Image Webs Cloud Tools Documentation
####################################

The documentation explains how to build `Image Webs <http://graphics.stanford.edu/imagewebs>`_.


**Who should read this?**

  Anyone wanting to build an image web on their own images can follow the instructions in the :ref:`tutorial <tutorial>` section.  No programming is required.  The :ref:`manual <manual>` section is for users who want to hack on the code of the matching pipeline used to build the image webs itself.  All of the code is open source and `available on github <https://github.com/heathkh/iwct>`_.

**What is this?**

  This is the documentation for The Image Webs Cloud Toolkit (IWCT).  The IWCT creates a computing environment on Amazon's EC2 cloud with the Image Webs software pipeline ready to use.  This environment consists of two elements: the workstation and the cluster.  You connect to your workstation from your local machine.  From the workstation, you can prepare your datasets and launch a cluster to run the processing pipeline.

.. image:: _static/iwct_overview_diagram.png
   :width: 75%
   :align: center
   
.. cssclass:: table-hover
   :align: center
   
    +-------------+----------------------------------------------------------------+
    |Machine      | Purpose                                                        |
    +=============+================================================================+
    |workstation  | A persistent remote workstation setup for you in the AWS cloud.|
    +-------------+----------------------------------------------------------------+
    |cluster      | A temporary cluster setup for you in the AWS cloud on-demand.  |
    +-------------+----------------------------------------------------------------+

**How much does it cost?**

The IWCT software is open-source and free to use and improve.  However, you pay amazon for whatever resources you use.  Using the default settings, it costs $1 per day to run the workstation and about $5 per hour to run a cluster with 144 cores.  See the :ref:`estimating costs <costs>` section for details.

Contents
--------

.. toctree::
   :maxdepth: 2
   
   tutorial
   manual
   guides
   
   

