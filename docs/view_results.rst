.. _view_results:

************
View Results
************

When the iwct_run_pipeline command completes, you can open the image graph viewer by entering the following commands:

.. code-block:: bash

   cd /home/ubuntu/Desktop/datasets/<dataset_name>/results/html
   python -m SimpleHTTPServer
   
   
Now open a webbrowser to the url output by the previous command

.. code-block:: bash
   
   chromium-browser localhost:8000
   
   
 .. note::

  The viewer inside the html directory is self-contained and portable.  Simply place it on any webserver to share your image-graph viewer with others. 
