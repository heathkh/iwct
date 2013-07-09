.. _view_results:

************
View Results
************

When the :command:`iwct_run_pipeline` command completes, you will have a :file:`results/html` directory with a self-contained static website that can be used to view the 
results.  Simply copy the directory to any webserver to publish your image-graph viewer on the web.

An easy way to view the result locally is to start a python webserver for the
html directory like this:

.. code-block:: bash

   cd /home/ubuntu/Desktop/datasets/{dataset_name}/results/html
   python -m SimpleHTTPServer
      
Now open a webbrowser to the url output by the  python command (e.g. http://localhost:8000).
   
.. note::

  The viewer inside the html directory is self-contained and portable.  Simply copy the html directory to any webserver to share your image-graph viewer with others.
