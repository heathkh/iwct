.. _launch_cluster:

**************
Launch cluster
**************

""""""""""""""""""""""""""

Login or register a new account at http://www.mapr.com/Log-yourself-in-mapr

.. image:: _static/register_with_mapr.png
   :width: 100%
   :align: center

""""""""""""""""""""""""""



Open a terminal, and start a cluster with 5 nodes

.. code-block:: bash
  
  iwct_cluster create 5

When the prompt shows this message, open the indicated URL in a browser.

.. image:: _static/master_ready_console.png
   :width: 100%
   :align: center

.. hint::

  Hover and right click on the URL and select **Open link in browser** from the context menu.


""""""""""""""""""""""""""

Click the **Proceed anyway** button.

.. note:: You can safely ignore the scary looking SSL warning.

.. image:: _static/ssl_warning.png
   :width: 100%
   :align: center

""""""""""""""""""""""""""

Login as the root user.

.. cssclass:: table-hover
   :align: center
    
    +-------------+--------+
    |**username** | root   |
    +-------------+--------+
    |**password** | mapr   |
    +-------------+--------+


.. image:: _static/login.png
   :width: 100%
   :align: center
   
""""""""""""""""""""""""""

Click the **Add license from web** button. 

.. image:: _static/add_license.png
   :width: 100%
   :align: center

""""""""""""""""""""""""""

If not yet done, create a MapR account (set the remember me option to skip this step next time).

""""""""""""""""""""""""""

Select **M3** license and click the **Register** button.

.. image:: _static/register_cluster.png
   :width: 100%
   :align: center
   
""""""""""""""""""""""""""

Click the **Return to your MapR Cluster Ui** link.

""""""""""""""""""""""""""

Click the **Apply Licenses** button.

.. image:: _static/apply_license.png
   :width: 100%
   :align: center
   
""""""""""""""""""""""""""

Return to the console and press ENTER key to continue...

.. image:: _static/continue_console.png
   :width: 100%
   :align: center

""""""""""""""""""""""""""

When the script finishes, the MapR control panel should look like this (one green square for each node you requested).


.. image:: _static/mapr_console.png
   :width: 100%
   :align: center


