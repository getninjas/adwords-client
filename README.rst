AdWords Client
==============

.. image:: https://travis-ci.org/getninjas/adwords-client.svg?branch=master
   :target: https://travis-ci.org/getninjas/adwords-client

.. image:: https://badges.gitter.im/getninjas/adwords-client.svg
   :target: https://gitter.im/getninjas/adwords-client?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge

Setup
=====

This package can be installed with pip or directly from this repo. To do so, just run

.. code:: bash

   pip install adwords-client

Or

.. code:: bash

   pip install -e git+https://github.com/getninjas/adwords-client.git#egg=adwords-client

Usage
=====

In order to test and run this package, you need some credentials from Google AdWords as
described here https://developers.google.com/adwords/api/docs/guides/signup.

By default, the ``autoload()'' method of the client looks for credentials in the following
environment variables:

.. code:: bash

   DEVELOPER_TOKEN=***developer token here***
   CLIENT_CUSTOMER_ID=***id of the addcount you wish to manage by default***
   CLIENT_ID=***client id for the google API credentials***
   CLIENT_SECRET=***client secret for the google API credentials***
   REFRESH_TOKEN=***refresh token that should be manually generated***

If these are not present, it fallsback to the file ``googleads.yaml'' in the project folder and,
if it does not exists, it looks for ``googleads.yaml'' inside the current users' home folder.
This file is as defined in the exaples at https://github.com/googleads/googleads-python-lib.
If none of these options are available, it raises an error.
