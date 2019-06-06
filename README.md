# The Wisconsin benchmark

This repository is set up to demonstrate the creation 
of a database instance with the seminal Wisconsin workload
as described in [DeWitt 1993](http://jimgray.azurewebsites.net/benchmarkhandbook/chapter4.pdf_) .

The data generator is available as a Python program in ./src
The data dirctory contains the data and scripts to instantiate
the database. To load the database patch the load.sql script 
to refer to absolute path names.

The information gathered by sqalpel is stored in the
sqalpel.yaml file. It is used to initialize a project.
