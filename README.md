# The Wisconsin benchmark

This repository is set up to ease the creation 
of a database instance with the seminal Wisconsin workload
as described in [DeWitt 1993](http://jimgray.azurewebsites.net/benchmarkhandbook/chapter4.pdf_) .

The data generator is available as a Python program in ./src
For each database system considered, we introduce a directory
to hold the schema, data and query file. Systems my have slightl
different SQL syntax.

The .sqalpel.yml file contains the minimal configuration
to create a SQALPEL project.
