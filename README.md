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

## Drivers
The experiments are ran against a local database system gathering its
tasks by contacting a Sqalpel web-server or running locally
the experiments in the sqalpel.yaml file.

Most drivers are structural identical, with minor twists 
depending on the DBMS being addressed. A few specific drivers
are included as examples on how to create them.

## Installation
Setting up theaa environment involves a few simple steps.
First make sure you have pipenv installed on your machine and that the pipenv command can be found in your $PATH.
If you do a user installation, you will need to add the right folder to your PATH variable.

```
  pip3 install --user pipenv
  PYTHON_BIN_PATH="$(python3 -m site --user-base)/bin"
  export PATH="$PATH:$PYTHON_BIN_PATH"
  which pipenv
```
Thereafter you can checkout this repository in a local directory, activate the virtual environment
and you are ready to launch the experiment driver.
```
  pipenv shell
  cd drivers
  python squll.py --help
```

