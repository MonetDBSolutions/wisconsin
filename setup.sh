#!/bin/bash

# This is generic script to instantiate a clean Wisconsin database on a number of database backends.
DB=monetdb

if [ $# == 0 ]
then
    echo "No target DBMS specified: monetdb, postgresql, sqlite"
    exit
fi


echo "create a new database instance for ${DB}"

if [ ${DB} == 'monetdb' ]
then
    monetdb destroy wisconsin -f
    monetdb create wisconsin
    monetdb release wisconsin
    mclient -d wisconsin data/schema.sql
fi
if [ ${DB} == 'postgresql' ]
then
    echo "Not yet implemented"
fi
if [ ${DB} == 'sqlite' ]
then
    echo "Not yet impemented"
fi

# for now we assume that we only load into MonetDB
# Change the relative paths into full paths

mclient -d wisconsin data/load.sql