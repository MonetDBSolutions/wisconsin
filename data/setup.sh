#!/bin/bash

# This is generic script to instantiate a clean Wisconsin database on a number of database backends.
DB=monetdb

if [ $# == 0 ]
then
    echo "No target DBMS specified: monetdb, postgresql, sqlite"
    exit
fi

for n in `ls data/*schema.sql`
do
    echo "create a new database instance '${n}' for ${DB}"

    if [ ${DB} == 'monetdb' ]
    then
        monetdb stop wisconsin
        monetdb destroy wisconsin -f
        monetdb create wisconsin
        monetdb release wisconsin
        mclient -d wisconsin ${n}
    fi
    if [ ${DB} == 'postgresql' ]
    then
        echo "Not yet implemented"
    fi
    if [ ${DB} == 'sqlite' ]
    then
        echo "Not yet impemented"
    fi
done

# for now we assume that we only load into MonetDB
for f in `ls data/*load.sql`
do
    echo "Loading the table ${f}"
done