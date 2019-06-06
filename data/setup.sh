#!/bin/bash

# This is a MonetDB specific script to instantiate a clean Wisconsin database
# It requires a database id and a target DBMS

if [ $# == 0 ]
then
    echo "No target DBMS specified {monetdb,...}"
    exit
fi

for n in `ls *schema.sql`
do
    echo "create a new database instance for ${n{}"
    monetdb stop wisconsin
    monetdb destroy wisconsin -f
    monetdb create wisconsin
    monetdb release wisconsin
    mclient -d wisconsin ${n}
done