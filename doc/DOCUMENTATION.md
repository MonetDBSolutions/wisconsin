The Wisconsin Benchmark  is the first effort to systematically measure and 
compare the performance of relational
database systems with database machines.  The benchmark is a
single-user and single-factor experiment using a synthetic database
and a controlled workload.  It measures the query optimization
performance of database systems with 32 query types to exercise the
components of the proposed systems.  The query suites include
selection, join, projection, aggregate, and simple update queries.

The original test database consists of four generic relations.  The tenk
relation is the key table and most used. Two data types of small
integer numbers and character strings are utilized.  Data values are
uniformly distributed. The primary metric is the query elapsed
time. 

The main criticisms of the benchmark include the nature of
single-user workload, the simplistic database structure, and the
unrealistic query tests.  A number of efforts have been made to extend
the benchmark to incorporate the multi-user test.  However, they do
not receive the same acceptance as the original Wisconsin benchmark
except an extension work called the AS3AP benchmark.

This version of the Wisconsin benchmark is not meant to exercise the
current state of affairs in database management systems, but rather
acts as a demonstration of the Sqalpel functionality.

[Bitton, DeWitt, and Turbyfill 1983](http://www.vldb.org/conf/1983/P008.PDF) 
[Boral and DeWitt 1984](https://dl.acm.org/citation.cfm?doid=602259.602283)
[Bitton and Turbyfill 1988](https://dl.acm.org/citation.cfm?id=48770), and 
[DeWitt 1993](http://jimgray.azurewebsites.net/benchmarkhandbook/chapter4.pdf_)