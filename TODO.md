# TO DO

* denormalization - start with manual
  * at start - 119 tables + 2 enum types
  * enum types
  * one-to-one
  * one-to-many

* '_id' for gid and guid
* PK indexes
* references DAG

* MD5SUM check
* load dependency
* branching metrics
  * count by type
  * denormalization - (sub-doc-count + array-count) / doc-count
  * flatness - element-count / (doc-count + sub-doc-count + array-count)

* CASCADE (?)

rake load_tables
     5302.46 real      3548.50 user        35.60 sys
