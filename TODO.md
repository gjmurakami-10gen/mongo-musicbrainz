# TO DO

* denormalization - start with manual
  * at start - 119 tables + 2 enum types
  * name
    * merge - pro: not overloaded, con: non-descript
    * denormalize - pro: common use include modifying, con: long
    * join - pro: common use, con: non-modifying
  * merge order - topological sort
  * 1_1
  * 1_n
  * many-to-many (join tables)
    * artist_credit
    * artist_credit_name
    * release_country
    * release_label
  * Advanced Relationships (AR)
    * [description](http://musicbrainz.org/doc/Next_Generation_Schema/Advanced_Relationships_Table_Structure)
* '_id' for gid and guid
* PK indexes
* references DAG

* tools
  * generalize, unify, undo in Ruby
  * port to mongo-c-driver, mongo-cxx-driver, mongo-cpp-driver
  * bson-metrics
    * expand to mongometrics when Ruby 3.0 driver is functional
* MD5SUM check
* load dependency

* CASCADE (?)

* bson-ruby examples
  * bson_metrics
  * bsondump
* mongo-ruby-driver examples
  * mongodump

rake load_tables
     5302.46 real      3548.50 user        35.60 sys
