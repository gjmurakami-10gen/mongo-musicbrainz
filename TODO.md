# TO DO

* denormalization - start with manual
  * at start - 119 tables + 2 enum types
  * one-to-one
    * large merge
      ['medium_cdtoc.cdtoc', 'cdtoc._id']
  * one-to-many
    * THRESHOLD to fetch all
  * many-to-many (join tables)
    * artist_credit
    * artist_credit_name
    * release_country
    * release_label
    * track
    * medium
* '_id' for gid and guid
* PK indexes
* references DAG

* MD5SUM check
* load dependency

* CASCADE (?)

* bson-ruby examples
  * bsondump.rb
* mongo-ruby-driver examples
  * mongodump

rake load_tables
     5302.46 real      3548.50 user        35.60 sys
