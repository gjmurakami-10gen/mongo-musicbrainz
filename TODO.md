# TO DO

* merge_n dbname from MONGODB_URI, eliminate MONGO_DBNAME
* retest 100000 for SLICE_SIZE and THRESHOLD

* mbdump update
  * CURRENT versus LATEST decoupling

# TOOLS

* generalize, unify, undo in Ruby
  * update instead of replace
    * individual docs
    * sort, group, in
  * port to mongo-c-driver, mongo-cxx-driver, mongo-cpp-driver
* bson_metrics
  * expand to mongometrics when Ruby 3.0 driver is functional
  * libbson version
* MD5SUM check
* load dependency

* bson-ruby examples
  * bson_metrics
  * bsondump
* mongo-ruby-driver examples
  * mongodump
* command-line option parsing - [Trollop](http://trollop.rubyforge.org/)

# SCHEMA NOTES

* [MusicBrainz Schema diagram](http://wiki.musicbrainz.org/-/images/5/52/ngs.png)
    * color coding
      * blue - core entities (9) - area artist label place recording release release_group url work

         19102427 total
         13102579 recording
          2291784 url
          1241852 release
          1020270 release_group
           819407 artist
           457407 work
            87908 area
            78167 label
             3053 place

      * yellow - mostly-static lists (21)
      * red - external identifiers (9)
    * Advanced Relationships (AR) - l_* combinations table count (45)
    * area(9), artist(8), label(7), place(6), recording(5), release(4), release_group(3), url(2), work(1)
    * [description](http://musicbrainz.org/doc/Next_Generation_Schema/Advanced_Relationships_Table_Structure)

* denormalization - start with manual
  * at start
    * 2 enum types
    * 119 tables
      * 74 original
        * 9 core
        * 21 mostly-static
        * 9 external identifiers
        * 17 other white
        * 1 annotation - unused - also area_annotation - unused
        * 7 link*
        * 10 *redirect
      * 45 Advanced Relationship
  * table status - make coherent, by merge, by DAG (core, static, external)
    * topological sort
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
* '_id' for gid and guid
* PK indexes
* references DAG
* CASCADE (?)

# MEASUREMENTS

rake load_tables
     5302.46 real      3548.50 user        35.60 sys

time rake merge_1 merge_n
    real 581m31.167s
    user 247m0.336s
    sys	32m47.260s
