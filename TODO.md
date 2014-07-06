# TO DO

* mbdump_to_mongo - C language version
  * pending - bson_append_date_time_from_s
  * fix memory leaks
  * ulimit -c unlimited
* mongomerge - C language version
  * skip merge_one_all if no one merges
  * size_t n_docs printf format
  * bson_new_from_iter_array, bson_append_iter
  * measure
  * profiling
  * documentation

* User Interface
  * reconsider with origin from both AR and MongoDB from scratch
  * USAGE

      usage: MONGODB_URI='mongodb://localhost:27017/database_name' #{$0} parent_collection merge_spec ...
      where: merge_spec: merge_one_spec | merge_many_spec
             merge_one_spec: foreign_key:child_collection.child_key
               child_collection default foreign_key
               child_key default parent_collection
             merge_many_spec: key:[child_collection.foreign_key]
               child_collection default key
               foreign_key default parent_collection
      examples:
        area
          type:area_type
          area_alias:[]
          place:[]
          gid_redirect:[artist_gid_redirect.new_id]
        area_alias
          type:area_alias_type
          alias:[area_alias]
        artist
          type:artist_type
          gender
          area
          alias:[artist_alias]
          credit_name:[artist_credit_name]
          ipi:[artist_ipi]
          isni:[artist_isni]
          gid_redirect:[artist_gid_redirect.new_id]
        country_area
          area
        label
          type:label_type

* aggregation exploration
* Rakefile desc
* musicbrainz core tables merger review
* submodules - musicbrainz-server libbson mongo-c-driver
* topological sort optimization to replace repeated merges
* rspec studying
* optimize
  * merge_1
  * merge_n
  * mbdump_to_mongo
* merge improvements
  * multi-merge
  * --progress option
  * parallel merge
* profile
* mongo-c-driver mbdump_to_mongo
* dbname in command line args
* rake indexes
* Advanced Relationships

* mbdump update
  * CURRENT versus LATEST decoupling

# MEASUREMENTS

MacBook Pro Retina, 15-inch, Late 2013 2.6 GHz Intel Core i7

* rake load_tables

     7244.43 real      3685.49 user        56.81 sys

* rake merge_1

    real	66m21.664s
    user	24m5.080s
    sys	    3m39.035s

* rake merge_n

    real	192m45.412s
    user	35m53.214s
    sys	    5m45.928s

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

* [MusicBrainz Search](http://musicbrainz.org/search)
* [MusicBrainz Schema diagram](http://wiki.musicbrainz.org/-/images/5/52/ngs.png)
    * color coding
      * blue - core entities (9) - area artist label place recording release release_group url work

         19102427 total
         13102579 recording
          2291784 url -
          1241852 release -
          1020270 release_group
           819407 artist
           457407 work
            87908 area -
            78167 label -
             3053 place -

        * url by gid - all incorrect - grep GID mbdump/* shows GID use is PK and not FK
            *
        * merge_1
        ['artist.area', 'area._id'],
        ['country_area.area', 'area._id'],
        ['label.area', 'area._id'],
        ['release_label.label', 'label._id'],
        * merge_n
        ['area.place', 'place.area'],
        ['release_group.release', 'release.release_group'],

        * remaining - artist release_group recording work
        * study artist-album-track examples

        * review - release_label

      * yellow - mostly-static lists (21)
      * red - external identifiers (9)
      * join (non-yellow, multiple refs out) - artist_credit_name medium_cdtoc release_country release_label track
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
