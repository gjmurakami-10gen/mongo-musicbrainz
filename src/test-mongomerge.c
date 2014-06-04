/*
 * Copyright 2014 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * This program will scan each BSON document contained in the provided files
 * and print metrics to STDOUT.
 */

#include <mongoc.h>
#include <stdio.h>
#include <math.h>
#include "mongomerge.h"

const char *one_to_one_fixture = "\
{\
    \"before\": {\
        \"people\": [\
            {\"_id\": 11, \"name\": \"Joe\", \"gender\": 1, \"alias\": 1},\
            {\"_id\": 22, \"name\": \"Jane\", \"gender\": 2},\
            {\"_id\": 33, \"name\": \"Other\"}\
        ],\
        \"gender\": [\
            {\"_id\": 1, \"name\": \"Male\"},\
            {\"_id\": 2, \"name\": \"Female\"},\
            {\"_id\": 3, \"name\": \"Other\"}\
        ],\
        \"alias\": [\
            {\"_id\": 1, \"name\": \"Joseph\"}\
        ]\
    },\
    \"after\": {\
        \"people\": [\
            {\"_id\": 11, \"name\": \"Joe\", \"gender\": {\"_id\": 1, \"name\": \"Male\"}, \"alias\": {\"_id\": 1, \"name\": \"Joseph\"}},\
            {\"_id\": 22, \"name\": \"Jane\", \"gender\": {\"_id\": 2, \"name\": \"Female\"}},\
            {\"_id\": 33, \"name\": \"Other\"}\
        ]\
    }\
}";

const char *merge_one_spec[] = {
   "gender",
   "alias"
};

const char *one_to_many_fixture = "\
{\
    \"before\": {\
        \"owner\": [\
            {\"_id\": 11, \"name\": \"Joe\"},\
            {\"_id\": 22, \"name\": \"Jane\"},\
            {\"_id\": 33, \"name\": \"Jack\"},\
            {\"_id\": 44, \"name\": \"Other\"}\
        ],\
        \"pet\": [\
            {\"_id\": 1, \"name\": \"Lassie\", \"owner\": 11},\
            {\"_id\": 2, \"name\": \"Flipper\", \"owner\": 22},\
            {\"_id\": 3, \"name\": \"Snoopy\", \"owner\": 22},\
            {\"_id\": 4, \"name\": \"Garfield\", \"owner\": 33},\
            {\"_id\": 5, \"name\": \"Marmaduke\"}\
        ],\
        \"alias\": [\
            {\"_id\": 1, \"name\": \"Joseph\", \"owner\": 11},\
            {\"_id\": 2, \"name\": \"Janey\", \"owner\": 22},\
            {\"_id\": 3, \"name\": \"JJ\", \"owner\": 22},\
            {\"_id\": 5, \"name\": \"Jim\"}\
        ]\
    },\
    \"after\": {\
        \"owner\": [\
            {\"_id\": 11, \"name\": \"Joe\",\
             \"pet\": [\
                {\"_id\": 1, \"name\": \"Lassie\", \"owner\": 11}\
             ],\
             \"alias\": [\
                {\"_id\": 1, \"name\": \"Joseph\", \"owner\": 11}\
             ]\
            },\
            {\"_id\": 22, \"name\": \"Jane\",\
             \"pet\": [\
                {\"_id\": 2, \"name\": \"Flipper\", \"owner\": 22},\
                {\"_id\": 3, \"name\": \"Snoopy\", \"owner\": 22}\
             ],\
             \"alias\": [\
                {\"_id\": 2, \"name\": \"Janey\", \"owner\": 22},\
                {\"_id\": 3, \"name\": \"JJ\", \"owner\": 22}\
             ]\
            },\
            {\"_id\": 33, \"name\": \"Jack\",\
             \"pet\": [\
                {\"_id\": 4, \"name\": \"Garfield\", \"owner\": 33}\
             ]\
            },\
            {\"_id\": 44, \"name\": \"Other\"}\
        ]\
    }\
}";

const char *merge_many_spec[] = {
   "pet:[]",
   "alias:[]"
};

bool
do_fixture (mongoc_database_t *db,
            const char *fixture,
            const char *key,
            bool (*fn)(mongoc_database_t *db, bson_iter_t *))
{
   bson_t bson_fixture;
   bson_error_t error;
   bson_iter_t iter_fixture, iter_collection;
   bool ret = true;

   bson_init_from_json (&bson_fixture, fixture, strlen (fixture), &error) || WARN_ERROR;
   bson_iter_init_find (&iter_fixture, &bson_fixture, key) || DIE;
   BSON_ITER_HOLDS_DOCUMENT (&iter_fixture) || DIE;
   bson_iter_recurse (&iter_fixture, &iter_collection) || DIE;
   while (ret && bson_iter_next (&iter_collection))
       ret = fn (db, &iter_collection);
   bson_destroy (&bson_fixture);
   return ret;
}

bool
load_fixture_fn (mongoc_database_t *db,
                 bson_iter_t *iter_collection)
{
   const char *collection_name;
   mongoc_collection_t *collection;
   bson_error_t error;
   bson_iter_t iter_doc;
   bool ret = true;

   collection_name = bson_iter_key (iter_collection);
   collection = mongoc_database_get_collection (db, collection_name);
   mongoc_collection_drop (collection, &error);
   BSON_ITER_HOLDS_ARRAY (iter_collection) || DIE;
   bson_iter_recurse (iter_collection, &iter_doc) || DIE;
   while (ret && bson_iter_next (&iter_doc)) {
      bson_t *bson;

      bson = bson_new_from_iter_document (&iter_doc);
      ret = mongoc_collection_insert (collection, MONGOC_INSERT_NONE, bson, NULL, &error) || WARN_ERROR;
      bson_destroy (bson);
   }
   mongoc_collection_destroy (collection);
   return ret;
}

bool
check_fixture_fn (mongoc_database_t *db,
                  bson_iter_t *iter_collection)
{
   const char *collection_name;
   mongoc_collection_t *collection;
   bson_t *query;
   mongoc_cursor_t *cursor;
   bson_iter_t iter_doc;
   bool ret = true;
   bson_error_t error;

   collection_name = bson_iter_key (iter_collection);
   collection = mongoc_database_get_collection (db, collection_name);
   query = BCON_NEW ("$query", "{", "}", "$orderby", "{", "_id", BCON_INT32 (1), "}");
   cursor = mongoc_collection_find (collection, MONGOC_QUERY_NONE, 0, 0, 0, query, NULL, NULL);
   BSON_ITER_HOLDS_ARRAY (iter_collection) || DIE;
   bson_iter_recurse (iter_collection, &iter_doc) || DIE;
   while (ret && bson_iter_next (&iter_doc)) {
      const bson_t *db_doc;
      bson_t *fixture_doc;

      fixture_doc = bson_new_from_iter_document (&iter_doc);
      if (mongoc_cursor_next (cursor, &db_doc)) {
         ret = (bson_compare (fixture_doc, db_doc) == 0);
         if (!ret) {
            printf ("fixture comparison failed\n");
            bson_printf ("expected fixture doc: %s\n", fixture_doc);
            bson_printf ("actual db doc: %s\n", db_doc);
         }
      }
      else {
         ret = false;
         bson_printf ("expected fixture doc: %s\n", fixture_doc);
         printf ("actual db doc: %s\n", "(cursor next failed)");
      }
      bson_destroy (fixture_doc);
   }
   if (mongoc_cursor_error (cursor, &error)) {
      printf ("check_fixture cursor failure: %s\n", error.message);
      ret = false;
   }
   mongoc_cursor_destroy (cursor);
   bson_destroy (query);
   mongoc_collection_destroy (collection);
   return ret;
}

bool
clear_fixture_fn (mongoc_database_t *db,
                  bson_iter_t *iter_collection)
{
   mongoc_collection_t *collection;
   bson_error_t error;
   const char *collection_name;

   collection_name = bson_iter_key (iter_collection);
   collection = mongoc_database_get_collection (db, collection_name);
   mongoc_collection_drop (collection, &error);
   mongoc_collection_destroy (collection);
   return true;
}

void
test_merge (mongoc_database_t *db)
{
   do_fixture (db, one_to_one_fixture, "before", load_fixture_fn) || DIE;
   execute ("people", sizeof merge_one_spec / sizeof (char*), (char**) merge_one_spec);
   do_fixture (db, one_to_one_fixture, "after", check_fixture_fn) || DIE;
   do_fixture (db, one_to_one_fixture, "before", clear_fixture_fn);

   do_fixture (db, one_to_many_fixture, "before", load_fixture_fn) || DIE;
   execute ("owner", sizeof merge_many_spec / sizeof (char*), (char**) merge_many_spec);
   do_fixture (db, one_to_many_fixture, "after", check_fixture_fn) || DIE;
   do_fixture (db, one_to_many_fixture, "before", clear_fixture_fn);

   printf ("tests passed\n");
}

int
main (int argc,
      char **argv)
{
   const char *default_uristr = "mongodb://localhost/test";
   char *uristr;
   const char *database_name;
   mongoc_uri_t *uri;
   mongoc_client_t *client;
   mongoc_database_t *db;

   mongoc_init ();

   uristr = getenv ("MONGODB_URI");
   uristr = uristr ? uristr : (char*)default_uristr;
   uri = mongoc_uri_new (uristr);
   client = mongoc_client_new_from_uri (uri);
   database_name = mongoc_uri_get_database (uri);
   db = mongoc_client_get_database (client, database_name);

   test_merge (db);

   mongoc_database_destroy (db);
   mongoc_client_destroy (client);
   mongoc_uri_destroy (uri);

   mongoc_cleanup ();

   return 0;
}
