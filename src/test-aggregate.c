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
#include <stdlib.h>

double
dtimeofday ()
{
   struct timeval tv;
   bson_gettimeofday (&tv);
   return tv.tv_sec + 0.000001 * tv.tv_usec;
}

int64_t
mongoc_cursor_dump (mongoc_cursor_t *cursor)
{
   int64_t count = 0;
   const bson_t *doc;
   bson_error_t error;

   while (mongoc_cursor_next (cursor, &doc)) {
      const char *str;

      str = bson_as_json (doc, NULL);
      printf ("%s\n", str);
      bson_free ((void*)str);
      ++count;
   }
   if (mongoc_cursor_error (cursor, &error)) {
      fprintf (stderr, "Cursor failure: %s\n", error.message);
   }
   return count;
}

void test_suite (mongoc_database_t   *db,
                 mongoc_collection_t *collection)
{
   bson_error_t error;
   bson_t query = BSON_INITIALIZER;
   int64_t count;
   bson_t *options, *pipeline;
   double start_time, end_time, delta_time;
   mongoc_cursor_t *cursor;

   count = mongoc_collection_count (collection, MONGOC_QUERY_NONE, &query, 0, 0, NULL, &error);
   printf ("mongoc_collection_count count: %"PRId64"\n", count);
   options = BCON_NEW ("cursor", "{", "}", "allowDiskUse", BCON_BOOL (1));
   pipeline = BCON_NEW (
      "pipeline", "[",
          "{",
             "$match", "{",
             "}",
          "}",
          "{",
             "$project", "{",
                "text", BCON_INT32 (1),
             "}",
          "}",
       "]"
   );
   start_time = dtimeofday ();
   cursor = mongoc_collection_aggregate (collection, MONGOC_QUERY_NONE, pipeline, options, NULL);
   count = mongoc_cursor_dump (cursor);
   end_time = dtimeofday ();
   delta_time = end_time - start_time + 0.0000001;
   printf ("mongoc_cursor_dump: secs: %.2f, count: %"PRId64", %.2f docs/sec\n", delta_time, count, count/delta_time);
}

int
main (int   argc,
      char *argv[])
{
   const char *default_uristr = "mongodb://localhost/test";
   char *uristr;
   const char *database_name;
   mongoc_uri_t *uri;
   mongoc_client_t *client;
   mongoc_database_t *db;
   mongoc_collection_t *collection;

   mongoc_init ();

   uristr = getenv ("MONGODB_URI");
   uristr = uristr ? uristr : (char*)default_uristr;
   uri = mongoc_uri_new (uristr);
   client = mongoc_client_new_from_uri (uri);
   database_name = mongoc_uri_get_database (uri);
   db = mongoc_client_get_database (client, database_name);
   collection = mongoc_database_get_collection (db, "test");

   test_suite (db, collection);

   mongoc_collection_destroy (collection);
   mongoc_database_destroy (db);
   mongoc_client_destroy (client);
   mongoc_uri_destroy (uri);

   mongoc_cleanup ();

   return 0;
}
