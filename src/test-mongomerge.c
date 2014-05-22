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
#include <bcon.h>
#include "mongomerge.h"


#define assert_cmpstr(a, b)                                             \
   do {                                                                 \
      if (((a) != (b)) && !!strcmp((a), (b))) {                         \
         fprintf(stderr, "FAIL\n\nAssert Failure: \"%s\" != \"%s\"\n",  \
                         a, b);                                         \
         abort();                                                       \
      }                                                                 \
   } while (0)


#define assert_cmpint(a, eq, b)                                         \
   do {                                                                 \
      if (!((a) eq (b))) {                                              \
         fprintf(stderr, "FAIL\n\nAssert Failure: "                     \
                         #a " " #eq " " #b "\n");                       \
         abort();                                                       \
      }                                                                 \
   } while (0)


#ifdef BSON_OS_WIN32
#include <stdarg.h>
#include <share.h>
static __inline int
bson_open (const char *filename,
           int flags,
           ...)
{
   int fd = -1;
   int mode = 0;

   if (_sopen_s (&fd, filename, flags|_O_BINARY, _SH_DENYNO, _S_IREAD | _S_IWRITE) == NO_ERROR) {
      return fd;
   }

   return -1;
}
# define bson_close _close
# define bson_read(f,b,c) ((ssize_t)_read((f), (b), (int)(c)))
# define bson_write _write
#else
# define bson_open open
# define bson_read read
# define bson_close close
# define bson_write write
#endif

void load_test_single(mongoc_collection_t *collection)
{
   bson_error_t error;
   bson_t *doc;

   mongoc_collection_drop (collection, &error);

   doc = BCON_NEW ("hello", BCON_UTF8 ("world"),
                   "count_down", "[", BCON_INT32(3), BCON_INT32(2), BCON_INT32(1), "]",
                   "goodbye", "{", "exit_code", BCON_INT32(0), "}");
   mongoc_collection_insert (collection, MONGOC_INSERT_NONE, doc, NULL, &error) || WARN_ERROR;
   bson_destroy (doc);

   mongoc_collection_dump (collection);
}

void load_test_bulk(mongoc_collection_t *collection)
{
   bson_error_t error;
   uint32_t n_docs, i;
   bson_t *docs[4];

   mongoc_collection_drop (collection, &error);

   n_docs = 0;
   docs[n_docs++] = BCON_NEW ("_id", BCON_INT32 (11), "name", BCON_UTF8 ("Joe"));
   docs[n_docs++] = BCON_NEW ("_id", BCON_INT32 (22), "name", BCON_UTF8 ("Jane"));
   docs[n_docs++] = BCON_NEW ("_id", BCON_INT32 (33), "name", BCON_UTF8 ("Jack"));
   docs[n_docs++] = BCON_NEW ("_id", BCON_INT32 (44), "name", BCON_UTF8 ("Other"));
   mongoc_collection_insert_bulk (collection, MONGOC_INSERT_NONE, (const bson_t**)docs, n_docs, NULL, &error) || WARN_ERROR;
   for (i = 0; i < n_docs; i++)
        bson_destroy (docs[i]);

   mongoc_collection_dump (collection);
}

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

void load_fixture (mongoc_database_t *db, const char *fixture, const char *key)
{
   mongoc_collection_t *collection;
   bson_error_t error;
   bson_t bson_fixture;
   bson_iter_t iter_fixture, iter_collection;

   bson_init_from_json (&bson_fixture, fixture, strlen (fixture), &error) || WARN_ERROR;
   bson_iter_init_find (&iter_fixture, &bson_fixture, key) || DIE;
   BSON_ITER_HOLDS_DOCUMENT (&iter_fixture) || DIE;
   bson_iter_recurse (&iter_fixture, &iter_collection) || DIE;
   while (bson_iter_next (&iter_collection)) {
       const char *collection_name;
       bson_iter_t iter_doc;
       collection_name = bson_iter_key (&iter_collection);
       printf ("collection_name: \"%s\"\n", collection_name);
       collection = mongoc_database_get_collection (db, collection_name);
       mongoc_collection_drop (collection, &error);
       BSON_ITER_HOLDS_ARRAY (&iter_collection) || DIE;
       bson_iter_recurse (&iter_collection, &iter_doc) || DIE;
       while (bson_iter_next (&iter_doc)) {
          bson_t *bson = bson_new_from_iter_document (&iter_doc); // review
          mongoc_collection_insert (collection, MONGOC_INSERT_NONE, bson, NULL, &error) || WARN_ERROR;
          bson_destroy (bson);
       }
       mongoc_collection_dump (collection);
       mongoc_collection_destroy (collection);
   }
   bson_destroy (&bson_fixture);
}

void test_pipeline (mongoc_collection_t *collection)
{
   bson_t *pipeline;
   mongoc_cursor_t *cursor;

   pipeline = BCON_NEW (
      "pipeline", "[",
         "{", "$match", "{", "name", "Jack", "}", "}",
      "]"
   );
   bson_printf ("test_pipeline: %s\n", pipeline);
   cursor = mongoc_collection_aggregate_pipeline(collection, MONGOC_QUERY_NONE, pipeline, NULL, NULL);
   assert (cursor);
   mongoc_cursor_dump (cursor);
   mongoc_cursor_destroy (cursor);
   bson_destroy (pipeline);
}

const char *merge_one_spec[] = {
   "gender",
   "alias"
};

const char *merge_many_spec[] = {
   "pet:[]",
   "alias:[]"
};

static ssize_t
test_reader_from_handle_read(void * handle, void * buf, size_t len)
{
   return bson_read(*(int *)handle, buf, len);
}

static void
test_reader_from_handle_destroy(void * handle)
{
   bson_close(*(int *)handle);
}

double
dtimeofday () {
   struct timeval tv;
   bson_gettimeofday (&tv, NULL);
   return tv.tv_sec + 0.000001 * tv.tv_usec;
}

static void
collection_load_from_file (mongoc_collection_t *collection, const char *file_name)
{
   bson_reader_t *reader;
   bson_t *doc;
   bool eof = false;
   int fd;
   bson_error_t error;

   fd  = bson_open(file_name, O_RDONLY);
   assert(-1 != fd);

   reader = bson_reader_new_from_handle ((void *)&fd, &test_reader_from_handle_read, &test_reader_from_handle_destroy);
   mongoc_collection_drop (collection, &error);
   while (1) {
      doc = (bson_t*)bson_reader_read(reader, &eof);
      if (!doc || eof)
         break;
      //bson_printf("collection_load_from_file: %s\n", b);
      mongoc_collection_insert (collection, MONGOC_INSERT_NONE, doc, NULL, &error) || WARN_ERROR;
   }

   bson_reader_destroy(reader);
}

void test_merge (mongoc_database_t *db, mongoc_collection_t *collection)
{
   bson_error_t error;
   load_test_single (collection);
   load_test_bulk (collection);
   load_fixture (db, one_to_one_fixture, "before");
   load_fixture (db, one_to_many_fixture, "before");
   test_pipeline (collection);
   bson_t *bson = child_by_merge_key("parent", "child", "key");
   bson_printf("child_by_merge_key: %s\n", bson);
   bson_destroy (bson);
   bson = parent_child_merge_key("parent", "child", "key");
   bson_printf("parent_child_merge_key: %s\n", bson);
   bson_destroy (bson);
   bson_t *accumulators = BCON_NEW("hello", "world");
   bson_t *projectors = BCON_NEW("hello", "world");
   bson = merge_one_all(accumulators, projectors);
   bson_printf("merge_one_all: %s\n", bson);
   bson_destroy (accumulators);
   bson_destroy (projectors);
   bson = copy_many_with_parent_id("parent", "child", "key");
   bson_printf("merge_one_all: %s\n", bson);
   bson_destroy (bson);
   bson = expand_spec ("people", sizeof(merge_one_spec)/sizeof(char*), (char**) merge_one_spec);
   bson_printf("expand_spec people: %s\n", bson);
   bson_destroy (bson);
   bson = expand_spec ("owner", sizeof(merge_many_spec)/sizeof(char*), (char**) merge_many_spec);
   bson_printf("expand_spec owner: %s\n", bson);
   bson_destroy (bson);
   execute ("people", sizeof(merge_one_spec)/sizeof(char*), (char**) merge_one_spec);
   execute ("owner", sizeof(merge_many_spec)/sizeof(char*), (char**) merge_many_spec);
   collection_load_from_file (collection, "../twitter.bson");
   mongoc_collection_t *temp_coll = mongoc_database_get_collection (db, "temp");
   mongoc_collection_drop (temp_coll, &error);
   double start_time = dtimeofday();
   bson_t query = BSON_INITIALIZER;
   mongoc_cursor_t *cursor = mongoc_collection_find (collection, MONGOC_QUERY_NONE, 0, 0, 0, &query, NULL, NULL);
   int64_t count = mongoc_cursor_insert (cursor, temp_coll, NULL, &error);
   double end_time = dtimeofday();
   double delta_time = end_time - start_time + 0.0000001;
   printf("mongoc_cursor_insert: docs: %lld, %.2f docs/sec\n", count, count/delta_time);
}

int
main (int argc,
      char *argv[])
{
   const char *uristr = "mongodb://localhost/test";
   const char *database_name;
   const char *collection_name;
   mongoc_uri_t *uri;
   mongoc_client_t *client;
   mongoc_database_t *db;
   mongoc_collection_t *collection;

   mongoc_init ();

   uristr = getenv ("MONGODB_URI");
   uri = mongoc_uri_new (uristr);
   client = mongoc_client_new (uristr);
   database_name = mongoc_uri_get_database (uri);
   db = mongoc_client_get_database(client, database_name);
   collection_name = "test";
   collection = mongoc_database_get_collection (db, collection_name);

   test_merge (db, collection);

   mongoc_collection_destroy (collection);
   mongoc_database_destroy(db);
   mongoc_client_destroy (client);

   mongoc_cleanup ();

   return 0;
}
