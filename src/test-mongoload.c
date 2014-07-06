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

#define WARN_ERROR \
    (MONGOC_WARNING ("%s\n", error.message), true);

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
# define bson_read(f,b,c) ((ssize_t)_read ((f), (b), (int)(c)))
# define bson_write _write
#else
# define bson_open open
# define bson_read read
# define bson_close close
# define bson_write write
#endif

static ssize_t
test_reader_from_handle_read (void  *handle,
                              void  *buf,
                              size_t len)
{
   return bson_read (*(int *)handle, buf, len);
}

static void
test_reader_from_handle_destroy (void * handle)
{
   bson_close (*(int *)handle);
}

double
dtimeofday ()
{
   struct timeval tv;

   bson_gettimeofday (&tv);
   return tv.tv_sec + 0.000001 * tv.tv_usec;
}

int64_t
collection_load_from_file_insert_single (mongoc_collection_t *collection,
                                         const char          *file_name,
                                         bool                 do_insert)
{
   int64_t count = 0;
   int fd;
   bson_reader_t *reader;
   bson_error_t error;
   bson_t *doc;
   bool eof = false;

   fd = bson_open (file_name, O_RDONLY);
   assert (-1 != fd);
   reader = bson_reader_new_from_handle ((void *)&fd, &test_reader_from_handle_read, &test_reader_from_handle_destroy);
   mongoc_collection_drop (collection, &error);
   while ((doc = (bson_t*)bson_reader_read (reader, &eof))) {
      if (do_insert)
         mongoc_collection_insert (collection, MONGOC_INSERT_NONE, doc, NULL, &error) || WARN_ERROR;
      ++count;
   }
   bson_reader_destroy (reader);
   return count;
}

int64_t
collection_load_from_file_insert_docs (mongoc_collection_t *collection,
                                       const char          *file_name,
                                       size_t               batch_size)
{
   int64_t count = 0;
   int fd;
   bson_reader_t *reader;
   bson_error_t error;
   const bson_t *doc;
   bool eof = false;
   bool ret = true;
   size_t i, n_docs;
   bson_t **docs;

   docs = bson_malloc (batch_size * sizeof (bson_t*));
   for (i = 0; i < batch_size; i++)
       docs[i] = bson_new ();
   fd = bson_open (file_name, O_RDONLY);
   assert (-1 != fd);
   reader = bson_reader_new_from_handle ((void *)&fd, &test_reader_from_handle_read, &test_reader_from_handle_destroy);
   mongoc_collection_drop (collection, &error);
   n_docs = 0;
   while ((doc = (bson_t*)bson_reader_read (reader, &eof))) {
      bson_copy_to (doc, docs[n_docs++]);
      if (n_docs == batch_size) {
         ret = mongoc_collection_insert_bulk (collection, MONGOC_INSERT_NONE, (const bson_t**)docs, n_docs, NULL, &error);
         for (i = 0; i < batch_size; i++)
            bson_reinit (docs[i]);
         count += n_docs;
         n_docs = 0;
      }
   }
   if (ret && n_docs > 0) {
      ret = mongoc_collection_insert_bulk (collection, MONGOC_INSERT_NONE, (const bson_t**)docs, n_docs, NULL, &error);
      count += n_docs;
   }
   bson_reader_destroy (reader);
   for (i = 0; i < batch_size; i++)
      bson_destroy (docs[i]);
   bson_free (docs);
   return ret ? count : -1;
}

int64_t
collection_load_from_file_bulk_insert (mongoc_collection_t *collection,
                                       const char          *file_name,
                                       size_t               batch_size)
{
   int64_t count = 0;
   int fd;
   bson_reader_t *reader;
   const bson_t *doc;
   bson_error_t error;
   bool eof = false;
   bool ret = true;
   size_t n_docs = 0;
   mongoc_bulk_operation_t *bulk;
   bson_t reply;

   fd = bson_open (file_name, O_RDONLY);
   assert (-1 != fd);
   reader = bson_reader_new_from_handle ((void *)&fd, &test_reader_from_handle_read, &test_reader_from_handle_destroy);
   mongoc_collection_drop (collection, &error);
   bulk = mongoc_collection_create_bulk_operation (collection, true, NULL);
   while (ret && (doc = (bson_t*)bson_reader_read (reader, &eof))) {
      mongoc_bulk_operation_insert (bulk, doc);
      if (++n_docs == batch_size) {
         ret = mongoc_bulk_operation_execute (bulk, &reply, &error);
         count += n_docs;
         n_docs = 0;
         mongoc_bulk_operation_destroy (bulk);
         bulk = mongoc_collection_create_bulk_operation (collection, true, NULL);
      }
   }
   if (ret && n_docs > 0) {
      ret = mongoc_bulk_operation_execute (bulk, &reply, &error);
      count += n_docs;
   }
   if (!ret) {
      printf ("Error: %s\n", error.message);
   }
   bson_reader_destroy (reader);
   mongoc_bulk_operation_destroy (bulk);
   return ret ? count : -1;
}

void execute (mongoc_database_t   *db,
              mongoc_collection_t *collection)
{
   double start_time;
   int64_t count;
   double end_time;
   double delta_time;

   start_time = dtimeofday ();
   /*
   measurements for Mac Pro Mid 2010, 2 x 2.66 GHz 6-Core Intel Xeon, MacPro5,1, SSD
   count = collection_load_from_file_insert_single (collection, "../twitter.bson", false); // secs: 0.03, count: 51428, 1515978 docs/sec
   count = collection_load_from_file_insert_single (collection, "../twitter.bson", true); // secs: 9.52, count: 51428, 5404 docs/sec
   count = collection_load_from_file_insert_docs (collection, "../twitter.bson", 1000); // secs: 2.09, count: 51428, 24607 docs/sec
   count = collection_load_from_file_bulk_insert (collection, "../twitter.bson", 1000); // pending repeat
   */
   count = collection_load_from_file_bulk_insert (collection, "../twitter.bson", 1000);
   end_time = dtimeofday ();
   delta_time = end_time - start_time + 0.0000001;
   printf ("secs: %.2f, count: %"PRId64", %"PRId64" docs/sec\n", delta_time, count, (int64_t)round (count/delta_time));
}

int
main (int   argc,
      char *argv[])
{
   const char *default_uristr = "mongodb://localhost/test";
   char *uristr;
   const char *database_name;
   const char *collection_name;
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
   collection_name = "test";
   collection = mongoc_database_get_collection (db, collection_name);

   execute (db, collection);

   mongoc_collection_destroy (collection);
   mongoc_database_destroy (db);
   mongoc_client_destroy (client);
   mongoc_uri_destroy (uri);

   mongoc_cleanup ();

   return 0;
}
