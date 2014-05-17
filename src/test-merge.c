#include <mongoc.h>
#include <stdio.h>
#include <bcon.h>

#define WARN_ERROR \
    (MONGOC_WARNING ("%s\n", error.message), true);
#define EXIT \
    ((void)printf ("%s:%u: failed execution\n", __FILE__, __LINE__), abort(), false)
#define EX(e) \
    ((void) ((e) ? 0 : __ex (#e, __FILE__, __LINE__)))
#define __ex(e, file, line) \
    ((void)printf ("%s:%u: failed execution `%s'\n", file, line, e), abort())
#define ASSERT(e) \
    assert(e)

bson_t *
bson_new_from_iter_document (bson_iter_t *iter)
{
   uint32_t document_len;
   const uint8_t *document;
   BSON_ITER_HOLDS_DOCUMENT (iter) || EXIT;
   bson_iter_document (iter, &document_len, &document);
   return bson_new_from_data (document, document_len);
}

bson_t *
bson_new_from_iter_array (bson_iter_t *iter)
{
   bson_t *b;
   bson_iter_t iter_array;
   BSON_ITER_HOLDS_ARRAY (iter) || EXIT;
   bson_iter_recurse (iter, &iter_array) || EXIT;
   b = bson_new ();
   while (bson_iter_next (&iter_array)) {
      bson_t *bsub = bson_new_from_iter_document (&iter_array);
      bson_append_document (b, bson_iter_key (&iter_array), -1, bsub) || EXIT;
      bson_destroy (bsub);
   }
   return b;
}

void bson_printf (const char *format, bson_t *b)
{
   char *str;
   str = bson_as_json (b, NULL);
   printf (format, str);
   bson_free (str);
}

void mongoc_cursor_dump (mongoc_cursor_t *cursor)
{
   const bson_t *doc;
   while (mongoc_cursor_next (cursor, &doc)) {
      char *str;
      str = bson_as_json (doc, NULL);
      printf ("%s\n", str);
      bson_free (str);
   }
}

void mongoc_collection_dump (mongoc_collection_t *collection)
{
   bson_t b = BSON_INITIALIZER;
   mongoc_cursor_t *cursor;
   cursor = mongoc_collection_find (collection, MONGOC_QUERY_NONE, 0, 0, 0, &b, NULL, NULL);
   mongoc_cursor_dump (cursor);
   mongoc_cursor_destroy (cursor);
}

bool mongoc_collection_remove_all (mongoc_collection_t *collection)
{
   bson_t b = BSON_INITIALIZER;
   bool r;
   bson_error_t error;
   (r = mongoc_collection_delete (collection, MONGOC_DELETE_NONE, &b, NULL, &error)) || WARN_ERROR;
   return (r);
}

/*
mongoc_cursor_t               *mongoc_collection_aggregate_pipeline  (mongoc_collection_t           *collection,
                                                                      mongoc_query_flags_t           flags,
                                                                      const bson_t                  *pipeline,
                                                                      const bson_t                  *options,
                                                                      const mongoc_read_prefs_t     *read_prefs) BSON_GNUC_WARN_UNUSED_RESULT;
 */

mongoc_cursor_t *
mongoc_collection_aggregate_pipeline (mongoc_collection_t       *collection, /* IN */
                                      mongoc_query_flags_t       flags,      /* IN */
                                      const bson_t              *pipeline,   /* IN */
                                      const bson_t              *options,    /* IN */
                                      const mongoc_read_prefs_t *read_prefs) /* IN */
{
   bson_t *subpipeline;
   bson_iter_t iter;
   mongoc_cursor_t *cursor;

   bson_iter_init_find (&iter, pipeline, "pipeline") || EXIT;
   subpipeline = bson_new_from_iter_array (&iter);
   cursor = mongoc_collection_aggregate (collection, flags, subpipeline, options, read_prefs);
   bson_destroy (subpipeline);
   return cursor;
}

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
   bson_iter_init_find (&iter_fixture, &bson_fixture, key) || EXIT;
   BSON_ITER_HOLDS_DOCUMENT (&iter_fixture) || EXIT;
   bson_iter_recurse (&iter_fixture, &iter_collection) || EXIT;
   while (bson_iter_next (&iter_collection)) {
       const char *collection_name;
       bson_iter_t iter_doc;
       collection_name = bson_iter_key (&iter_collection);
       printf ("collection_name: \"%s\"\n", collection_name);
       collection = mongoc_database_get_collection (db, collection_name);
       mongoc_collection_drop (collection, &error);
       BSON_ITER_HOLDS_ARRAY (&iter_collection) || EXIT;
       bson_iter_recurse (&iter_collection, &iter_doc) || EXIT;
       while (bson_iter_next (&iter_doc)) {
          bson_t *b = bson_new_from_iter_document (&iter_doc);
          mongoc_collection_insert (collection, MONGOC_INSERT_NONE, b, NULL, &error) || WARN_ERROR;
          bson_destroy (b);
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

void test_merge (mongoc_database_t *db, mongoc_collection_t *collection)
{
   load_test_single (collection);
   load_test_bulk (collection);
   load_fixture (db, one_to_one_fixture, "before");
   load_fixture (db, one_to_many_fixture, "before");
   test_pipeline (collection);
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
