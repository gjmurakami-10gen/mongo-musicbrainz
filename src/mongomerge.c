#include <mongoc.h>
#include <stdio.h>
#include "mongomerge.h"

bson_t *
bson_new_from_iter_document (bson_iter_t *iter)
{
   uint32_t document_len;
   const uint8_t *document;
   BSON_ITER_HOLDS_DOCUMENT (iter) || DIE;
   bson_iter_document (iter, &document_len, &document);
   return bson_new_from_data (document, document_len);
}

bson_t *
bson_new_from_iter_array (bson_iter_t *iter)
{
   bson_t *b;
   bson_iter_t iter_array;
   BSON_ITER_HOLDS_ARRAY (iter) || DIE;
   bson_iter_recurse (iter, &iter_array) || DIE;
   b = bson_new ();
   while (bson_iter_next (&iter_array)) {
      bson_t *bsub = bson_new_from_iter_document (&iter_array);
      bson_append_document (b, bson_iter_key (&iter_array), -1, bsub) || DIE;
      bson_destroy (bsub);
   }
   return b;
}

void
bson_printf (const char *format,
             bson_t     *b)
{
   char *str;
   str = bson_as_json (b, NULL);
   printf (format, str);
   bson_free (str);
}

void
mongoc_cursor_dump (mongoc_cursor_t *cursor)
{
   const bson_t *doc;
   while (mongoc_cursor_next (cursor, &doc)) {
      char *str;
      str = bson_as_json (doc, NULL);
      printf ("%s\n", str);
      bson_free (str);
   }
}

void
mongoc_collection_dump (mongoc_collection_t *collection)
{
   bson_t b = BSON_INITIALIZER;
   mongoc_cursor_t *cursor;
   cursor = mongoc_collection_find (collection, MONGOC_QUERY_NONE, 0, 0, 0, &b, NULL, NULL);
   mongoc_cursor_dump (cursor);
   mongoc_cursor_destroy (cursor);
}

bool
mongoc_collection_remove_all (mongoc_collection_t *collection)
{
   bson_t b = BSON_INITIALIZER;
   bool r;
   bson_error_t error;
   (r = mongoc_collection_delete (collection, MONGOC_DELETE_NONE, &b, NULL, &error)) || WARN_ERROR;
   return (r);
}

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

   bson_iter_init_find (&iter, pipeline, "pipeline") || DIE;
   subpipeline = bson_new_from_iter_array (&iter);
   cursor = mongoc_collection_aggregate (collection, flags, subpipeline, options, read_prefs);
   bson_destroy (subpipeline);
   return cursor;
}

bson_t *
child_by_merge_key(const char *parent_key, const char *child_name, const char *child_key)
{
   bson_t *b;
   size_t dollar_child_key_size = strlen("$") + strlen(child_key);
   char *dollar_child_key = bson_malloc (dollar_child_key_size + 1);
   bson_snprintf (dollar_child_key, dollar_child_key_size, "$%s", child_key);
   b = BCON_NEW (
      "pipeline", "[",
         "{",
            "$project", "{",
               "_id", BCON_INT32(0),
               "child_name", "{", "$literal", child_name, "}",
               "merge_id", dollar_child_key,
               parent_key, "$$ROOT",
            "}",
         "}",
      "]"
   );
   bson_free (dollar_child_key);
   return b;
}

bson_t *
parent_child_merge_key(const char *parent_key, const char *child_name, const char *child_key)
{
   bson_t *b;
   size_t parent_key_dot_child_key_size = strlen("$") + strlen(parent_key) + strlen(".") + strlen(child_key);
   char *parent_key_dot_child_key = bson_malloc (parent_key_dot_child_key_size + 1);
   bson_snprintf (parent_key_dot_child_key, parent_key_dot_child_key_size, "$%s.%s", parent_key, child_key);
   b = BCON_NEW (
      "pipeline", "[",
         "{",
            "$project", "{",
              "_id", BCON_INT32(0),
              "child_name", "{", "$literal", child_name, "}",
              "merge_id", "{", "$ifNull", "[", parent_key_dot_child_key, parent_key, "]", "}",
              "parent_id", "$_id",
            "}",
         "}",
      "]"
   );
   bson_free (parent_key_dot_child_key);
   return b;
}

bson_t *
merge_one_all(bson_t *accumulators, bson_t *projectors)
{
   bson_t *b;
   b = BCON_NEW (
      "pipeline", "[",
         "{", "$group", "{",
                 "_id", "{",
                    "child_name", "$child_name",
                    "merge_id", "$merge_id", "}",
                 "parent_id", "{",
                    "$push", "$parent_id", "}", "}", //.merge(accumulators)
          "}",
          "{", "$unwind", "$parent_id", "}",
          "{", "$group", "{",
                  "_id", "$parent_id", "}", //.merge(accumulators)
          "}",
          "{", "$project", "{",
                  "_id", BCON_INT32(0),
                  "parent_id", "$_id", "}", //.merge(projectors)
          "}",
      "]"
   );
   return b;
}

bson_t *
copy_many_with_parent_id(const char *parent_key, const char *child_name, const char *child_key)
{
   return BCON_NEW (
      "pipeline", "[",
          "{", "$match", "{", child_key, "{", "$ne", BCON_NULL, "}", "}", "}",
          "{", "$project", "{",
                  "_id", BCON_INT32(0),
                  "parent_id", "$#{child_key}",
                  parent_key, "$$ROOT", "}", "}",
      "]"
   );
}

#ifdef MAIN
int
main (int argc,
      char *argv[])
{
   const char *uristr = "mongodb://localhost/test";
   const char *database_name;
   mongoc_uri_t *uri;
   mongoc_client_t *client;
   mongoc_collection_t *collection;
   mongoc_cursor_t *cursor;
   const bson_t *doc;
   bson_t *query;
   char *str;

   mongoc_init ();

   uristr = getenv("MONGODB_URI");
   uri = mongoc_uri_new (uristr);
   client = mongoc_client_new (uristr);
   database_name = mongoc_uri_get_database (uri);
   collection = mongoc_client_get_collection (client, database_name, "test");
   query = bson_new ();
   cursor = mongoc_collection_find (collection, MONGOC_QUERY_NONE, 0, 0, 0, query, NULL, NULL);

   while (mongoc_cursor_next (cursor, &doc)) {
      str = bson_as_json (doc, NULL);
      printf ("%s\n", str);
      bson_free (str);
   }

   bson_destroy (query);
   mongoc_cursor_destroy (cursor);
   mongoc_collection_destroy (collection);
   mongoc_client_destroy (client);

   mongoc_cleanup ();

   return 0;
}
#endif
