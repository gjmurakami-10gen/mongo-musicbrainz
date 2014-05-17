#include <mongoc.h>
#include <stdio.h>

/*
void child_by_merge_key(const char *parent_key, const char *child_name, const char *child_key)
   bson_t pipeline;
      [
          {'$project' => {
              '_id' => 0, 'child_name' => {'$literal' => child_name},
              'merge_id' => "$#{child_key}",
              parent_key => '$$ROOT'}
          }
      ]
}
*/

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
