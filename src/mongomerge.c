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
#include "mongomerge.h"

char *
str_compose (const char *s1,
             const char *s2)
{
   char *s;
   size_t len;

   len = strlen (s1) + strlen (s2) + 1;
   s = bson_malloc (len);
   bson_snprintf (s, len, "%s%s", s1, s2);
   return s;
}

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
   bson_t *bson;
   bson_iter_t iter_array;

   BSON_ITER_HOLDS_ARRAY (iter) || DIE;
   bson_iter_recurse (iter, &iter_array) || DIE;
   bson = bson_new ();
   while (bson_iter_next (&iter_array)) {
      bson_t *bson_sub;

      bson_sub = bson_new_from_iter_document (&iter_array); /* review */
      bson_append_document (bson, bson_iter_key (&iter_array), -1, bson_sub) || DIE;
      bson_destroy (bson_sub);
   }
   return bson;
}

void
bson_printf (const char   *format,
             const bson_t *bson)
{
   const char *str;

   str = bson_as_json (bson, NULL);
   printf (format, str);
   bson_free ((void*)str);
}

const char *
bson_iter_next_utf8 (bson_iter_t *iter,
                     uint32_t    *length)
{
    bson_iter_next (iter) || DIE;
    BSON_ITER_HOLDS_UTF8 (iter) || DIE;
    return bson_iter_utf8 (iter, length);
}

void
mongoc_cursor_dump (mongoc_cursor_t *cursor)
{
   const bson_t *doc;

   while (mongoc_cursor_next (cursor, &doc)) {
      const char *str;

      str = bson_as_json (doc, NULL);
      printf ("%s\n", str);
      bson_free ((void*)str);
   }
}

void
mongoc_collection_dump (mongoc_collection_t *collection)
{
   bson_t bson = BSON_INITIALIZER;
   mongoc_cursor_t *cursor;

   cursor = mongoc_collection_find (collection, MONGOC_QUERY_NONE, 0, 0, 0, &bson, NULL, NULL);
   mongoc_cursor_dump (cursor);
   mongoc_cursor_destroy (cursor);
}

bool
mongoc_collection_remove_all (mongoc_collection_t *collection)
{
   bson_t bson = BSON_INITIALIZER;
   bool ret;
   bson_error_t error;

   (ret = mongoc_collection_remove (collection, MONGOC_REMOVE_NONE, &bson, NULL, &error)) || WARN_ERROR;
   return (ret);
}

int64_t
mongoc_cursor_insert (mongoc_cursor_t              *cursor,
                      mongoc_collection_t          *dest_coll,
                      const mongoc_write_concern_t *write_concern,
                      bson_error_t                 *error)
{
   bool ret = true;
   int64_t count = 0;
   const bson_t *doc;

   while (ret && mongoc_cursor_next (cursor, &doc)) {
      ret = mongoc_collection_insert (dest_coll, MONGOC_INSERT_NONE, doc, write_concern, error);
      ++count;
   }
   if (mongoc_cursor_error (cursor, error)) {
      fprintf (stderr, "mongoc_cursor_insert failure: %s\n", error->message);
      ret = false;
   }
   return ret ? count : -1;
}

int64_t
mongoc_cursor_insert_batch (mongoc_cursor_t              *cursor,
                            mongoc_collection_t          *dest_coll,
                            const mongoc_write_concern_t *write_concern,
                            bson_error_t                 *error,
                            size_t                        batch_size)
{
   bool ret = true;
   int64_t count = 0;
   const bson_t *doc;
   size_t n_docs = 0;
   bson_t **docs;
   size_t i;

   docs = bson_malloc (batch_size * sizeof (bson_t*));
   for (i = 0; i < batch_size; i++)
       docs[i] = bson_new ();
   while (ret && mongoc_cursor_next (cursor, &doc)) {
      bson_copy_to (doc, docs[n_docs++]);
      if (n_docs == batch_size) {
         ret = mongoc_collection_insert_bulk (dest_coll, MONGOC_INSERT_NONE, (const bson_t**)docs, n_docs, write_concern, error);
         if (ret)
            count += n_docs;
         else
            fprintf (stderr, "mongoc_cursor_insert_batch failure: %s\n", error->message);
         for (i = 0; i < batch_size; i++)
            bson_reinit (docs[i]);
         n_docs = 0;
      }
   }
   if (ret && n_docs > 0) {
      ret = mongoc_collection_insert_bulk (dest_coll, MONGOC_INSERT_NONE, (const bson_t**)docs, n_docs, write_concern, error);
      if (ret)
         count += n_docs;
      else
         fprintf (stderr, "mongoc_cursor_insert_batch failure: %s\n", error->message);
   }
   for (i = 0; i < batch_size; i++)
      bson_destroy (docs[i]);
   bson_free (docs);
   return ret ? count : -1;
}

int64_t
mongoc_cursor_bulk_insert (mongoc_cursor_t              *cursor,
                           mongoc_collection_t          *dest_coll,
                           const mongoc_write_concern_t *write_concern,
                           bson_error_t                 *error,
                           size_t                        bulk_ops_size)
{
   int64_t ret = true;
   int64_t count = 0;
   const bson_t *doc;
   size_t n_docs = 0;
   mongoc_bulk_operation_t *bulk;
   bson_t reply;

   bulk = mongoc_collection_create_bulk_operation (dest_coll, true, NULL);
   while (ret && mongoc_cursor_next (cursor, &doc)) {
      mongoc_bulk_operation_insert (bulk, doc);
      if (++n_docs == bulk_ops_size) {
         ret = mongoc_bulk_operation_execute (bulk, &reply, error);
         if (ret)
            count += n_docs;
         else
            fprintf (stderr, "mongoc_cursor_bulk_insert execute failure: %s\n", error->message);
         n_docs = 0;
         mongoc_bulk_operation_destroy (bulk);
         bulk = mongoc_collection_create_bulk_operation (dest_coll, true, NULL);
      }
   }
   if (ret && n_docs > 0) {
      ret = mongoc_bulk_operation_execute (bulk, &reply, error);
      if (ret)
         count += n_docs;
      else
         fprintf (stderr, "mongoc_cursor_bulk_insert execute failure: %s\n", error->message);
   }
   mongoc_bulk_operation_destroy (bulk);
   return ret ? count : -1;
}

bson_t *
child_by_merge_key (const char *parent_key,
                    const char *child_name,
                    const char *child_key)
{
   bson_t *bson;
   const char *dollar_child_key;

   dollar_child_key = str_compose ("$", child_key);
   bson = BCON_NEW (
      "pipeline", "[",
         "{",
            "$project", "{",
               "_id", BCON_INT32 (0),
               "child_name", "{", "$literal", child_name, "}",
               "merge_id", dollar_child_key,
               parent_key, "$$ROOT",
            "}",
         "}",
      "]"
   );
   bson_free ((void*)dollar_child_key);
   return bson;
}

bson_t *
parent_child_merge_key (const char *parent_key,
                        const char *child_name,
                        const char *child_key)
{
   bson_t *bson;
   size_t dollar_parent_key_dot_child_key_size;
   char *dollar_parent_key_dot_child_key;
   const char *dollar_parent_key;

   dollar_parent_key_dot_child_key_size = strlen ("$") + strlen (parent_key) + strlen (".") + strlen (child_key) + 1;
   dollar_parent_key_dot_child_key = bson_malloc (dollar_parent_key_dot_child_key_size);
   bson_snprintf (dollar_parent_key_dot_child_key, dollar_parent_key_dot_child_key_size, "$%s.%s", parent_key, child_key);
   dollar_parent_key = str_compose ("$", parent_key);
   bson = BCON_NEW (
      "pipeline", "[",
         "{",
            "$project", "{",
              "_id", BCON_INT32 (0),
              "child_name", "{", "$literal", child_name, "}",
              "merge_id", "{", "$ifNull", "[", dollar_parent_key_dot_child_key, dollar_parent_key, "]", "}",
              "parent_id", "$_id",
            "}",
         "}",
      "]"
   );
   bson_free (dollar_parent_key_dot_child_key);
   bson_free ((char*)dollar_parent_key);
   return bson;
}

bson_t *
merge_one_all (bson_t *accumulators,
               bson_t *projectors)
{
   bson_t *bson;

   bson = BCON_NEW (
      "pipeline", "[",
         "{", "$group", "{",
                 "_id", "{",
                    "child_name", "$child_name",
                    "merge_id", "$merge_id", "}",
                 "parent_id", "{",
                    "$push", "$parent_id", "}",
                    BCON (accumulators), "}",
         "}",
         "{", "$unwind", "$parent_id", "}",
         "{", "$group", "{",
                  "_id", "$parent_id",
                  BCON (accumulators), "}",
         "}",
         "{", "$project", "{",
                  "_id", BCON_INT32 (0),
                  "parent_id", "$_id",
                  BCON (projectors), "}",
         "}",
      "]"
   );
   return bson;
}

bson_t *
copy_many_with_parent_id (const char *parent_key,
                          const char *child_name,
                          const char *child_key)
{
   char *dollar_child_key;
   bson_t *bson;

   dollar_child_key = str_compose ("$", child_key);
   bson = BCON_NEW (
      "pipeline", "[",
          "{", "$match", "{", child_key, "{", "$ne", BCON_NULL, "}", "}", "}",
          "{", "$project", "{",
                  "_id", BCON_INT32 (0),
                  "parent_id", dollar_child_key,
                  parent_key, "$$ROOT", "}", "}",
      "]"
   );
   bson_free ((void*)dollar_child_key);
   return bson;
}

int64_t
agg_copy (mongoc_collection_t *source_coll,
          mongoc_collection_t *dest_coll,
          bson_t              *pipeline)
{
   bson_t *options;
   mongoc_cursor_t *cursor;
   int64_t count;
   bson_error_t error;

   options = BCON_NEW ("cursor", "{", "}", "allowDiskUse", BCON_BOOL (1));
   cursor = mongoc_collection_aggregate (source_coll, MONGOC_QUERY_NONE, pipeline, options, NULL);
   /*
   count = mongoc_cursor_insert (cursor, dest_coll, NULL, &error);
   count = mongoc_cursor_insert_batch (cursor, dest_coll, NULL, &error, INSERT_BATCH_SIZE);
   */
   count = mongoc_cursor_bulk_insert (cursor, dest_coll, NULL, &error, BULK_OPS_SIZE);
   mongoc_cursor_destroy (cursor);
   bson_destroy (options);
   return count;
}

int64_t
group_and_update (mongoc_collection_t *source_coll,
                  mongoc_collection_t *dest_coll,
                  bson_t              *accumulators)
{
   bson_t *options;
   bson_t *pipeline;
   mongoc_cursor_t *cursor;
   bool ret = true;
   int64_t count = 0;
   const bson_t *doc;
   bson_error_t error;
   size_t n_docs = 0;
   mongoc_bulk_operation_t *bulk;
   bson_t reply;

   options = BCON_NEW ("cursor", "{", "}", "allowDiskUse", BCON_BOOL (1));
   pipeline = BCON_NEW ("pipeline", "[", "{", "$group", "{", "_id", "$parent_id", BCON (accumulators), "}", "}", "]");
   cursor = mongoc_collection_aggregate (source_coll, MONGOC_QUERY_NONE, pipeline, options, NULL);
   bulk = mongoc_collection_create_bulk_operation (dest_coll, true, NULL);
   while (ret && mongoc_cursor_next (cursor, &doc)) {
      bson_t q, fields, *u;
      bson_iter_t iter, iter_ary;
      bool do_update = false;

      bson_iter_init_find (&iter, doc, "_id");
      bson_init (&q);
      bson_append_iter (&q, NULL, -1, &iter);
      bson_init (&fields);
      while (bson_iter_next (&iter)) {
         if (BSON_ITER_HOLDS_NULL (&iter))
            continue;
         if (BSON_ITER_HOLDS_ARRAY (&iter) && bson_iter_recurse (&iter, &iter_ary) && !bson_iter_next (&iter_ary))
            continue;
         bson_append_iter (&fields, NULL, -1, &iter);
         do_update = true;
      }
      u = BCON_NEW ("$set", BCON_DOCUMENT (&fields));
      /*
      if (do_update)
         ret = mongoc_collection_update (dest_coll, MONGOC_UPDATE_NONE, &q, u, NULL, &error);
      */
      if (do_update) {
         mongoc_bulk_operation_update_one (bulk, &q, u, false);
         if (++n_docs == BULK_OPS_SIZE) {
            ret = mongoc_bulk_operation_execute (bulk, &reply, &error);
            if (ret)
               count += n_docs;
            else
               fprintf (stderr, "group_and_update bulk execute failure: %s\n", (char*)&error.message);
            n_docs = 0;
            mongoc_bulk_operation_destroy (bulk);
            bulk = mongoc_collection_create_bulk_operation (dest_coll, true, NULL);
         }
      }
      bson_destroy (&q);
      bson_destroy (&fields);
      bson_destroy (u);
      if (!ret)
         fprintf (stderr, "mongoc_collection_update failure: %s\n", (char*)&error.message);
      else
         ++count;
   }
   if (ret && n_docs > 0) {
      ret = mongoc_bulk_operation_execute (bulk, &reply, &error);
      if (ret)
         count += n_docs;
      else
         fprintf (stderr, "group_and_update bulk execute failure: %s\n", (char*)&error.message);
   }
   if (mongoc_cursor_error (cursor, &error)) {
      fprintf (stderr, "mongoc_cursor_insert failure: %s\n", (char*)&error.message);
      ret = false;
   }
   mongoc_cursor_destroy (cursor);
   mongoc_bulk_operation_destroy (bulk);
   bson_destroy (options);
   return ret ? count : -1;
}

bson_t *
expand_spec (const char *parent_name,
             int         merge_spec_count,
             char      **merge_spec)
{
   bson_t *bson, bson_array;
   int i;

   bson = bson_new ();
   bson_append_array_begin (bson, "merge_spec", -1, &bson_array);
   for (i = 0; i < merge_spec_count; i++) {
      char *s, *relation, *parent_key, *child_s, *child_name, *child_key, *colon, *dot;

      s = bson_malloc (strlen (merge_spec[i]) + 1);
      strcpy (s, merge_spec[i]);
      parent_key = child_name = child_s = s;
      colon = strchr (s, ':');
      if (colon != NULL) {
         *colon = '\0';
         child_s = colon + 1;
      }
      if (*child_s != '[') {
         relation = "one";
         child_key = "_id";
      }
      else {
         char *terminator;

         child_s += 1;
         terminator = strchr (child_s, ']');
         (terminator != NULL && *(terminator + 1) == '\0') || DIE;
         *terminator = '\0';
         relation = "many";
         child_key = (char*)parent_name;
      }
      dot = strchr (child_s, '.');
      if (dot != NULL) {
         *dot = '\0';
         child_key = dot + 1;
      }
      if (*child_s != '\0')
         child_name = child_s;
      /* check non-empty, legal chars */
      BCON_APPEND (&bson_array, "0", "[", relation, parent_key, child_name, child_key, "]");
      bson_free (s);
   }
   bson_append_array_end (bson, &bson_array);
   return bson;
}

void
one_children_append (const char          *parent_name,
                     bson_iter_t         *iter_spec_top,
                     mongoc_database_t   *db,
                     mongoc_collection_t *parent_coll,
                     mongoc_collection_t *temp_coll,
                     bson_t              *all_accumulators)
{
   const char *temp_one_name;
   mongoc_collection_t *child_coll, *temp_one_coll;
   bson_t *one_accumulators, *one_projectors, *pipeline;
   bson_iter_t iter_spec, iter;
   bson_error_t error;

   temp_one_name = str_compose (parent_name, "_merge_temp_one");
   temp_one_coll = mongoc_database_get_collection (db, temp_one_name);
   mongoc_collection_drop (temp_one_coll, &error);
   bson_free ((void*)temp_one_name);

   one_accumulators = bson_new ();
   one_projectors = bson_new ();

   bson_iter_recurse (iter_spec_top, &iter_spec) || DIE;
   while (bson_iter_next (&iter_spec)) {
      const char *type, *parent_key, *child_name, *child_key, *dollar_parent_key;

      BSON_ITER_HOLDS_ARRAY (&iter_spec) || DIE;
      bson_iter_recurse (&iter_spec, &iter) || DIE;
      type = bson_iter_next_utf8 (&iter, NULL);
      if (strcmp ("one", type) != 0)
         continue;
      parent_key = bson_iter_next_utf8 (&iter, NULL);
      child_name = bson_iter_next_utf8 (&iter, NULL);
      child_key = bson_iter_next_utf8 (&iter, NULL);
      /*
      printf ("info: spec: {type: \"%s\", parent_key: \"%s\", child_name: \"%s\", child_key: \"%s\"}\n",
              type, parent_key, child_name, child_key);
      */
      child_coll = mongoc_database_get_collection (db, child_name);
      pipeline = child_by_merge_key (parent_key, child_name, child_key);
      agg_copy (child_coll, temp_one_coll, pipeline);
      bson_destroy (pipeline);
      pipeline = parent_child_merge_key (parent_key, child_name, child_key);
      agg_copy (parent_coll, temp_one_coll, pipeline);
      bson_destroy (pipeline);
      mongoc_collection_destroy (child_coll);
      dollar_parent_key = str_compose ("$", parent_key);
      BCON_APPEND (all_accumulators, parent_key, "{", "$max", dollar_parent_key, "}");
      BCON_APPEND (one_accumulators, parent_key, "{", "$max", dollar_parent_key, "}");
      BCON_APPEND (one_projectors, parent_key, dollar_parent_key);
      bson_free ((void*)dollar_parent_key);
   }
   pipeline = merge_one_all (one_accumulators, one_projectors);
   agg_copy (temp_one_coll, temp_coll, pipeline);
   bson_destroy (pipeline);
   bson_destroy (one_accumulators);
   bson_destroy (one_projectors);
   mongoc_collection_drop (temp_one_coll, &error);
}

void
many_children_append (const char         *parent_name,
                     bson_iter_t         *iter_spec_top,
                     mongoc_database_t   *db,
                     mongoc_collection_t *temp_coll,
                     bson_t              *all_accumulators)
{
   mongoc_collection_t *child_coll;
   bson_iter_t iter_spec, iter;
   bson_t *pipeline;

   bson_iter_recurse (iter_spec_top, &iter_spec) || DIE;
   while (bson_iter_next (&iter_spec)) {
      const char *type, *parent_key, *child_name, *child_key, *dollar_parent_key;

      BSON_ITER_HOLDS_ARRAY (&iter_spec) || DIE;
      bson_iter_recurse (&iter_spec, &iter) || DIE;
      type = bson_iter_next_utf8 (&iter, NULL);
      if (strcmp ("many", type) != 0)
         continue;
      parent_key = bson_iter_next_utf8 (&iter, NULL);
      child_name = bson_iter_next_utf8 (&iter, NULL);
      child_key = bson_iter_next_utf8 (&iter, NULL);
      /*
      printf ("info: spec: {type: \"%s\", parent_key: \"%s\", child_name: \"%s\", child_key: \"%s\"}\n",
              type, parent_key, child_name, child_key);
      */
      child_coll = mongoc_database_get_collection (db, child_name);
      pipeline = copy_many_with_parent_id (parent_key, child_name, child_key);
      agg_copy (child_coll, temp_coll, pipeline);
      bson_destroy (pipeline);
      mongoc_collection_destroy (child_coll);
      dollar_parent_key = str_compose ("$", parent_key);
      BCON_APPEND (all_accumulators, parent_key, "{", "$push", dollar_parent_key, "}");
      bson_free ((void*)dollar_parent_key);
   }
}

int64_t
execute (const char *parent_name,
         int         merge_spec_count,
         char      **merge_spec)
{
   int64_t count;
   const char *uristr = "mongodb://localhost/test";
   const char *database_name;
   mongoc_uri_t *uri;
   mongoc_client_t *client;
   mongoc_database_t *db;
   const char *temp_name;
   mongoc_collection_t *parent_coll, *temp_coll;
   bson_t *bson_spec, *all_accumulators;
   bson_iter_t iter_spec_top;
   bson_error_t error;

   uristr = getenv ("MONGODB_URI");
   uri = mongoc_uri_new (uristr);
   client = mongoc_client_new (uristr);
   database_name = mongoc_uri_get_database (uri);
   db = mongoc_client_get_database (client, database_name);
   parent_coll = mongoc_database_get_collection (db, parent_name);

   temp_name = str_compose (parent_name, "_merge_temp");
   temp_coll = mongoc_database_get_collection (db, temp_name);
   mongoc_collection_drop (temp_coll, &error);
   bson_free ((void*)temp_name);

   bson_spec = expand_spec (parent_name, merge_spec_count, merge_spec);
   bson_iter_init_find (&iter_spec_top, bson_spec, "merge_spec") || DIE;
   BSON_ITER_HOLDS_ARRAY (&iter_spec_top) || DIE;
   all_accumulators = bson_new ();

   one_children_append (parent_name, &iter_spec_top, db, parent_coll, temp_coll, all_accumulators);

   many_children_append (parent_name, &iter_spec_top, db, temp_coll, all_accumulators);

   count = group_and_update (temp_coll, parent_coll, all_accumulators);

   bson_destroy (all_accumulators);
   bson_destroy (bson_spec);
   mongoc_collection_drop (temp_coll, &error);
   mongoc_collection_destroy (temp_coll);
   mongoc_collection_destroy (parent_coll);
   mongoc_database_destroy (db);
   mongoc_client_destroy (client);
   mongoc_uri_destroy (uri);

   return count;
}
