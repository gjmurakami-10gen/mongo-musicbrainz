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

#ifndef MONGOMERGE_H
#define MONGOMERGE_H
#include <mongoc.h>
#include <stdio.h>
#include <bcon.h>

#define WARN_ERROR \
    (MONGOC_WARNING ("%s\n", error.message), true);
#define DIE \
    ((void)printf ("%s:%u: failed execution\n", __FILE__, __LINE__), abort(), false)
#define EX(e) \
    ((void) ((e) ? 0 : __ex (#e, __FILE__, __LINE__)))
#define __ex(e, file, line) \
    ((void)printf ("%s:%u: failed execution `%s'\n", file, line, e), abort())
#define ASSERT(e) \
    assert(e)

bson_t *
bson_new_from_iter_document (bson_iter_t *iter) BSON_GNUC_WARN_UNUSED_RESULT;

bson_t *
bson_new_from_iter_array (bson_iter_t *iter) BSON_GNUC_WARN_UNUSED_RESULT;

void
bson_printf (const char *format, const bson_t *b);

void
mongoc_cursor_dump (mongoc_cursor_t *cursor);

void
mongoc_collection_dump (mongoc_collection_t *collection);

bool
mongoc_collection_remove_all (mongoc_collection_t *collection);

int64_t
mongoc_cursor_insert (mongoc_cursor_t *cursor,
                      mongoc_collection_t *dest_coll,
                      const mongoc_write_concern_t *write_concern,
                      bson_error_t *error);

int64_t
mongoc_cursor_insert_batch (mongoc_cursor_t *cursor,
                           mongoc_collection_t *dest_coll,
                           const mongoc_write_concern_t *write_concern,
                           bson_error_t *error, size_t batch_size) BSON_GNUC_DEPRECATED_FOR (mongoc_cursor_insert_batch);

int64_t
mongoc_cursor_bulk_insert (mongoc_cursor_t *cursor,
                           mongoc_collection_t *dest_coll,
                           const mongoc_write_concern_t *write_concern,
                           bson_error_t *error);

bson_t *
child_by_merge_key(const char *parent_key, const char *child_name, const char *child_key);

bson_t *
parent_child_merge_key(const char *parent_key, const char *child_name, const char *child_key);

bson_t *
merge_one_all(bson_t *accumulators, bson_t *projectors);

bson_t *
copy_many_with_parent_id(const char *parent_key, const char *child_name, const char *child_key);

bson_t *
expand_spec(const char *parent_name, int merge_spec_count, char **merge_spec);

int64_t
execute(const char *parent_name, int merge_spec_count, char **merge_spec);

#endif
