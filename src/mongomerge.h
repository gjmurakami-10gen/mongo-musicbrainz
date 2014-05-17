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
bson_printf (const char *format, bson_t *b);

void
mongoc_cursor_dump (mongoc_cursor_t *cursor);

void
mongoc_collection_dump (mongoc_collection_t *collection);

bool
mongoc_collection_remove_all (mongoc_collection_t *collection);

mongoc_cursor_t *
mongoc_collection_aggregate_pipeline  (mongoc_collection_t       *collection,
                                       mongoc_query_flags_t       flags,
                                       const bson_t              *pipeline,
                                       const bson_t              *options,
                                       const mongoc_read_prefs_t *read_prefs) BSON_GNUC_WARN_UNUSED_RESULT;

#endif
