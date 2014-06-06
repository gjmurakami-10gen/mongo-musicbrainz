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
#include <sys/param.h>
#include <sys/stat.h>
#include <libgen.h>

#define INSERT_BATCH_SIZE 1000
#define BULK_OPS_SIZE 1000
#define PROGRESS_SIZE (1000*BULK_OPS_SIZE)
#define PROGRESS_SIZE_FORMAT "M"
#define PROGRESS_END_FORMAT ">%zd=%"PRId64

#define WARN_ERROR \
    (MONGOC_WARNING ("%s\n", error.message), true);
#define DIE \
    ((void)printf ("%s:%u: failed execution\n", __FILE__, __LINE__), abort (), false)
#define EX(e) \
    ((void) ((e) ? 0 : __ex (#e, __FILE__, __LINE__)))
#define __ex(e, file, line) \
    ((void)printf ("%s:%u: failed execution `%s'\n", file, line, e), abort ())
#define ASSERT(e) \
    assert (e)

char cwd[MAXPATHLEN];
char base_dir[MAXPATHLEN];
char fullexport_dir[MAXPATHLEN];
char latest_file[MAXPATHLEN];
char latest_name[MAXPATHLEN];
char mbdump_dir[MAXPATHLEN];
char schema_file[MAXPATHLEN];
char mbdump_file[MAXPATHLEN];

#define MONGODB_DEFAULT_URI "mongodb://localhost/musicbrainz"

char buf[BUFSIZ];

char *
realpath_replace (char *file_name)
{
    char temp_path[MAXPATHLEN];
    realpath (file_name, temp_path);
    strcpy (file_name, temp_path);
    return file_name;
}

char *
dirname_replace (char *file_name)
{
    char temp_path[MAXPATHLEN];
    snprintf (temp_path, MAXPATHLEN, "%s/../", file_name);
    strcpy (file_name, temp_path);
    realpath_replace (file_name);
    return file_name;
}

off_t
file_size (FILE * fp)
{
    struct stat buf;

    fstat (fileno (fp), &buf) == 0 || DIE;
    return buf.st_size;
}

char *
file_to_s (const char *file_name)
{
    FILE *fp;
    char *s;
    off_t st_size;

    fp = fopen (file_name, "r");
    if (!fp) DIE;
    st_size = file_size (fp);
    s = malloc (st_size + 1);
    fread (s, 1, st_size, fp) || DIE;
    s[st_size] = '\0';
    fclose (fp);
    return s;
}

char *
chomp (char *s)
{
    size_t len;

    len = strlen (s);
    if (*s && s[len - 1]=='\n')
        s[len - 1] = '\0';
    return s;
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

bool
bson_init_from_json_file (bson_t     *bson,
                          const char *file_name)
{
    char *json, *json_wrapped;
    size_t len;
    bson_error_t error;
    bool ret;

    json = file_to_s (file_name);
    len = strlen (json) + 16;
    json_wrapped = malloc (len);
    snprintf (json_wrapped, len, "{\n\"json\": %s\n}\n", json);
    ret = bson_init_from_json (bson, json_wrapped, len, &error) || WARN_ERROR;
    free (json);
    free (json_wrapped);
    return ret;
}

void
set_paths (char *argv[])
{
    char *latest_name;

    getcwd (cwd, MAXPATHLEN);
    snprintf (base_dir, MAXPATHLEN, "%s/%s", cwd, dirname (argv[0]));
    dirname_replace (base_dir);
    realpath_replace (base_dir);
    snprintf (fullexport_dir, MAXPATHLEN, "%s/%s", base_dir, "ftp.musicbrainz.org/pub/musicbrainz/data/fullexport");
    snprintf (latest_file, MAXPATHLEN, "%s/%s", fullexport_dir, "LATEST");
    latest_name = chomp (file_to_s (latest_file));
    snprintf (mbdump_dir, MAXPATHLEN, "%s/data/fullexport/%s/mbdump", base_dir, latest_name);
    free (latest_name);
    snprintf (schema_file, MAXPATHLEN, "%s/schema/create_tables.json", base_dir);
}

bool
bson_find_create_table (bson_t *bson_schema,
                        const char *table_name,
                        bson_iter_t *iter_col)
{
    bson_iter_t iter_json, iter_ary, iter_sql, iter_table_prop;

    bson_iter_init_find (&iter_json, bson_schema, "json") || DIE;
    BSON_ITER_HOLDS_ARRAY (&iter_json) || DIE;
    bson_iter_recurse (&iter_json, &iter_ary) || DIE;
    while (bson_iter_next (&iter_ary)) {
        (BSON_ITER_HOLDS_DOCUMENT (&iter_ary) || DIE);
        bson_iter_recurse (&iter_ary, &iter_sql) || DIE;
        if (bson_iter_find (&iter_sql, "create_table") &&
            (BSON_ITER_HOLDS_DOCUMENT (&iter_sql) || DIE) &&
            (bson_iter_recurse (&iter_sql, &iter_table_prop) || DIE) &&
            (bson_iter_find (&iter_table_prop, "table_name") || DIE) &&
            (BSON_ITER_HOLDS_UTF8 (&iter_table_prop) || DIE) &&
            (strcmp (bson_iter_utf8 (&iter_table_prop, NULL), table_name) == 0) &&
            (bson_iter_find (&iter_table_prop, "columns") || DIE) &&
            (BSON_ITER_HOLDS_ARRAY (&iter_table_prop) || DIE)) {
            bson_iter_recurse (&iter_table_prop, iter_col) || DIE;
            return true;
        }
    }
    return (false);
}

bool
bson_append_utf8_from_s (bson_t     *bson,
                         const char *key,
                         const char *value)
{
    bool ret = true;

    if (value)
        ret = BSON_APPEND_UTF8 (bson, key, value);
    return ret;
}

bool
bson_append_int32_from_s (bson_t     *bson,
                          const char *key,
                          const char *value)
{
    bool ret = true;

    if (strcmp("\\N", value) != 0)
        ret = BSON_APPEND_INT32 (bson, key, atoi (value));
    return ret;
}

bool
bson_append_bool_from_s (bson_t     *bson,
                          const char *key,
                          const char *value)
{
    strcmp ("t", value) == 0 || strcmp ("f", value) == 0 || DIE;
    return BSON_APPEND_BOOL (bson, key, (strcmp ("t", value) == 0) ? true : false);
}

#define FORMAT_PG_TIMESTAMP_WITH_TIME_ZONE ""

bool
pg_timestamp_with_time_zone_from_s (const char     *s,
                                    struct timeval *timeval)
{
    struct tm tm;
    char *p;

    p = strptime (s, FORMAT_PG_TIMESTAMP_WITH_TIME_ZONE, &tm);
    if (p)
       printf ("WARNING: strptime parsing incomplete\n");
    timeval->tv_sec = mktime (&tm);
    timeval->tv_usec = 0; /* pending */
    return false;
}

bool
bson_append_timeval_from_s (bson_t     *bson,
                                        const char *key,
                                        const char *value)
{
    struct timeval timeval;

    timeval.tv_sec = 0;
    timeval.tv_usec = 0;
    pg_timestamp_with_time_zone_from_s (value, &timeval);
    return bson_append_timeval (bson, key, -1, &timeval) && false;
}

bool
bson_append_int32_array_from_s (bson_t     *bson,
                                const char *key,
                                const char *value)
{
    /* pending */
    return false;
}

bool
bson_append_point_from_s (bson_t     *bson,
                          const char *key,
                          const char *value)
{
    /* pending */
    return false;
}

typedef struct {
    const char *data_type;
    bool (*bson_append_from_s) (bson_t *bson, const char *key, const char *value);
} data_type_map_t;

data_type_map_t data_type_map[] = {
    { "BOOLEAN",       bson_append_bool_from_s },
    { "CHAR(2)",       NULL },
    { "CHAR(3)",       NULL },
    { "CHAR(4)",       NULL },
    { "CHAR(8)",       NULL },
    { "CHAR(11)",      NULL },
    { "CHAR(12)",      NULL },
    { "CHAR(16)",      NULL },
    { "CHAR(28)",      NULL },
    { "CHARACTER(15)", NULL },
    { "INT",           bson_append_int32_from_s },
    { "INTEGER",       bson_append_int32_from_s },
    { "SERIAL",        bson_append_int32_from_s },
    { "SMALLINT",      bson_append_int32_from_s },
    { "TEXT",          NULL },
    { "TIMESTAMP",     bson_append_timeval_from_s },
    { "UUID",          NULL },
    { "uuid",          NULL },
    { "VARCHAR",       NULL },
    { "VARCHAR(10)",   NULL },
    { "VARCHAR(50)",   NULL },
    { "VARCHAR(100)",  NULL },
    { "VARCHAR(255)",  NULL },
    { "INTEGER[]",     bson_append_int32_array_from_s },
    { "POINT",         bson_append_point_from_s }
};

typedef struct {
    const char *column_name;
    bool (*bson_append_from_s) (bson_t *bson, const char *key, const char *value);
} column_map_t;

int
get_column_map (bson_t            *bson_schema,
                const char        *table_name,
                column_map_t      **column_map,
                int               *column_map_size)
{
    bson_iter_t iter_col, iter_dup;
    int size;
    column_map_t *column_map_p;
    const char *data_type;
    data_type_map_t *data_type_map_p;

    bson_find_create_table (bson_schema, table_name, &iter_col) || DIE;
    for (iter_dup = iter_col, size = 0; bson_iter_next (&iter_dup); size++)
        ;
    *column_map = calloc (sizeof (column_map_t), size);
    *column_map_size = size;
    column_map_p = *column_map;
    while (bson_iter_next (&iter_col)) {
        bson_iter_t iter_col_prop;

        BSON_ITER_HOLDS_DOCUMENT (&iter_col) || DIE;
        bson_iter_recurse (&iter_col, &iter_col_prop) || DIE;
        bson_iter_find (&iter_col_prop, "column_name") || DIE;
        BSON_ITER_HOLDS_UTF8 (&iter_col_prop) || DIE;
        column_map_p->column_name = bson_iter_dup_utf8 (&iter_col_prop, NULL);
        bson_iter_find (&iter_col_prop, "data_type") || DIE;
        BSON_ITER_HOLDS_UTF8 (&iter_col_prop) || DIE;
        data_type = bson_iter_utf8 (&iter_col_prop, NULL);
        for (data_type_map_p = data_type_map;
             data_type_map_p < (data_type_map + sizeof (data_type_map)) &&
             strcmp (data_type_map_p->data_type, data_type) != 0;
             data_type_map_p++)
            ;
        if (data_type_map < (data_type_map + sizeof (data_type_map)))
            column_map_p->bson_append_from_s = data_type_map_p->bson_append_from_s ?
                data_type_map_p->bson_append_from_s : bson_append_utf8_from_s;
        else
            DIE;
        column_map_p++;
    }
    return true;
}

void
load_table (mongoc_database_t *db,
            const char        *table_name,
            bson_t            *bson_schema)
{
    column_map_t *column_map, *column_map_p;
    int column_map_size, i;
    FILE *fp;
    char *token;
    bson_t bson;

    printf ("load_table table_name: \"%s\"\n", table_name);
    get_column_map (bson_schema, table_name, &column_map, &column_map_size) || DIE;
    snprintf (mbdump_file, MAXPATHLEN, "%s/%s", mbdump_dir, table_name);
    printf ("mbdump_file: \"%s\"\n", mbdump_file);
    fp = fopen (mbdump_file, "r");
    if (!fp) DIE;
    fgets (buf, BUFSIZ, fp);
    fputs (buf, stdout);
    chomp (buf);
    bson_init (&bson);
    {
        for (i = 0, column_map_p = column_map, token = strtok(buf, "\t");
             i < column_map_size;
             i++, column_map_p++, token = strtok(NULL, "\t")) {
             bool ret;

             ret = (*column_map_p->bson_append_from_s) (&bson, column_map_p->column_name, token);
             ret || printf ("WARNING: column_map_p->bson_append_from_s failed column \"%s\": \"%s\"\n", column_map_p->column_name, token);
             /*
             printf ("%s: \"%s\"\n", column_map_p->column_name, token);
             */
        }
        bson_printf ("bson: %s\n", &bson);
        bson_reinit (&bson);
    }
    bson_destroy (&bson);
    fclose (fp);
    free (column_map);
}

int64_t
execute (int   argc,
         char *argv[])
{
    int64_t count = 0;
    const char *uristr = MONGODB_DEFAULT_URI;
    const char *database_name;
    mongoc_uri_t *uri;
    mongoc_client_t *client;
    mongoc_database_t *db;

    bson_t bson_schema;
    bson_error_t error;
    int argi;

    uristr = getenv ("MONGODB_URI");
    uri = mongoc_uri_new (uristr);
    client = mongoc_client_new (uristr);
    database_name = mongoc_uri_get_database (uri);
    db = mongoc_client_get_database (client, database_name);

    set_paths (argv);
    bson_init_from_json_file (&bson_schema, schema_file) || WARN_ERROR;
    for (argi = 0; argi < argc; argi++) {
        /* printf ("argv[%d]: \"%s\"\n", argi, argv[argi]); */
        load_table (db, argv[argi], &bson_schema);
    }
    bson_destroy (&bson_schema);

    mongoc_database_destroy (db);
    mongoc_client_destroy (client);
    mongoc_uri_destroy (uri);
    return count;
}

void
test_suite (void)
{
    struct timeval timeval;
    char *s = "2013-07-21 22:47:57.660809+00";

    pg_timestamp_with_time_zone_from_s (s, &timeval);
    printf ("TIMESTAMP: \"%s\", sec:%ld, usec:%ld\n", s, timeval.tv_sec, (long)timeval.tv_usec);
}

void
log_local_handler (mongoc_log_level_t  log_level,
                   const char         *log_domain,
                   const char         *message,
                   void               *user_data)
{
   /*
   printf ("log_local_handler MONGOC_LOG_LEVEL_INFO:%d log_level:%d\n", MONGOC_LOG_LEVEL_INFO, log_level);
   */
   if (log_level <= MONGOC_LOG_LEVEL_INFO)
      mongoc_log_default_handler (log_level, log_domain, message, user_data);
}

double
dtimeofday ()
{
   struct timeval tv;

   bson_gettimeofday (&tv, NULL);
   return tv.tv_sec + 0.000001 * tv.tv_usec;
}

int
main (int   argc,
      char *argv[])
{
   double start_time;
   int64_t count;
   double end_time;
   double delta_time;

   if (argc < 2) {
      DIE; /* pending - usage */
   }

   test_suite ();

   mongoc_init ();
   mongoc_log_set_handler (log_local_handler, NULL);

   start_time = dtimeofday ();
   count = execute (argc - 1, &argv[1]);
   end_time = dtimeofday ();
   delta_time = end_time - start_time + 0.0000001;
   fprintf (stderr, "info: real: %.2f, count: %"PRId64", %"PRId64" docs/sec\n", delta_time, count, (int64_t)round (count/delta_time));

   mongoc_cleanup ();

   return 0;
}

