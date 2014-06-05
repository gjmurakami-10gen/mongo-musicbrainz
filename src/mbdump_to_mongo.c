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

#define MONGODB_DEFAULT_URI "mongodb://localhost/musicbrainz"

char *
realpath_replace (char *file_name)
{
    char temp_path[MAXPATHLEN];
    realpath(file_name, temp_path);
    strcpy(file_name, temp_path);
    return file_name;
}

char *
dirname_replace (char *file_name)
{
    char temp_path[MAXPATHLEN];
    snprintf (temp_path, MAXPATHLEN, "%s/../", file_name);
    strcpy(file_name, temp_path);
    realpath_replace (file_name);
    return file_name;
}

off_t
file_size (FILE * fp)
{
    struct stat buf;

    fstat (fileno(fp), &buf) == 0 || DIE;
    return buf.st_size;
}

char *
file_to_s (const char *file_name)
{
    FILE *fp;
    char *s;
    off_t st_size;

    fp = fopen(file_name, "r");
    if (!fp) DIE;
    st_size = file_size(fp);
    s = malloc (st_size + 1);
    fread (s, 1, st_size, fp) || DIE;
    s[st_size] = '\0';
    return s;
}

char *
chomp (char *s)
{
    size_t len;

    len = strlen(s);
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
    len = strlen(json) + 16;
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

    getcwd(cwd, MAXPATHLEN);
    snprintf (base_dir, MAXPATHLEN, "%s/%s", cwd, dirname(argv[0]));
    dirname_replace(base_dir);
    realpath_replace (base_dir);
    printf ("base_dir: \"%s\"\n", base_dir);
    snprintf (fullexport_dir, MAXPATHLEN, "%s/%s", base_dir, "ftp.musicbrainz.org/pub/musicbrainz/data/fullexport");
    printf ("fullexport_dir: \"%s\"\n", fullexport_dir);
    snprintf (latest_file, MAXPATHLEN, "%s/%s", fullexport_dir, "LATEST");
    printf ("latest_file: \"%s\"\n", latest_file);
    latest_name = chomp (file_to_s (latest_file));
    printf ("latest_name: \"%s\"\n", latest_name);
    snprintf (mbdump_dir, MAXPATHLEN, "%s/data/fullexport/%s/mbdump", base_dir, latest_name);
    free (latest_name);
    printf ("mbdump_dir: \"%s\"\n", mbdump_dir);
    snprintf (schema_file, MAXPATHLEN, "%s/schema/create_tables.json", base_dir);
    printf ("schema_file: \"%s\"\n", schema_file);
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

    uristr = getenv ("MONGODB_URI");
    uri = mongoc_uri_new (uristr);
    client = mongoc_client_new (uristr);
    database_name = mongoc_uri_get_database (uri);
    db = mongoc_client_get_database (client, database_name);

    set_paths (argv);
    bson_init_from_json_file (&bson_schema, schema_file) || WARN_ERROR;
    bson_printf ("bson_schema: %s\n", &bson_schema);
    bson_destroy (&bson_schema);

    mongoc_database_destroy (db);
    mongoc_client_destroy (client);
    mongoc_uri_destroy (uri);
    return count;
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
   char **argvp;
   char *parent_name;
   double start_time;
   int64_t count;
   double end_time;
   double delta_time;

   if (argc < 2) {
      DIE; /* pending - usage */
   }
   mongoc_init ();
   mongoc_log_set_handler (log_local_handler, NULL);

   argvp = argv;
   parent_name = *argv++;

   start_time = dtimeofday ();
   count = execute (argc - 2, argvp);
   end_time = dtimeofday ();
   delta_time = end_time - start_time + 0.0000001;
   fprintf (stderr, "info: real: %.2f, count: %"PRId64", %"PRId64" docs/sec\n", delta_time, count, (int64_t)round (count/delta_time));

   mongoc_cleanup ();

   return 0;
}

