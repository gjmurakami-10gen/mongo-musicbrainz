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

   if (argc < 3) {
      DIE; /* pending - usage */
   }
   mongoc_init ();
   mongoc_log_set_handler (log_local_handler, NULL);

   argvp = argv;
   parent_name = *argv++;

   start_time = dtimeofday ();
   count = execute (parent_name, argc - 2, argvp);
   end_time = dtimeofday ();
   delta_time = end_time - start_time + 0.0000001;
   fprintf (stderr, "info: real: %.2f, count: %"PRId64", %"PRId64" docs/sec\n", delta_time, count, (int64_t)round (count/delta_time));

   mongoc_cleanup ();

   return 0;
}

