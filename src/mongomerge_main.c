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

int
main (int argc,
      char *argv[])
{
   char **argvp;
   char *parent_name;

   if (argc < 3) {
      DIE; // usage
   }
   mongoc_init ();

   argvp = argv;
   parent_name = *argv++;

   execute(parent_name, argc - 2, argvp);

   mongoc_cleanup ();

   return 0;
}

