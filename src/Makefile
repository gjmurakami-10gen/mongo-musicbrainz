CMDS = mbdump_to_mongo mongomerge
TESTS = test-aggregate test-cursor test-mongoload test-mongomerge

WARNINGS = -std=c89 -Wall -Wno-deprecated-declarations -Wno-format-extra-args -Wdeclaration-after-statement
DEBUG = -g
OPTIMIZE = -O2 -mtune=native

CFLAGS = $(shell pkg-config --cflags libmongoc-1.0) $(DEBUG) $(WARNINGS)
LIBS = $(shell pkg-config --libs libmongoc-1.0) -lm

SCHEMA_FILE = ../../mongo-musicbrainz/schema/create_tables.json
MBDUMP_DIR = ../../mongo-musicbrainz/data/fullexport/20140604-002730/mbdump

default: test

test: all
	MONGODB_URI='mongodb://localhost/test' $$VALGRIND ./mbdump_to_mongo $(SCHEMA_FILE) $(MBDUMP_DIR) area

all: $(CMDS) $(TESTS)

mbdump_to_mongo: mbdump_to_mongo.o
	$(CC) -o $@ $(WARNINGS) $(DEBUG) $(OPTIMIZE) $(CFLAGS) $^ $(LIBS)

mongomerge: mongomerge.o mongomerge_main.o
	$(CC) -o $@ $(WARNINGS) $(DEBUG) $(OPTIMIZE) $(CFLAGS) $^ $(LIBS)

test-aggregate: test-aggregate.c
	$(CC) -o $@ $(WARNINGS) $(DEBUG) $(OPTIMIZE) $(CFLAGS) $^ $(LIBS)

test-cursor: test-cursor.c
	$(CC) -o $@ $(WARNINGS) $(DEBUG) $(OPTIMIZE) $(CFLAGS) $^ $(LIBS)

test-mongoload: test-mongoload.c
	$(CC) -o $@ $(WARNINGS) $(DEBUG) $(OPTIMIZE) $(CFLAGS) $^ $(LIBS)

test-mongomerge: mongomerge.o test-mongomerge.o
	$(CC) -o $@ $(WARNINGS) $(DEBUG) $(OPTIMIZE) $(CFLAGS) $^ $(LIBS)

test-mongorestore:
	echo "db.test.drop();" | mongo
	ruby -e 'require "benchmark"; result = [];\
		tms = Benchmark.measure { result = `mongorestore -db test -c test ../twitter.bson 2>&1` };\
		objects = result.split(/\n/).grep(/objects found/).first.to_i;\
		puts "real: #{"%.2f" % tms.real}, docs: #{objects}, docs_per_second: #{(objects.to_f/tms.real).round}"'

clean:
	rm -fr $(CMDS) $(TESTS) *.o *.dSYM

mongomerge.o: mongomerge.h mongomerge.c
