CMDS = mongomerge

TESTS = test-cursor test-mongoload test-mongomerge

all: $(CMDS) $(TESTS)

WARNINGS = -Wall -Wno-deprecated-declarations
DEBUG = -ggdb
OPTIMIZE = -O2 -mtune=native

CFLAGS = $(shell pkg-config --cflags libmongoc-1.0) $(WARNINGS)
LIBS = $(shell pkg-config --libs libmongoc-1.0)

test-mongoload: test-mongoload.o
	$(CC) -o $@ $(WARNINGS) $(DEBUG) $(OPTIMIZE) $(CFLAGS) $^ $(LIBS)

mongomerge: mongomerge.o mongomerge_main.o
	$(CC) -o $@ $(WARNINGS) $(DEBUG) $(OPTIMIZE) $(CFLAGS) $^ $(LIBS)

test-cursor: test-cursor.o
	$(CC) -o $@ $(WARNINGS) $(DEBUG) $(OPTIMIZE) $(CFLAGS) $^ $(LIBS)

test-mongomerge: mongomerge.o test-mongomerge.o
	$(CC) -o $@ $(WARNINGS) $(DEBUG) $(OPTIMIZE) $(CFLAGS) $^ $(LIBS)

clean:
	rm -fr $(CMDS) $(TESTS) *.o *.dSYM

test: all
	MONGODB_URI='mongodb://localhost/test' $$VALGRIND ./test-mongoload

test-mongorestore:
	echo "db.test.drop();" | mongo
	ruby -e 'require "benchmark"; result = [];\
		tms = Benchmark.measure { result = `mongorestore -db test -c test ../twitter.bson 2>&1` };\
		objects = result.split(/\n/).grep(/objects found/).first.to_i;\
		puts "real: #{"%.2f" % tms.real}, docs: #{objects}, docs_per_second: #{(objects.to_f/tms.real).round}"'

mongomerge.c: mongomerge.h

