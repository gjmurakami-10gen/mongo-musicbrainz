CMDS = mongomerge

TESTS = test-mongomerge

all: $(CMDS) $(TESTS)

WARNINGS = -Wall -Wno-deprecated-declarations
DEBUG = -ggdb
OPTIMIZE = -O2 -mtune=native

CFLAGS = $(shell pkg-config --cflags libmongoc-1.0)
LIBS = $(shell pkg-config --libs libmongoc-1.0)

mongomerge: mongomerge.h mongomerge.c
	$(CC) -o $@ $(WARNINGS) $(DEBUG) $(OPTIMIZE) $(CFLAGS) $(LIBS) -DMAIN mongomerge.c

test-mongomerge: mongomerge.h mongomerge.c test-mongomerge.c
	$(CC) -o $@ $(WARNINGS) $(DEBUG) $(OPTIMIZE) $(CFLAGS) $(LIBS) mongomerge.c test-mongomerge.c

clean:
	rm -fr $(CMDS) $(TESTS) *.dSYM

test: test-mongomerge
	MONGODB_URI='mongodb://localhost/test' $$VALGRIND ./test-mongomerge
