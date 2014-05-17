CMDS = merge

TESTS = test-merge

all: $(CMDS) $(TESTS)

WARNINGS = -Wall -Wno-deprecated-declarations
DEBUG = -ggdb
OPTIMIZE = -O2 -mtune=native

CFLAGS = $(shell pkg-config --cflags libmongoc-1.0)
LIBS = $(shell pkg-config --libs libmongoc-1.0)

merge: merge.c
	$(CC) -o $@ $(WARNINGS) $(DEBUG) $(OPTIMIZE) $(CFLAGS) $(LIBS) -DMAIN merge.c

test-merge: merge.c test-merge.c
	$(CC) -o $@ $(WARNINGS) $(DEBUG) $(OPTIMIZE) $(CFLAGS) $(LIBS) merge.c test-merge.c

clean:
	rm -fr $(CMDS) $(TESTS) *.dSYM

test: test-merge
	MONGODB_URI='mongodb://localhost/test' ./test-merge
