##############################################################
#               CMake Project Wrapper Makefile               #
##############################################################
CC = g++
CFLAGS = -std=c++14 -g -Wall
OUT_FILE = ./badgerdb_main

all:
	cd src;\
	$(CC) $(CFLAGS) *.cpp exceptions/*.cpp -I. -o ${OUT_FILE}

run:
	cd src;\
	${OUT_FILE}

clean:
	cd src;\
	rm -f badgerdb_main test.?

format:
	find . \( -iname '*.h' -o -iname '*.cpp' \) -exec clang-format -style=Google -i {} \;

docs:
	doxygen Doxyfile
