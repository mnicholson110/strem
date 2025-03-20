TARGET = bin/strem
SRC = $(wildcard src/*.c)

UNAME_S := $(shell uname -s)

ifeq ($(UNAME_S),Darwin)
    # MacOS
    CFLAGS = -I/opt/homebrew/include
    LDFLAGS = -L/opt/homebrew/lib
    LDLIBS = -lrdkafka -ljson-c
else
    # Linux
    CFLAGS =
    LDFLAGS =
    LDLIBS = -lrdkafka -ljson-c
endif

EXTRA_FLAGS = -O3

all: clean $(TARGET)

$(TARGET): $(SRC)
	mkdir -p bin
	gcc $(CFLAGS) -Wall $(EXTRA_FLAGS) -o $(TARGET) $(SRC) $(LDFLAGS) $(LDLIBS)

# Debug target: adds debugging symbols, disables optimization, and enables AddressSanitizer
debug: EXTRA_FLAGS = -g -O0 -fsanitize=address
debug: clean $(TARGET)

clean:
	rm -rf bin/

run: $(TARGET)
	./$(TARGET)

.PHONY: all clean run debug
