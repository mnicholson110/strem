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

all: $(TARGET)

$(TARGET): $(SRC)
	mkdir -p bin
	gcc $(CFLAGS) -Wall -O3 -o $(TARGET) $(SRC) $(LDFLAGS) $(LDLIBS)

clean:
	rm -rf bin/

run: $(TARGET)
	./$(TARGET)

.PHONY: all clean run
