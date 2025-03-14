TARGET = bin/strem
SRC = $(wildcard src/*.c)

all: $(TARGET)

$(TARGET): $(SRC)
	mkdir -p bin 
	gcc -Wall -O3 -o $(TARGET) $(SRC) -lrdkafka -ljson-c

clean:
	rm -rf bin/

run: $(TARGET)
	./$(TARGET)

.PHONY: all clean run
