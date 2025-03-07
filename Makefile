TARGET = bin/strem
SRC = $(wildcard src/*.c)

all: $(TARGET)

$(TARGET): $(SRC)
	mkdir -p bin 
	gcc -o $(TARGET) $(SRC) -lrdkafka

clean:
	rm -rf bin/

run: $(TARGET)
	./$(TARGET)

.PHONY: all clean run
