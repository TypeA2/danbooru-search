CXX := g++
CXXFLAGS := -Wall -Wextra -Wpedantic -std=c++23 -O3

all: search

search: search.cpp
	$(CXX) $(CXXFLAGS) -o search search.cpp

clean:
	rm search
