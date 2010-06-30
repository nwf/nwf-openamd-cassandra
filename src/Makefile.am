GEN_SRC = gen-cpp/Cassandra.cpp gen-cpp/cassandra_types.cpp

receiver_SOURCES = cassandra_receiver.cpp ${GEN_SRC}
receiver_CXXFLAGS = -std=c++0x -Wall -Werror -I/usr/local/include/thrift -I./gen-cpp
receiver_LDADD = -L/usr/local/lib -lthrift

bin_PROGRAMS = receiver
