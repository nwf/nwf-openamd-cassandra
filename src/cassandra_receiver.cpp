#include <time.h>
#include <sys/time.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <stdint.h>
#include <unistd.h>

#include "Cassandra.h"
#include <sstream>
#include <protocol/TBinaryProtocol.h>
#include <transport/TSocket.h>
#include <transport/TTransportUtils.h>
#include <boost/lexical_cast.hpp>

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::cassandra;

std::string host("127.0.0.1");
int port= 9160;

#define FIFO_NAME "cass_pipe"

typedef struct rxevt {
	int id;
	time_t print_time;
	double avgx;
	double avgy;
	double avgz;
	int nsamp;
	
	int touch;
	time_t touch_time;
} rxevt;

int main() {
  boost::shared_ptr<TTransport> socket(new TSocket(host, port));
  boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
  boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
  CassandraClient client(protocol);
  double end_of_days = 1356940800.0;
  std::string keyspace = "HiveTest";
  int num;
  FILE *fp;

  rxevt rx;

  mknod(FIFO_NAME, S_IFIFO | 0666, 0);
  fp = fopen(FIFO_NAME, "r");

  if(!fp) {
	fprintf(stderr, "Unable to open input file: %s: %m\n", FIFO_NAME);
	return -1;
  } else {
	fprintf(stderr, "OK, here we go!\n");
  }

  while ((num = fscanf(fp, "%X %lX %lA %lA %lA %X %X@%lX\n",
					&rx.id, &rx.print_time,
					&rx.avgx, &rx.avgy, &rx.avgz, &rx.nsamp,
					&rx.touch, &rx.touch_time
  )) == 8) {
    // printf ("tag id=%u px=%f py=%f pz=%f\n", rx.id, rx.avgx, rx.avgy, rx.avgz);

    try {
      transport->open();

      double t = rx.print_time;

      ColumnPath new_col;
      new_col.__isset.column = true;
      new_col.__isset.super_column = true;
      new_col.column_family.assign("LocationHistory");
      new_col.super_column.assign(boost::lexical_cast<std::string>(rx.id));

      new_col.column.assign("x");    
      client.insert(keyspace,
		    boost::lexical_cast<std::string>(end_of_days-t),
		    new_col,
		    boost::lexical_cast<std::string>(rx.avgx),
		    t,ONE);
      new_col.column.assign("y");    
      client.insert(keyspace,
		    boost::lexical_cast<std::string>(end_of_days-t),
		    new_col,
		    boost::lexical_cast<std::string>(rx.avgy),
		    t,ONE); 
      new_col.column.assign("z");
      client.insert(keyspace,
		    boost::lexical_cast<std::string>(end_of_days-t),
		    new_col,
		    boost::lexical_cast<std::string>(rx.avgz),
		    t,ONE); 

      if(rx.touch_time != 0) {
          new_col.column.assign("button");    
          client.insert(keyspace,
	            boost::lexical_cast<std::string>(end_of_days-t),
                new_col,
                boost::lexical_cast<std::string>(rx.touch),
                t,ONE);  
      }

      transport->close();
    } catch (InvalidRequestException &re) {
      fprintf(stderr, "ERROR: %s\n", re.why.c_str());
	  return -2;
    } catch (TException &tx) {
      fprintf(stderr, "ERROR: %s\n", tx.what());
	  return -3;
    }
  }

  fprintf(stderr, "At bottom of loop, with num=%d\n", num);

  return -4;
}
