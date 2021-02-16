#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdint.h>
#include <arpa/inet.h>
#include <limits.h>
#include <math.h>
#include "prodcon.h"

#define MIN(a,b) ( a < b ? a : b)
#define MAX(a,b) ( a < b ? b : a)

typedef struct pair{
	pthread_t thread_id;
	int 	id;
}pair;

pair *pairs;
char *host;
char *port;
int   bad;
void* initiate_producer(void * ptr);
double poissonRandomInterarrivalDelay( double r );


int main( int argc, char *argv[] )
{
	int 		N;
	int 		status;
	double 		rate;
	
	if(argc != 6 && argc != 5){
		fprintf( stderr, "usage: chat [host] port clients rate bad\n" );
		exit(-1);
	}
	if(argc == 6){
		host = argv[1];
		port = argv[2];
		N = atoi(argv[3]);
		rate = atof(argv[4]);
		bad = atoi(argv[5]);
	}else{
		host = "localhost";
		port = argv[1];
		N = atoi(argv[2]);
		rate = atof(argv[3]);
		bad = atoi(argv[4]);
	}
	if(rate < 0){
		fprintf( stderr, "rate should be positive\n" );
		exit(-1);
	}
	if(bad < 0 || bad > 100){
		fprintf( stderr, "bad should between 0 and 100\n" );
		exit(-1);
	}
	if(bad == 0){
		bad = INT_MAX;
	}else{
		bad = 100/bad;
	}
	pairs = (pair*)malloc(sizeof(pair) * N);
	/*	Create the N sockets in threads to the controller  */
	
	for (int i = 0; i< N; i++){
		usleep(poissonRandomInterarrivalDelay(rate) * 1E6);
		pairs[i].id = i;
		status = pthread_create(&pairs[i].thread_id,NULL,initiate_producer,(void*)&pairs[i]);
		if(status < 0)
			exit(-1);
	}
	for(int i = 0; i < N; i++){
		pthread_join(pairs[i].thread_id,NULL);
	}
	free(pairs);
}

void* initiate_producer(void *pairptr){
	srand ( time(NULL) );
	char		buf[BUFSIZE]; // from prodcon.h
	char 		*temp_ch;
	char 		*temp_buff = NULL;
	int32_t 	conv;
	int32_t 	letter_count;
	int 		cc;
	int 		csock;
	int 		counter;
	

	if ( ( csock = connectsock(host, port, "tcp" )) == 0 )
	{
		fprintf( stderr, "Cannot connect to server.\n" );
		goto END;
	}

	if( ( ((pair*)pairptr)->id + 1 ) % bad == 0 ){
		printf("SLOW_CLIENT %d\n",((pair*)pairptr)->id + 1);
		sleep(SLOW_CLIENT);

	}
	
	if ( write( csock, "PRODUCE\r\n", strlen("PRODUCE\r\n") ) < 0 )
	{
		fprintf( stderr, "client write: %s\n", strerror(errno) );
		goto END;
	}
	if ( (cc = read( csock, buf, strlen("GO\r\n") )) <= 0)
	{
		printf( "Server closed socket.\n" );
		goto END;
	}
	
	if ( strcmp(buf, "GO\r\n") != 0 )
	{
		printf( "Expected GO+CRLF, found %s",buf);
		goto END;		
	}
	
	letter_count 	= random()%MAX_LETTERS + 10;
	conv 			= htonl(letter_count);
	temp_ch 		= (char*)&conv;
	
	if ( write( csock, temp_ch, sizeof(conv) ) < 0 )
	{
		fprintf( stderr, "client write: %s\n", strerror(errno) );
		goto END;
	}
	
	counter = 0;
	temp_buff = (char*)malloc(letter_count * sizeof(char));
	for(size_t i = 0; i < letter_count; i++)
		temp_buff[i] = random()%26 + 'a';

	while( counter < letter_count )
	{
		if ( (cc = write( csock, temp_buff + counter, MIN( BUFSIZE, letter_count-counter ) ) )< 0 ){
			fprintf( stderr, "client write: %s\n", strerror(errno) );
			goto END;
		}
		counter += cc;
	}
	printf( "%d bytes sent\n", letter_count );
	if ( (cc = read( csock, buf, strlen("DONE\r\n") )) <= 0)
	{
		printf( "The server has gone.\n" );
		goto END;
	}

	buf[cc]='\0';
	if ( strcmp(buf, "DONE\r\n") != 0 )
	{
		printf( "Expected DONE\\r\\n, found %s",buf);
		goto END;
	}
	END:close( csock );
	if( temp_buff != NULL)
		free( temp_buff );
	pthread_exit(NULL);
}

double poissonRandomInterarrivalDelay( double r )
{
    return (log((double) 1.0 - 
			((double) rand())/((double) RAND_MAX)))/-r;
}