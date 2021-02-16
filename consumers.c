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
#include <fcntl.h>
#include <unistd.h>
#include <stdint.h>
#include <arpa/inet.h>
#include <limits.h>
#include <math.h>
#include "prodcon.h"

#define MIN(a,b) (a < b ? a : b)
#define MAX(a,b) (a < b ? b : a)
/*
**	Client
*/
typedef struct pair{
	pthread_t thread_id;
	int 	id;
}pair;

char *host;
char *port;
int  bad;
pair 	*pairs;

void* initiate_consumer(void * ptr);
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
		usleep(poissonRandomInterarrivalDelay(rate)*1E6);
		pairs[i].id = i;
		status = pthread_create(&pairs[i].thread_id,NULL,initiate_consumer,(void*)&pairs[i]);//(void*)&threads[i]
		if(status < 0)
			exit(-1);
	}
	for(int i = 0; i < N; i++){
		pthread_join(pairs[i].thread_id,NULL);
	}
	free(pairs);
}

void* initiate_consumer(void * pairptr){
	pthread_t 	tid = ((pair*)pairptr)->thread_id;
	
	char  		filename[BUFSIZE];
	
	sprintf(filename,"%ld",tid);
	memcpy(filename + strlen(filename),".txt",strlen(".txt")+1);
	char		buf[BUFSIZE];
	char 		*temp_ch;
	char 		*temp_buf = NULL;
	int32_t 	letter_count;
	int32_t 	conv;
	int 		cc;
	int 		csock;
	int 		counter;
	int 		fd = -1;
	
	if ( ( csock = connectsock( host, port, "tcp" )) == 0 )
	{
		fprintf( stderr, "Cannot connect to server.\n" );
		goto END;
	}

	if( ( ((pair*)pairptr)->id + 1 ) % bad == 0){
		printf("SLOW_CLIENT %d\n",((pair*)pairptr)->id + 1);
		sleep(SLOW_CLIENT);
	}

	if ( write( csock, "CONSUME\r\n", strlen("CONSUME\r\n") ) < 0 )
	{
		fprintf( stderr, "client write: %s\n", strerror(errno) );
		goto END;
	}
	
	temp_ch = (char*)&conv;
	if ( (cc = read( csock, temp_ch, sizeof(conv) )) <= 0)
	{
		printf( "Server closed socket.\n" );
		goto  END;
	}
	
	letter_count = ntohl(conv);
	if((fd = open(filename,O_WRONLY | O_CREAT,0777)) == -1){
		printf( "Could not open file to write.\n" );
		goto END;	
	}
	
	temp_buf = (char*)malloc(letter_count*sizeof(char));
	counter = 0;
	while (counter < letter_count)
	{
		if( (cc = read( csock, temp_buf + counter, MIN(BUFSIZE,letter_count - counter) ) ) <= 0 )
		{
			printf( "Read failed in temp_buf %d.\n", counter );
			goto END;
		}
		counter += cc;
	}
	counter = 0;
	while (counter < letter_count)
	{
		if( (cc = write( fd, temp_buf + counter, MIN(BUFSIZE,letter_count - counter) ) ) < 0 )
		{
			printf( "Write file failed %d.\n", counter );
			goto END;
		}
		counter += cc;
	}
	printf("%d bytes read\n",letter_count);
	END:close( csock );
	if(fd != -1)
		close( fd );	
	if(temp_buf != NULL)
		free(temp_buf);
	pthread_exit( NULL );
}

double poissonRandomInterarrivalDelay( double r )
{
    return (log((double) 1.0 - 
			((double) rand())/((double) RAND_MAX)))/-r;
}



