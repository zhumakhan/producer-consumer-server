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
#include <semaphore.h>
#include <sys/select.h>

#include "prodcon.h"
#define MIN(a,b) ( a < b ? a : b)
#define MAX(a,b) (	a < b? b : a)

typedef struct pair{
	pthread_t thread_id;
	int	sock_id;
	int     id;
} pair;

int BUF_SIZE = BUFSIZE;

pthread_mutex_t mutex;
sem_t 	full, empty;

int 	count;
ITEM 	**items;

ITEM *makeItem(int letter_count, char *buf);
void *serve_producer(void* ssockid);
void *serve_consumer(void* ssockid);

int	     ended_threads[MAX_CLIENTS] = {0};
int 	 empty_threads_counter = MAX_CLIENTS;
int 	producers;
int     consumers;
pair	pairs[MAX_CLIENTS];

fd_set			rfds;
fd_set			afds;
int				nfds;

int main( int argc, char *argv[] )
{
	char			buf[BUF_SIZE];
	char			*service;
	struct 			sockaddr_in	fsin;
	int				msock;
	int				ssock;
	int				rport;
	int				alen;
	int				fd;
	int				cc;
	int 			i;	
	int 			status;
	int 			pcexceed;

	if( argc == 3 )
	{
		rport = 0;
		service = argv[1];
		BUF_SIZE = atoi(argv[2]);
	}
	else if( argc == 2 ){
		rport = 1;
		BUF_SIZE = atoi(argv[1]);
	}
	else
	{
		printf( "Please specify: [port] buffersize ");
		exit(-1);
	}
	
	items = (ITEM**)malloc(sizeof(ITEM*) * BUF_SIZE);

	pthread_mutex_init( &mutex, NULL );
	sem_init( &full, 0, 0 );
	sem_init( &empty, 0, BUF_SIZE );

	count = 0;
	producers = 0;
	consumers = 0;
	
	msock = passivesock( service, "tcp", QLEN, &rport );
	if ( rport )
		printf( "server: port %d\n", rport );	
	else
		printf( "server: port %s\n", service );	

	fflush( stdout );
	
	nfds = msock+1;
	FD_ZERO(&afds);
	FD_SET(msock, &afds);

	for(i = 0; i < MAX_CLIENTS; i++)
		ended_threads[i] = i;
	
	while (1)
	{
		memcpy((char*)&rfds, (char*)&afds, sizeof(rfds));
		
		if(select(nfds, &rfds, (fd_set*)0, (fd_set*)0, (struct timeval*)0) < 0){
			fprintf(stderr,"server select: %s\n",strerror(errno));
			exit(-1);
		}

		if(FD_ISSET(msock, &rfds)){
			alen = sizeof(fsin);
			ssock = accept(msock, (struct sockaddr*)&fsin, &alen);
			
			if(ssock < 0){
				fprintf(stderr, "accept: %s\n",strerror(errno));
				exit(-1);
			}

			FD_SET(ssock, &afds);
			if(ssock+1 > nfds)
				nfds = ssock+1;

		}

		for ( fd = 0; fd < nfds; fd++ )
		{
			// check every socket to see if it's in the ready set
			// But don't recheck the main socket
			if (fd != msock && FD_ISSET(fd, &rfds))
			{
				// you can read without blocking because data is there
				// the OS has confirmed this
				if ( (cc = read( fd, buf, BUFSIZE )) <= 0 )
				{
					printf( "The client has gone.\n" );
					(void) close(fd);
					
					// If the client has closed the connection, we need to
					// stop monitoring the socket (remove from afds)
					FD_CLR( fd, &afds );
					if ( nfds == fd+1 )
						nfds--;

				}
				else
				{	
					if( empty_threads_counter == 0 )
					{
						close( fd );
						FD_CLR( fd, &afds );
						if ( nfds == fd+1 )
							nfds--;
						printf( "CLOSED SOCKET\n" );	
						continue;		
					}
					else
					{
						pthread_mutex_lock( &mutex );
						i = ended_threads[ --empty_threads_counter ]; 
						pthread_mutex_unlock( &mutex );

						pairs[i].sock_id = fd;
						pairs[i].id = i;	

					}
					buf[cc] = '\0';
					pcexceed = 0;
					if( strcmp(buf,"PRODUCE\r\n") == 0 ){
						
						if(producers+1  > MAX_PROD){
							pcexceed = 1;
							goto PCEXCEED;
						}
						producers+=1;
						status = pthread_create(&pairs[i].thread_id,NULL,serve_producer,(void*)&pairs[i]);			
						FD_CLR( fd, &afds );
						if ( nfds == fd+1 )
							nfds--;
						if( status < 0 )
						{
							fprintf( stdout,"failed with status %d ",status );
							fflush( stdout );
							break;
						}
					}else if( strcmp(buf,"CONSUME\r\n") == 0 ){
						if(consumers+1 > MAX_CON){
							pcexceed = 1;
							goto PCEXCEED;
						}
						consumers+=1;
						status = pthread_create(&pairs[i].thread_id,NULL,serve_consumer,(void*)&pairs[i]);			
						FD_CLR( fd, &afds );
						if ( nfds == fd+1 )
							nfds--;
						if( status < 0 )
						{
							fprintf( stdout,"failed with status %d ",status );
							fflush( stdout );
							break;
						}
					}else{
						fprintf( stdout,"Nor producer nor consumer\n");
						fflush( stdout );
						close(fd);
						FD_CLR( fd, &afds );
						if ( nfds == fd+1 ){
							nfds--;
						}
					}
					PCEXCEED:
					if(pcexceed == 1){
						close(fd);
						FD_CLR( fd, &afds );
						if ( nfds == fd+1 )
							nfds--;
						pthread_mutex_lock( &mutex );
						ended_threads[empty_threads_counter++] = i;
						pthread_mutex_unlock( &mutex );
					}

				}
			}//if

		}//for
		
	}//while
}//main


void* serve_producer(void* pairptr){
	int 		ssockid 		= 		((pair *)pairptr)->sock_id;
	char 		*temp_buf;
	char 		*temp_ch;
	int 		cc;
	int32_t 	letter_count;	
	int32_t 	conv;
	int 		temp_counter;
	ITEM 		*item;

	if ( write( ssockid, "GO\r\n", strlen( "GO\r\n" ) ) < 0 )
	{	
		printf( "Cant write.\n" );
		goto END;
	}

	temp_ch = (char*)&conv;	
	if ( (cc = read( ssockid, temp_ch, sizeof(conv) )) <= 0 )
	{	
		printf( "The client has gone2.\n" );
		goto END;
	}
	
	letter_count = ntohl(conv);
	temp_buf = (char*)malloc(letter_count);
	temp_counter = 0;
	while (temp_counter < letter_count)
	{
		if( (cc = read( ssockid, temp_buf + temp_counter, MIN(BUFSIZE,letter_count - temp_counter) ) ) <= 0 )
		{
			printf( "Read failed in temp_buf %d.\n", count );
			goto END;
		}
		temp_counter += cc;
	}
	printf("%d bytes read\n",temp_counter);		
		
	item = makeItem(letter_count, temp_buf);	
	sem_wait( &empty );
	pthread_mutex_lock( &mutex );
	items[count] = item;
	count++;
	printf( "P Count %d.\n", count );
	pthread_mutex_unlock( &mutex );
	sem_post( &full );

	if ( write( ssockid, "DONE\r\n", strlen("DONE\r\n") ) < 0 )
		goto END;
	
	END:
	producers-=1;
	close(ssockid);
	pthread_mutex_lock( &mutex );
	ended_threads[empty_threads_counter++] = ((pair *)pairptr)->id;
	pthread_mutex_unlock( &mutex );
	pthread_exit( NULL );

}

void* serve_consumer(void* pairptr){
	int 		ssockid 		= 		((pair *)pairptr)->sock_id;
	char 		buf[BUF_SIZE];
	char 		*temp_buf;
	char 		*temp_ch;
	int 		cc;
	int32_t 	letter_count;	
	int32_t 	conv;
	int 		temp_counter;
	ITEM 		*item;

	sem_wait( &full );
	pthread_mutex_lock( &mutex );
	item = items[count-1];
	items[count-1]=NULL;
	count--;
	printf( "C Count %d.\n", count );
	pthread_mutex_unlock( &mutex );
	sem_post( &empty );

	conv = htonl(item->size);
	temp_ch = (char*)&(conv);
		
	if ( write( ssockid, temp_ch, sizeof(conv) ) < 0 ){
		printf( "Write failed in temp_ch %d.\n", count );
		goto END;
	}

	temp_counter = 0;
	while( temp_counter < item->size ){
		if ( (cc = write( ssockid, item->letters + temp_counter, MIN(BUFSIZE,item->size - temp_counter) ) ) < 0 ) {
			printf( "Write failed %d.\n", temp_counter );
			goto END;
		}		
		temp_counter += cc;
	}
	printf( "%d bytes written\n",temp_counter );
	END:
	consumers-=1;
	close(ssockid);
	free(item->letters);
	free(item);
	printf("FREED\n");
	fflush(stdout);
	pthread_mutex_lock( &mutex );
	ended_threads[empty_threads_counter++] = ((pair *)pairptr)->id;
	pthread_mutex_unlock( &mutex );
	pthread_exit( NULL );
}


ITEM *makeItem(int letter_count, char *buf)
{
	ITEM *p = malloc( sizeof(ITEM) );
	p->size = letter_count;
	p->letters = buf;	
	return p;
}

