#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <hiredis.h>
#include <async.h>
#include <adapters/libevent.h>
#include <unistd.h>
#include <getopt.h>
#include "cjson.h"

struct event_base *base;
struct event read_task_timeout;

struct redis_ev {
    redisAsyncContext *c;
    char *msg;
    struct event *evfd;
};

void write_taskCallback(redisAsyncContext *c, void *r, void *privdata)
{
    redisReply *reply = r;
    if (reply == NULL) return;
}

static void write_task_timeout_cb(evutil_socket_t fd, short event, void *arg)
{
    struct timeval tv;
    struct redis_ev *ev = NULL;

    ev = (struct redis_ev *)arg;

    redisAsyncCommand(ev->c, write_taskCallback, (char*)"[write_task]", "write_task XXXX %s", ev->msg);

    if(ev) {
        if(ev->msg)
            free(ev->msg);
        if(ev->evfd) {
            event_del(ev->evfd);
		    event_free(ev->evfd);
        }

        free(ev);
    }
}

void getCallback(redisAsyncContext *c, void *r, void *privdata)
{
    struct event *write_task_timeout = NULL;
    cJSON * root = NULL;
    struct timeval tv;
    struct redis_ev *ev;
    int time = 1;
    char *repdata = NULL;
    int repdata_len = 0;

    redisReply *reply = r;
    if (reply == NULL)
        return;

    /*get read_task data and handle*/
    if(reply->str) {
        /*json parse arg time*/
        root = cJSON_Parse((const char *)reply->str);
        if (!root) {
        	fprintf(stderr, "parse redis reply err![%s]\n", reply->str);
        	return;
        }

        if (cJSON_GetObjectItem(root, "time")) {
        	time = cJSON_GetObjectItem(root, "time")->valueint;
            /*fprintf(stdout, "timer task [%d] second\n", time);*/
        }
        else {
        	cJSON_Delete(root);
        	fprintf(stderr, "cannot parse time from read_task msg [%s]\n", reply->str);
        	return;
        }

        repdata_len = strlen(reply->str);
        repdata = (char *)malloc(repdata_len + 1);
        if(repdata == NULL) {
                fprintf(stderr, "repdata malloc err.\n");
                cJSON_Delete(root);
                return;
        }

        memset(repdata, 0, repdata_len);
        snprintf(repdata, (repdata_len + 1), "%s", reply->str);
        ev = (struct redis_ev *)malloc(sizeof(struct redis_ev));
        ev->c = c;
        ev->msg = repdata;

        memset(&tv,0,sizeof(struct timeval));
        tv.tv_sec = time;
        write_task_timeout = evtimer_new(base, write_task_timeout_cb, (void *)ev);
        event_add(write_task_timeout, &tv);

        /*for clean event ptr after write_task*/
        ev->evfd = write_task_timeout;
    }

    cJSON_Delete(root);
}

void authCallback(redisAsyncContext *c, void *r, void *privdata)
{
    redisReply *reply = r;
    if (reply == NULL) return;
    printf("[%s]: %s\n", (char*)privdata, reply->str);

    if(strncmp(reply->str, "OK", 2) != 0) {
        fprintf(stderr, "redis AUTH error, Disconnect.\n");
        redisAsyncDisconnect(c);
        exit(1);
    }
}

void connectCallback(const redisAsyncContext *c, int status)
{
    if (status != REDIS_OK) {
        printf("Error: %s\n", c->errstr);
        exit(1);
    }
    printf("Connected...\n");
}

void disconnectCallback(const redisAsyncContext *c, int status)
{
    if (status != REDIS_OK) {
        printf("Error: %s\n", c->errstr);
        exit(1);
    }
    printf("Disconnected...\n");
}

static void read_task_timeout_cb(evutil_socket_t fd, short event, void *arg)
{
    struct timeval tv;
    redisAsyncContext *c = NULL;

    c = (redisAsyncContext *)arg;

    redisAsyncCommand(c, getCallback, (char*)"[read_task]", "read_task XXXX");

    memset(&tv, 0, sizeof(struct timeval));
	tv.tv_usec = 1000;
	if(event_add(&read_task_timeout, &tv) < 0) {
		printf("read_task timer add failed in read_task_timeout_cb\n");
	}
}

static struct option opts[] = {
     {"help",    no_argument,    NULL, 'h'},
     {"ip",    1,    NULL, 'i'},
     {"port", 1,    NULL, 'p'},
     {"auth", 1,    NULL, 'a'}
 };


int main (int argc, char **argv)
{
    struct timeval tv;
    redisAsyncContext *c = NULL;
    int opt = 0;
    char *ip = NULL;
    int port = NULL;
    char *auth = NULL;

    while ((opt = getopt_long(argc, argv, "hi:p:a:", opts, NULL)) != -1) {
        switch (opt) {
        case 'a':
            if(optarg == NULL) {
                printf("error: auth passwd is null.\n");
                return -1;
            }
            auth = optarg;
            break;
        case 'h':
            printf("Usage: ./redis_timer -h | --help\n");
            printf(" -i --ip         redis db IP address.\n");
            printf(" -a --auth       redis auth passwd.\n");
            printf(" -p --port       redis db listen port.\n");
            printf("eg: ./redis_timer -i 192.168.2.171 -p 6379 -a 123456\n");
            return 0;
        case 'i':
            if(optarg == NULL) {
                printf("error: redis ip address null.\n");
                return -1;
            }
            ip = optarg;
            break;
        case 'p':
            if(optarg == NULL) {
                printf("error: redis port null.\n");
                return -1;
            }
            port = atoi(optarg);
            break;
        default:
            printf("Usage: ./redis_timer -h | --help\n");
            printf(" -i --ip         redis db IP address.\n");
            printf(" -a --auth       redis auth passwd.\n");
            printf(" -p --port       redis db listen port.\n");
            printf("eg: ./redis_timer -i 192.168.2.171 -p 6379 -a 123456\n");
            break;
        }
    }

    signal(SIGPIPE, SIG_IGN);

    if((argc > 1) && ip && port && auth) {
        printf("Commond parameter set complete.\n");
    }
    else {
        printf("Commond parameter setting error | Usage : -h .\n");
        exit(1);
    }

    base = event_base_new();
    /*connect redis*/
    c = redisAsyncConnect(ip, port);
    if (c->err) {
        /* Let *c leak for now... */
        printf("Error: %s\n", c->errstr);
        return 1;
    }

    redisLibeventAttach(c, base);
    redisAsyncSetConnectCallback(c, connectCallback);
    redisAsyncSetDisconnectCallback(c, disconnectCallback);
    redisAsyncCommand(c, authCallback, (char*)"AUTH", "Auth %s", auth);

    event_assign(&read_task_timeout, base, -1, 0, read_task_timeout_cb, (void *)c);
    memset(&tv,0,sizeof(struct timeval));
    tv.tv_usec = 1000;
	event_add(&read_task_timeout, &tv);

    event_base_dispatch(base);
    event_base_free(base);

    return 0;
}
