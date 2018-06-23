#ifndef __NGX_HTTP_KAFKA_MODULE__
#define __NGX_HTTP_KAFKA_MODULE__

#define KAFKA_TOPIC_MAXLEN      256
#define KAFKA_BROKER_MAXLEN     512

/*
c语言 头文件切勿定义变量
否则在引用头文件的每 个.c中各分配一次内存空间。
http://blog.chinaunix.net/uid-22762900-id-163517.html
*/
//extern int run;
extern rd_kafka_topic_partition_list_t* topics;  

typedef enum {
    ngx_str_push = 0,
    ngx_str_pop = 1
} ngx_str_op;

typedef struct {
    ngx_array_t	     *meta_brokers;
    rd_kafka_t       *rk;
    rd_kafka_conf_t  *rkc;

    size_t         broker_size;
    size_t         nbrokers;
    ngx_str_t     *brokers;
} ngx_http_kafka_main_conf_t;

typedef struct {
    ngx_log_t  *log;
    ngx_str_t   topic;
    ngx_str_t   broker;

    rd_kafka_topic_t *rkt;
    rd_kafka_topic_conf_t  *rktc;
	int init;

} ngx_http_kafka_loc_conf_t;

static char g_broker_list[KAFKA_BROKER_MAXLEN * 2];
static ngx_int_t ngx_http_kafka_init_worker(ngx_cycle_t *cycle);
static void ngx_http_kafka_exit_worker(ngx_cycle_t *cycle);

static void *ngx_http_kafka_create_main_conf(ngx_conf_t *cf);
static void *ngx_http_kafka_create_loc_conf(ngx_conf_t *cf);
static char *ngx_http_set_kafka_broker_list(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);
static char *ngx_http_set_kafka_topic(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);
static ngx_int_t ngx_http_kafka_handler(ngx_http_request_t *r);
void  msg_consume2 (ngx_http_request_t* r, char* info);
void ngx_http_kafka_post_callback_handler(ngx_http_request_t *r);

void  rd_kafka_conf_set_default_topic_conf(rd_kafka_conf_t  *rkc,  rd_kafka_topic_conf_t  *rktc); 
rd_kafka_message_t *rd_kafka_consumer_poll (rd_kafka_t *rk, int timeout_ms);
rd_kafka_resp_err_t rd_kafka_poll_set_consumer (rd_kafka_t *rk);
//rd_kafka_topic_partition_list_t* rd_kafka_topic_partition_list_new (int size);
//rd_kafka_resp_err_t rd_kafka_subscribe (rd_kafka_t *rk,  const rd_kafka_topic_partition_list_t *topics);
//static rd_kafka_topic_partition_t * rd_kafka_topic_partition_list_add (rd_kafka_topic_partition_list_t *rktparlist,const char *topic, int32_t partition);
void ngx_str_helper(ngx_str_t *str, ngx_str_op op);

#endif