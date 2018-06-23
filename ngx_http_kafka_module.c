#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>

#include <librdkafka/rdkafka.h>
#include "ngx_http_kafka_module.h"


int run = 1;
//extern rd_kafka_topic_partition_list_t* topics; 




static ngx_command_t ngx_http_kafka_commands[] = {
    {
        ngx_string("kafka.broker.list"),
        NGX_HTTP_MAIN_CONF|NGX_CONF_1MORE,
        ngx_http_set_kafka_broker_list,
        NGX_HTTP_MAIN_CONF_OFFSET,
        offsetof(ngx_http_kafka_main_conf_t, meta_brokers),
        NULL },
    {
        ngx_string("kafka.topic"),
        NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
        ngx_http_set_kafka_topic,
        NGX_HTTP_LOC_CONF_OFFSET,
        offsetof(ngx_http_kafka_loc_conf_t, topic),
        NULL },
    ngx_null_command
};

static ngx_http_module_t ngx_http_kafka_module_ctx = {
    NULL,
    NULL,

    ngx_http_kafka_create_main_conf,      
    NULL,

    NULL,
    NULL,

    ngx_http_kafka_create_loc_conf,
    NULL,
};

ngx_module_t ngx_http_kafka_module = {
    NGX_MODULE_V1,
    &ngx_http_kafka_module_ctx,
    ngx_http_kafka_commands,
    NGX_HTTP_MODULE,

    NULL,
    NULL,
    ngx_http_kafka_init_worker,
    NULL,
    NULL,
    ngx_http_kafka_exit_worker,
    NULL,

    NGX_MODULE_V1_PADDING
};

ngx_int_t ngx_str_equal(ngx_str_t *s1, ngx_str_t *s2)
{
    if (s1->len != s1->len) {
        return 0;
    }
    if (ngx_memcmp(s1->data, s2->data, s1->len) != 0) {
        return 0;
    }
    return 1;
}

void *ngx_http_kafka_create_main_conf(ngx_conf_t *cf)
{
    ngx_http_kafka_main_conf_t  *conf;

    conf = ngx_pcalloc(cf->pool, sizeof(ngx_http_kafka_main_conf_t));
    if (conf == NULL) {
        return NGX_CONF_ERROR;
    }

    conf->rk = NULL;
    conf->rkc = NULL;

    conf->broker_size = 0;
    conf->nbrokers = 0;
    conf->brokers = NULL;

    return conf;
}

void *ngx_http_kafka_create_loc_conf(ngx_conf_t *cf)
{
    ngx_http_kafka_loc_conf_t  *conf;

    conf = ngx_pcalloc(cf->pool, sizeof(ngx_http_kafka_loc_conf_t));
    if (conf == NULL) {
        return NGX_CONF_ERROR;
    }
    conf->log = cf->log;
    ngx_str_null(&conf->topic);
    ngx_str_null(&conf->broker);

    return conf;
}

void kafka_callback_handler(rd_kafka_t *rk, void *msg, size_t len, int err, void *opaque, void *msg_opaque)
{
    if (err != 0) {
        ngx_log_error(NGX_LOG_ERR, (ngx_log_t *)msg_opaque, 0, rd_kafka_err2str(err));
    }
}

char *ngx_http_set_kafka_broker_list(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
	fprintf(stderr, "7777777\n");
    ngx_str_t	*value;
    ngx_uint_t	i;
    char 		*ptr;
    memset(g_broker_list, 0, sizeof(g_broker_list));
    ptr = g_broker_list;

    for (i = 1; i < cf->args->nelts; i++) {
        value = cf->args->elts;
        ngx_str_t url = value[i];
        memcpy(ptr, url.data, url.len);
        ptr += url.len;
        *ptr++ = ',';
    }
    *(ptr - 1) = '\0';

    return NGX_CONF_OK;
}

char *ngx_http_set_kafka_topic(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
	fprintf(stderr, "666666666666\n");

    ngx_http_core_loc_conf_t   *clcf;
    ngx_http_kafka_loc_conf_t  *local_conf;

    clcf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module);
    if (clcf == NULL) {
        return NGX_CONF_ERROR;
    }
    clcf->handler = ngx_http_kafka_handler;

    if (ngx_conf_set_str_slot(cf, cmd, conf) != NGX_CONF_OK) {
        return NGX_CONF_ERROR;
    }

    local_conf = conf;

    local_conf->rktc = rd_kafka_topic_conf_new();

    return NGX_CONF_OK;
}

void msg_consume (rd_kafka_message_t *rkmessage, void *opaque) {
	ngx_http_request_t* request = (ngx_http_request_t*)opaque;
	char* res = NULL;
fprintf(stderr,"rkmessage  partition %d\n", rkmessage->partition);

  if (rkmessage->err) {  
    

	if (rkmessage->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
		//fprintf(stderr," aaaa  Consumer reached end of [%ld] \n",rkmessage->offset);
		// run =0;
	msg_consume2(request, "no data, daye");
		return;
	}

	    if (rkmessage->rkt)  {
		msg_consume2(request, "no data, rkt");
		return;
		} else {
		msg_consume2(request, "no data, nb");
		return;
			}
  }
	
	res = (char*)rkmessage->payload;
	fprintf(stderr, "input: %s\n", res);
	msg_consume2(request, res);
}

void msg_consume2 (ngx_http_request_t* request, char* info) {

	ngx_buf_t	*buf;
	ngx_chain_t		out;	

	buf = ngx_pcalloc(request->pool, sizeof(ngx_buf_t));
	out.buf = buf;
	out.next = NULL;
	buf->pos = (u_char *)info;
	buf->last = (u_char *)info + strlen(info);
	buf->memory = 1;
	buf->last_buf = 1;

	ngx_str_set(&(request->headers_out.content_type), "text/html");
	request->headers_out.status = NGX_HTTP_OK;
	request->headers_out.content_length_n = strlen(info);
	ngx_http_send_header(request);

	if( ngx_http_output_filter(request, &out) ==NGX_OK ) {
		ngx_log_error(NGX_LOG_ERR , request->connection->log, 0, "send http outputfilter OK!");
	}else{
		ngx_log_error(NGX_LOG_ERR, request->connection->log, 0, "send http outputfilter FAILE!");
	}

	ngx_http_finalize_request(request, NGX_HTTP_REQUEST_URI_TOO_LARGE);
}

void ngx_http_kafka_post_callback_handler(ngx_http_request_t *request) {
	
	//msg_consume(rkmessage, NULL);  
	/*释放rkmessage的资源，并把所有权还给rdkafka*/  

	rd_kafka_message_t *rkmessage;  
	ngx_http_kafka_main_conf_t    *main_conf = NULL;
	main_conf = ngx_http_get_module_main_conf(request, ngx_http_kafka_module);

	/*
	轮询消费者的消息或事件，最多阻塞timeout_ms 
	应用程序应该定期调用consumer_poll()，即使没有预期的消息，以服务 
	所有排队等待的回调函数，当注册过rebalance_cb，该操作尤为重要， 
	因为它需要被正确地调用和处理以同步内部消费者状态 
	*/  
	//while(1)
	{
		rkmessage = rd_kafka_consumer_poll(main_conf->rk, 1000);  

		if(rkmessage){  
			msg_consume(rkmessage, request);
		}  else {
			fprintf(stderr, "no data, cao csao \n");
		}
	}
}

/*
每一个location 对应的handler 中 将topic放进去
local_conf里的init 是个开关, 初始值为0, 第一个请求过来时设置为1, 这样不用每次绑定topic了
*/
static ngx_int_t ngx_http_kafka_handler(ngx_http_request_t *request) {

	ngx_http_kafka_loc_conf_t       *localConf;
//	ngx_http_kafka_main_conf_t    *mainConf;
	localConf = ngx_http_get_module_loc_conf(request, ngx_http_kafka_module);
	//int ret;
	if (localConf->init == 0) {
	//	const char* topic = NULL;
	//	topic = (const char *)localConf->topic.data;
	//	ngx_log_error(NGX_LOG_ERR , request->connection->log, 0, "topic is %s\n", topic);

	//	mainConf = ngx_http_get_module_main_conf(request, ngx_http_kafka_module);

//		localConf->rkt = rd_kafka_topic_new(mainConf->rk, topic, localConf->rktc);


	//rd_kafka_conf_set_default_topic_conf(mainConf->rkc, localConf->rktc);


				


		/*
		RD_KAFKA_OFFSET_END
		RD_KAFKA_OFFSET_BEGINNING
		*/

//		int ret;
//		ret = rd_kafka_consume_start(localConf->rkt, 0, RD_KAFKA_OFFSET_END);
//		  if (ret == -1) {
//			fprintf(stderr, "rd_kafka_consume_start error\n");
//		  } else {
//			fprintf(stderr, "rd_kafka_consume_start ok\n");
//		  }
		//rd_kafka_conf_set(mainConf->rkc, "auto.offset.reset", "RD_KAFKA_OFFSET_BEGINNING", NULL, 0);
		//rd_kafka_topic_conf_set(localConf->rktc, "auto.offset.reset", "RD_KAFKA_OFFSET_BEGINNING", NULL, 0);

		  


		// char errstr[512];  
		// Consumer groups always use broker based offset storage
//		if (rd_kafka_conf_set(mainConf->rkc, "offset.store.method", "broker", errstr, sizeof(errstr)) !=  RD_KAFKA_CONF_OK) {  
//			fprintf(stderr, "%% %s\n", errstr);  
//			return -1;  
//		}  else {
//			fprintf(stderr," rd_kafka_conf_set  offset.store.methodok\n");
//		}

/*
		if (rd_kafka_conf_set(mainConf->rkc, "auto.offset.reset", "latest", errstr, sizeof(errstr)) !=  RD_KAFKA_CONF_OK) {  
			fprintf(stderr, "%% %s\n", errstr);  
			return -1;  
		}   else {
			fprintf(stderr," rd_kafka_conf_set ok\n");
		}
*/


	//	localConf->init = 1;
	}

	rd_kafka_message_t *rkmessage;  
	ngx_http_kafka_main_conf_t    *main_conf = NULL;
	main_conf = ngx_http_get_module_main_conf(request, ngx_http_kafka_module);

	/*
	轮询消费者的消息或事件，最多阻塞timeout_ms 
	应用程序应该定期调用consumer_poll()，即使没有预期的消息，以服务 
	所有排队等待的回调函数，当注册过rebalance_cb，该操作尤为重要， 
	因为它需要被正确地调用和处理以同步内部消费者状态 
	*/  
	//while(1)
	{
//		rkmessage =  rd_kafka_consume(localConf->rkt, 0, 1000);
		rkmessage = rd_kafka_consumer_poll(main_conf->rk, 1000);  

		if(rkmessage){ 
			msg_consume(rkmessage, request);
		}  else {
			fprintf(stderr, "no data, haha \n");
				msg_consume2(request, "no result msg_consume");
		}
	}

	/*
	ngx_int_t  rv;
	rv = ngx_http_read_client_request_body(request, ngx_http_kafka_post_callback_handler);
	if (rv >= NGX_HTTP_SPECIAL_RESPONSE) {
		return rv;
	}
	*/

	return NGX_DONE;

	 


//	    ngx_http_kafka_main_conf_t    *main_conf;
//    ngx_http_kafka_loc_conf_t       *local_conf;
//		main_conf = NULL;
//	    main_conf = ngx_http_get_module_main_conf(r, ngx_http_kafka_module);
//    local_conf = ngx_http_get_module_loc_conf(r, ngx_http_kafka_module);
//   
//
//	fprintf(stderr, "rd_kafka_consume_start\n");
//
//	while(1)
//	{
//
//
//	 rd_kafka_message_t *rkmessage;  
//      /*-轮询消费者的消息或事件，最多阻塞timeout_ms 
//        -应用程序应该定期调用consumer_poll()，即使没有预期的消息，以服务 
//        所有排队等待的回调函数，当注册过rebalance_cb，该操作尤为重要， 
//        因为它需要被正确地调用和处理以同步内部消费者状态 */  
//      rkmessage = rd_kafka_consumer_poll(main_conf->rk, 1000);  
//
//      if(rkmessage){  
//		  fprintf(stderr, "dddd  the result is %s\n", (char*)rkmessage->payload);
//		//msg_consume2(r, 0);
//      }  else {
//		 fprintf(stderr, "no data\n");
//	  }
//
//	}
//    return NGX_DONE;
}

static int partition_cnt = 0;
static int eof_cnt = 0;
 void rebalance_cb (rd_kafka_t *rk,
			  rd_kafka_resp_err_t err,
			  rd_kafka_topic_partition_list_t *partitions,
			  void *opaque) {

	switch (err)
	{
	case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
		fprintf(stderr,
			"%% Group rebalanced: %d partition(s) assigned\n",
			partitions->cnt);
		eof_cnt = 0;
		partition_cnt = partitions->cnt;
		rd_kafka_assign(rk, partitions);
		break;

	case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
		fprintf(stderr,
			"%% Group rebalanced: %d partition(s) revoked\n",
			partitions->cnt);
		eof_cnt = 0;
		partition_cnt = 0;
		rd_kafka_assign(rk, NULL);
		break;

	default:
		break;
	}
}

ngx_int_t ngx_http_kafka_init_worker(ngx_cycle_t *cycle) {
	ngx_http_kafka_main_conf_t  *main_conf;
	  rd_kafka_topic_conf_t *topic_conf;  

    main_conf = ngx_http_cycle_get_module_main_conf(cycle, ngx_http_kafka_module);
    main_conf->rkc = rd_kafka_conf_new();
	fprintf(stderr, "555555555555\n");

  topic_conf = rd_kafka_topic_conf_new();  

   // rd_kafka_conf_set_dr_cb(main_conf->rkc, kafka_callback_handler);

  
	char*	group = "test2";  
	char	errstr[512];  

	if (rd_kafka_conf_set(main_conf->rkc, "group.id", group,  errstr, sizeof(errstr)) !=  RD_KAFKA_CONF_OK) {  
		fprintf(stderr, "%% %s\n", errstr);  
		return -1;  
	} else {
		fprintf(stderr, "rd_kafka_conf_set group.id ok11111\n");  
	}

	  /* Consumer groups always use broker based offset storage */  
  if (rd_kafka_topic_conf_set(topic_conf, "offset.store.method",  
                              "broker",  
                              errstr, sizeof(errstr)) !=  
      RD_KAFKA_CONF_OK) {  
          fprintf(stderr, "%% %s\n", errstr);  
          return -1;  
  }  

  rd_kafka_conf_set_default_topic_conf(main_conf->rkc, topic_conf);  

//	rd_kafka_conf_set_rebalance_cb(main_conf->rkc, rebalance_cb);
	
	
	

  // rd_kafka_conf_set(main_conf->rkc, "metadata.broker.list", g_broker_list, NULL, 0);
	main_conf->rk = rd_kafka_new(RD_KAFKA_CONSUMER, main_conf->rkc, NULL, 0);



	if (rd_kafka_brokers_add(main_conf->rk, g_broker_list) == 0){  
		fprintf(stderr, "No valid brokers specified\n");
	} else {
		fprintf(stderr, "rd_kafka_brokers_add ok 11111111111111\n");
	}

		
		rd_kafka_poll_set_consumer(main_conf->rk);  


				//创建一个Topic+Partition的存储空间(list/vector)  
	//	rd_kafka_topic_partition_list_t *topics2;
	rd_kafka_topic_partition_list_t *topics;  
		topics = rd_kafka_topic_partition_list_new(1);  
if(topics == NULL){
	fprintf(stderr, "rd_kafka_topic_partition_list_new error \n");
} else {
	fprintf(stderr, "rd_kafka_topic_partition_list_new ok \n");
}

		//把Topic+Partition加入list  
		char* topic="test2";
		rd_kafka_topic_partition_list_add(topics, topic, -1);
		
		//开启consumer订阅，匹配的topic将被添加到订阅列表中  
		rd_kafka_resp_err_t err;
		if((err = rd_kafka_subscribe(main_conf->rk, topics))){  
			fprintf(stderr, "%% Failed to start consuming topics: %s\n", rd_kafka_err2str(err));  
			return -1;  
		} else {
			fprintf(stderr, "rd_kafka_subscribe ok\n");
		}

	//rd_kafka_conf_set(main_conf->rkc, "auto.offset.reset", "RD_KAFKA_OFFSET_STORED", NULL, 0);

// char errstr[512];  
  /* Consumer groups always use broker based offset storage */  
//  if (rd_kafka_topic_conf_set(local_conf->rktc, "offset.store.method",  
//                              "broker",  
//                              errstr, sizeof(errstr)) !=  
//      RD_KAFKA_CONF_OK) {  
//          fprintf(stderr, "%% %s\n", errstr);  
//          return -1;  
//  }  

//rd_kafka_conf_set_default_topic_conf(main_conf->rkc, local_conf->rktc);  


    return 0;
}

void ngx_http_kafka_exit_worker(ngx_cycle_t *cycle) {
    ngx_http_kafka_main_conf_t  *main_conf;

    main_conf = ngx_http_cycle_get_module_main_conf(cycle, ngx_http_kafka_module);

    rd_kafka_destroy(main_conf->rk);
}

void ngx_str_helper(ngx_str_t *str, ngx_str_op op) {
    static char backup;

    switch (op) {
        case ngx_str_push:
            backup = str->data[str->len];
            str->data[str->len] = 0;
            break;
        case ngx_str_pop:
            str->data[str->len] = backup;
            break;
        default:
            ngx_abort();
    }
}
