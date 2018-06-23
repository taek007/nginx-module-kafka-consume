#include <string.h>  
#include <stdlib.h>  
#include <syslog.h>  
#include <signal.h>  
#include <error.h>  
#include <getopt.h>  
#include <librdkafka/rdkafka.h>
  
  /*
gcc 2.c -o 2  -lrdkafka -lz -lpthread -lrt
*/
static int run = 1;  
//`rd_kafka_t`自带一个可选的配置API，如果没有调用API，Librdkafka将会使用CONFIGURATION.md中的默认配置。  
static rd_kafka_t *rk;  
static rd_kafka_topic_partition_list_t *topics;  
  
static void stop (int sig) {  
  if (!run)  
    exit(1);  
  run = 0;  
  fclose(stdin); /* abort fgets() */  
}  
  
static void sig_usr1 (int sig) {  
  rd_kafka_dump(stdout, rk);  
}  
  
/** 
 * 处理并打印已消费的消息 
 */  
static void msg_consume (rd_kafka_message_t *rkmessage,  
       void *opaque) {  
  if (rkmessage->err) {  
    if (rkmessage->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {  
      fprintf(stderr,  
        "%% Consumer reached end of %s [%"PRId32"] "  
             "message queue at offset %"PRId64"\n",  
             rd_kafka_topic_name(rkmessage->rkt),  
             rkmessage->partition, rkmessage->offset);  
  
      return;  
    }  
  
    if (rkmessage->rkt)  
            fprintf(stderr, "%% Consume error for "  
                    "topic \"%s\" [%"PRId32"] "  
                    "offset %"PRId64": %s\n",  
                    rd_kafka_topic_name(rkmessage->rkt),  
                    rkmessage->partition,  
                    rkmessage->offset,  
                    rd_kafka_message_errstr(rkmessage));  
    else  
            fprintf(stderr, "%% Consumer error: %s: %s\n",  
                    rd_kafka_err2str(rkmessage->err),  
                    rd_kafka_message_errstr(rkmessage));  
  
    if (rkmessage->err == RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION ||  
        rkmessage->err == RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC)  
          run = 0;  
    return;  
  }  
  
  fprintf(stdout, "%% Message (topic %s [%"PRId32"], "  
                      "offset %"PRId64", %zd bytes):\n",  
                      rd_kafka_topic_name(rkmessage->rkt),  
                      rkmessage->partition,  
    rkmessage->offset, rkmessage->len);  
  
  if (rkmessage->key_len) {  
    printf("Key: %.*s\n",  
             (int)rkmessage->key_len, (char *)rkmessage->key);  
  }  
  
  printf("%.*s\n",  
           (int)rkmessage->len, (char *)rkmessage->payload);  
    
}  
  
/* 
  init all configuration of kafka 
 */  
int initKafka(char *brokers, char *group,char *topic){  
  rd_kafka_conf_t *conf;  
  rd_kafka_topic_conf_t *topic_conf;  
  rd_kafka_resp_err_t err;  
  char tmp[16];  
  char errstr[512];  
  
  /* Kafka configuration */  
  conf = rd_kafka_conf_new();  
  
  //quick termination  
  snprintf(tmp, sizeof(tmp), "%i", SIGIO);  
  rd_kafka_conf_set(conf, "internal.termination.signal", tmp, NULL, 0);  
  
  //topic configuration  
  topic_conf = rd_kafka_topic_conf_new();  
  
  /* Consumer groups require a group id */  
  if (!group)  
          group = "rdkafka_consumer_example";  
  if (rd_kafka_conf_set(conf, "group.id", group,  
                        errstr, sizeof(errstr)) !=  
      RD_KAFKA_CONF_OK) {  
          fprintf(stderr, "%% %s\n", errstr);  
          return -1;  
  }  
  
  /* Consumer groups always use broker based offset storage */  
  if (rd_kafka_topic_conf_set(topic_conf, "offset.store.method",  
                              "broker",  
                              errstr, sizeof(errstr)) !=  
      RD_KAFKA_CONF_OK) {  
          fprintf(stderr, "%% %s\n", errstr);  
          return -1;  
  }  
  
  /* Set default topic config for pattern-matched topics. */  
  rd_kafka_conf_set_default_topic_conf(conf, topic_conf);  
  
  //实例化一个顶级对象rd_kafka_t作为基础容器，提供全局配置和共享状态  
  rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));  
  if(!rk){  
    fprintf(stderr, "%% Failed to create new consumer:%s\n", errstr);  
    return -1;  
  }  
  
  //Librdkafka需要至少一个brokers的初始化list  
  if (rd_kafka_brokers_add(rk, brokers) == 0){  
    fprintf(stderr, "%% No valid brokers specified\n");  
    return -1;  
  }  
  
  //重定向 rd_kafka_poll()队列到consumer_poll()队列  
  rd_kafka_poll_set_consumer(rk);  
  
  //创建一个Topic+Partition的存储空间(list/vector)  
  topics = rd_kafka_topic_partition_list_new(1);  
  //把Topic+Partition加入list  
  rd_kafka_topic_partition_list_add(topics, topic, -1);  
  //开启consumer订阅，匹配的topic将被添加到订阅列表中  
  if((err = rd_kafka_subscribe(rk, topics))){  
      fprintf(stderr, "%% Failed to start consuming topics: %s\n", rd_kafka_err2str(err));  
      return -1;  
  }  
  
  return 1;  
}  
  
int main(int argc, char **argv){  
  char *brokers = "localhost:9092";  
  char *group = NULL;  
  char *topic = NULL;  
    
  int opt;  
  rd_kafka_resp_err_t err;  
  
  while ((opt = getopt(argc, argv, "g:b:t:qd:eX:As:DO")) != -1){  
    switch (opt) {  
      case 'b':  
        brokers = optarg;  
        break;  
      case 'g':  
        group = optarg;  
        break;  
      case 't':  
        topic = optarg;  
        break;  
      default:  
        break;  
    }  
  }   
  
  signal(SIGINT, stop);  
  signal(SIGUSR1, sig_usr1);  
  
  if(!initKafka(brokers, group, topic)){  
    fprintf(stderr, "kafka server initialize error\n");  
  }else{  
    while(run){  
      rd_kafka_message_t *rkmessage;  
      /*-轮询消费者的消息或事件，最多阻塞timeout_ms 
        -应用程序应该定期调用consumer_poll()，即使没有预期的消息，以服务 
        所有排队等待的回调函数，当注册过rebalance_cb，该操作尤为重要， 
        因为它需要被正确地调用和处理以同步内部消费者状态 */  
      rkmessage = rd_kafka_consumer_poll(rk, 1000);  
      if(rkmessage){  
        msg_consume(rkmessage, NULL);  
        /*释放rkmessage的资源，并把所有权还给rdkafka*/  
        rd_kafka_message_destroy(rkmessage);  
      }  
    }  
  }  
  
done:  
    /*此调用将会阻塞，直到consumer撤销其分配，调用rebalance_cb(如果已设置)， 
    commit offset到broker,并离开consumer group 
    最大阻塞时间被设置为session.timeout.ms 
    */  
    err = rd_kafka_consumer_close(rk);  
    if(err){  
      fprintf(stderr, "%% Failed to close consumer: %s\n", rd_kafka_err2str(err));  
    }else{  
      fprintf(stderr, "%% Consumer closed\n");  
    }  
  
    //释放topics list使用的所有资源和它自己  
    rd_kafka_topic_partition_list_destroy(topics);  
  
    //destroy kafka handle  
    rd_kafka_destroy(rk);  
    
    run = 5;  
    //等待所有rd_kafka_t对象销毁，所有kafka对象被销毁，返回0，超时返回-1  
    while(run-- > 0 && rd_kafka_wait_destroyed(1000) == -1){  
      printf("Waiting for librdkafka to decommission\n");  
    }  
    if(run <= 0){  
      //dump rdkafka内部状态到stdout流  
      rd_kafka_dump(stdout, rk);  
    }  
  
    return 0;  
} 