#include <string.h>  
#include <stdlib.h>  
#include <syslog.h>  
#include <signal.h>  
#include <error.h>  
#include <getopt.h>  

#include <librdkafka/rdkafka.h>

int main() {
rd_kafka_t *rk; 
 rd_kafka_topic_conf_t *topic_conf;  
 rd_kafka_conf_t *conf;  
    
    conf = rd_kafka_conf_new();
	fprintf(stderr, "555555555555\n");

  topic_conf = rd_kafka_topic_conf_new();  

   // rd_kafka_conf_set_dr_cb(main_conf->rkc, kafka_callback_handler);

  
	char*	group = "test2";  
	char	errstr[512];  

	if (rd_kafka_conf_set(conf, "group.id", group,  errstr, sizeof(errstr)) !=  RD_KAFKA_CONF_OK) {  
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

  rd_kafka_conf_set_default_topic_conf(conf, topic_conf);  

//	rd_kafka_conf_set_rebalance_cb(main_conf->rkc, rebalance_cb);
	
	
	

  // rd_kafka_conf_set(main_conf->rkc, "metadata.broker.list", g_broker_list, NULL, 0);
	rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, NULL, 0);

 char *g_broker_list = "10.99.1.135:9092";  

	if (rd_kafka_brokers_add(rk, g_broker_list) == 0){  
		fprintf(stderr, "No valid brokers specified\n");
	} else {
		fprintf(stderr, "rd_kafka_brokers_add ok\n");
	}

		
		rd_kafka_poll_set_consumer(rk);  


				//创建一个Topic+Partition的存储空间(list/vector)  
	//	rd_kafka_topic_partition_list_t *topics2;
	rd_kafka_topic_partition_list_t *topics;  
		topics = rd_kafka_topic_partition_list_new(1);  


		//把Topic+Partition加入list  
		char* topic="test";
		rd_kafka_topic_partition_list_add(topics, topic, -1);
		
		//开启consumer订阅，匹配的topic将被添加到订阅列表中  
		rd_kafka_resp_err_t err;
		if((err = rd_kafka_subscribe(rk, topics))){  
			fprintf(stderr, "%% Failed to start consuming topics: %s\n", rd_kafka_err2str(err));  
			return -1;  
		} else {
			printf("rd_kafka_subscribe ok\n");
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


}
