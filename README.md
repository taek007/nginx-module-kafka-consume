# nginx-module-kafka-consume
## 通过http请求方式,消费kafka数据<br/>
### 完整的消费数据，需要调用两个接口<br/>
a) http://127.0.0.1:81/register_topic?group_name=test_group&topic_name=test&set_offset_method=largest<br/>
   设置消费组: test_group以及topic: test<br/><br/>
   返回值: {"error_code":0"error_msg":"url":"/consume?group_name=test_group""set_offset_method":"largest"}<br/>
b) http://127.0.0.1:81/consume?group_name=test_group<br/>
   开始消费<br/>
   
nginx配置
```
#user  nobody;
worker_processes  1;
error_log  logs/error.log;
pid        logs/nginx.pid;
events {
    worker_connections  1024;
}

http {
    include       mime.types;
    default_type  application/octet-stream;
    kafka.broker.list  1.2.3.4:9092 5.6.7.8:9092;

    log_format  main  '$remote_addr - $remote_user [$time_local] "$request" '
                      '$status $body_bytes_sent "$http_referer" '
                      '"$http_user_agent" "$http_x_forwarded_for"';

    access_log  logs/access.log  main;
    sendfile        on;

    keepalive_timeout  65;

    server {
        listen       8029;
        server_name  localhost;
        charset koi8-r;

        location / {
            root   html;
            index  index.html index.htm;
        }

        location = /register_topic {
             set $topic_name $arg_topic_name;
             set $group_name $arg_group_name;
             set $set_offset_method $arg_set_offset_method;
             kafka.register_topic ab;
        }

        location = /consume { 
            set $group_name $arg_group_name;
            kafka.topic  test;
        }
    }
}
```
