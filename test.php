<?php
$url = "http://10.99.1.148:8029/consume?group_name=test_group";
while(1){
	$res = file_get_contents($url);
	echo $res."\n";
}
