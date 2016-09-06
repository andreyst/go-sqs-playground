<?php

$stdin = file_get_contents("php://stdin");
sleep(mt_rand(3, 5));
echo $stdin . PHP_EOL;
