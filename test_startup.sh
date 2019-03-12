#!/bin/bash
file=$1
config=$2
num=$3
nn=$4
rep=$5
path=$6

for i in $(seq 0 $((num-1)));
do
  nice -n 19 nohup java -jar ${file} ${config} ${i} ${num} ${rep} ${nn} ${path} > output_${i} &
done
