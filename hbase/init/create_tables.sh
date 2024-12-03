#!/bin/bash
sleep 20  # Give HBase time to initialize
echo "create 'real_stream', 'price', 'time'" | hbase shell
echo "create 'pred_stream', 'price', 'time'" | hbase shell
