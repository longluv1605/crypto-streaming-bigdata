#!/bin/bash
# sleep 30  # Give HBase time to initialize
echo "create 'real_stream', 'cf'" | hbase shell
echo "create 'pred_stream', 'cf'" | hbase shell
