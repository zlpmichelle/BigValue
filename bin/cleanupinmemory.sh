#!/bin/sh

HBASE_TABLE_NAME=FAKE_TABLE

echo "disable '$HBASE_TABLE_NAME'" | hbase shell
echo "drop '$HBASE_TABLE_NAME'" | hbase shell
