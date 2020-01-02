#!/bin/bash

usage="USAGE: $0 [hdfs|ftp|samba|oracle] [--help | arg1 arg2 ...]"
if [ $# -lt 1 ];
then
  echo $usage
  exit 1
fi