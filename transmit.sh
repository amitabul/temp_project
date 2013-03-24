#!/bin/bash

HOME_DIR=${0%/*}
EXEC_FILE=${0##*/}

if [ $# -lt 2 ] ; then
    echo "invalid args"
    echo "Usage: $EXEC_FILE <dump_type> <service_name>"
    echo "       <dump_type> : full | incr"
    echo "       ex) $EXEC_FILE incr test_collection"
    exit 1
fi

SERVICE_NAME=$1

pushd $HOME_DIR &> /dev/null

export PYTHONPATH=$HOME_DIR
python3 ./asap/transmitter.py $SERVICE_NAME

popd &> /dev/null
