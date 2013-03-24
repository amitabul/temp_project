#!/bin/bash

HOME_DIR=${0%/*}

if [ $# -lt 1 ] ; then
    echo "invalid args"
    echo "Usage: ${0##*/} <servicename>"
    exit 1
fi

SERVICE_NAME=$1

pushd $PYTHONPATH &> /dev/null

export PYTHONPATH=$HOME_DIR
python3 ./asap/transmitter.py $SERVICE_NAME

popd &> /dev/null



