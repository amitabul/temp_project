#!/bin/bash

HOME_DIR=${0%/*}

pushd $HOME_DIR &> /dev/null

PYTHONPATH=$HOME_DIR

for unittest in `ls ./asap/tests/*.py`; do
    # TODO: __init__.py 제외
    echo $unittest
    python3 $unittest
done

popd &> /dev/null




