#!/bin/bash

HOME_DIR=${0%/*}

pushd $HOME_DIR &> /dev/null

export PYTHONPATH=$HOME_DIR

for unittest in `ls ./asap/tests/*.py`; do
    # __init__.py 파일 제외
    if [ ${unittest##*/} == "__init__.py" ] ; then
        continue
    fi

    echo $unittest
    python3 $unittest
done

popd &> /dev/null




