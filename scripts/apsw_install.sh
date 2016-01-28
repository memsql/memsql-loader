#!/bin/bash

##### UPDATE memsql_loader/main.py if you change this file!

pip freeze | grep -q "apsw.git@1be8f228f8b337f33f1a88ee55f61915133dd9b4"
ret=$?
if [ "$ret" != "0" ]
then
    pip install \
        -e "git+git://github.com/rogerbinns/apsw.git@1be8f228f8b337f33f1a88ee55f61915133dd9b4#egg=apsw" \
        --global-option="fetch" --global-option="--sqlite" --global-option="--missing-checksum-ok" \
        --global-option="build" --global-option="--enable-all-extensions"
fi
