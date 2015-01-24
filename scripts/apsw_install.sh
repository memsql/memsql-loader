#!/bin/bash

##### UPDATE memsql_loader/main.py if you change this file!

pip freeze | grep -q "apsw.git@29e3c7f28a660eddade9188969fb974aef6d2ee6"
ret=$?
if [ "$ret" != "0" ]
then
    pip install \
        -e "git+git://github.com/rogerbinns/apsw.git@29e3c7f28a660eddade9188969fb974aef6d2ee6#egg=apsw" \
        --global-option="fetch" --global-option="--sqlite" --global-option="--missing-checksum-ok" \
        --global-option="build" --global-option="--enable-all-extensions"
fi
