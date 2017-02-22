#!/bin/bash

##### UPDATE memsql_loader/main.py if you change this file!

pip freeze | grep -q "apsw.git@26b61c39a98db3ccad7f852adc944b0b8e94c242"
ret=$?
if [ "$ret" != "0" ]
then
    pip install \
        -e "git+git://github.com/rogerbinns/apsw.git@26b61c39a98db3ccad7f852adc944b0b8e94c242#egg=apsw" \
        --global-option="fetch" --global-option="--sqlite" --global-option="--missing-checksum-ok" \
        --global-option="build" --global-option="--enable-all-extensions"
fi
