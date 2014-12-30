#!/bin/bash

pip freeze | grep -q "apsw.git@91418ad337da1081a6c476bfe698c141faf752ce"
ret=$?
if [ "$ret" != "0" ]
then
    pip install \
        -e "git+git://github.com/rogerbinns/apsw.git@91418ad337da1081a6c476bfe698c141faf752ce#egg=apsw" \
        --global-option="fetch" --global-option="--sqlite" --global-option="--missing-checksum-ok" \
        --global-option="build" --global-option="--enable-all-extensions"
fi
