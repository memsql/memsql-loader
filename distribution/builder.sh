#!/bin/bash
set -e

[[ $# != 3 ]] && echo "Usage: $0 VERSION BASE_DIR UID" && exit 1

VERSION=$1
BASE=$2
MEMSQL_UID=$3

# create the memsql user
adduser --shell=/bin/bash --uid $MEMSQL_UID memsql

# install dependency
/usr/local/bin/pip2.7 install -e $BASE
$BASE/scripts/apsw_install.sh

# compile memsql_loader
su memsql -c \ "cd $BASE/distribution && pyi-build -y memsql_loader.spec"

# cleanup some stuff (this is created as root)
rm -rf /memsql_loader/memsql_loader.egg-info
