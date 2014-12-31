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

############################
# TAR.GZ

DISTPATH=$BASE/distribution/dist

mv $DISTPATH/memsql-loader $DISTPATH/memsql-loader.bin

mkdir $DISTPATH/memsql-loader
cp $DISTPATH/memsql-loader.bin $DISTPATH/memsql-loader/memsql-loader

chown -R memsql:memsql $DISTPATH

# change dir to tar build dir
cd $DISTPATH
tar czf memsql-loader.tar.gz memsql-loader
chown memsql:memsql memsql-loader.tar.gz

rm -r $DISTPATH/memsql-loader
mv $DISTPATH/memsql-loader.bin $DISTPATH/memsql-loader
