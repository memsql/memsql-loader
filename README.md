=============
MemSQL Loader
=============

MemSQL Loader is a tool that lets you load sets of files from Amazon S3, the Hadoop Distributed
File System (HDFS), and the local filesystem into MemSQL (or MySQL) with just one command. You can
specify all of the files you want to load with one command, and MemSQL Loader will take care of
deduplicating files, parallelizing the workload, retrying files if they fail to load, and more.

Background
==========

One of the most common tasks with any database is loading large amounts of data into
it from an external data store. Both MemSQL and MySQL provide the LOAD DATA command
for this task; this command is very powerful, but by itself, it has a number of restrictions:

 * It can only read from the local filesystem, so loading data from a remote store like
   Amazon S3 requires first downloading the files you need.
 * Since it can only read from a single file at a time, if you want to load from multiple
   files, you need to issue multiple LOAD DATA commands. If you want to perform this work
   in parallel, you have to write your own scripts.
 * If you are loading multiple files, it’s up to you to make sure that you’ve deduplicated
   the files and their contents.

At MemSQL, we’ve acutely felt all of these limitations. That’s why we developed MemSQL Loader,
which solves all of the above problems and more.

Basic Usage
===========

Downloading the Loader
----------------------

The loader is a standalone binary that you can download and run directly. We keep the latest
version hosted at https://github.com/memsql/memsql-loader/releases. The binary is produced by compiling
this python project with PyInstaller.

You can download this repo and run the loader directly. If you do so, you will need to
install virtualenv, libcurl, and libncurses. Once you've downloaded the repo,
`cd` into its directory and run

    $ source activate

You should see the prefix `(venv)` in your shell. You can run the loader with

    (venv) $ ./bin/memsql-loader --help

Running the Loader
------------------

The primary interface to the loader is the `memsql-loader load` command. The command takes arguments
that specify the source, parsing options, and destination server. For example, to load some files
from S3, you can run

    $ ./memsql-loader load -h 127.0.0.1 -u root --database db --table t \
        s3://memsql-loader-examples/sanity/*

The loader automatically daemonizes and runs the load process in a background server. You can monitor its
progress with

    $ ./memsql-loader ps --watch

If you would like to run this example against MemSQL or MySQL, run

    memsql> CREATE DATABASE db;
    memsql> CREATE TABLE    db.t (a int, b int, primary key (a));

File Pattern Syntax
-------------------

The loader supports loading files from Amazon S3, HDFS, and the local filesystem. The file's prefix
determines the source. You can specify "s3://", "hdfs://", or "file://". If you omit the prefix,
then the loader defaults to the local filesystem.

The loader also supports glob syntax (with semantics similar to bash). A single `*` matches files
in the current directory, and `**` matches files recursively. MemSQL Loader uses the glob2 library
under the hood to facilitate this.

File Parsing Options
--------------------

MemSQL Loader's command line options mirror the LOAD DATA command's syntax. See the `load data options`
section in `./memsql-loader load --help` for reference.

Automatic Deduplication
-----------------------

MemSQL Loader is designed to support one-time loads as well as synchronizing behavior. You can use this
functionality to effectively sync a table's data to the set of files matching a path. The loader will automatically
deduplicate files that it knows it does not need to load (by using the MD5 feature on S3), and transactionally
delete and reload data when the contents of a file have changed.

NOTE: This reload behavior requires specifying a column to use as a `file_id`.

Spec Files
----------

We found with usage that it was really convenient to be able to define a load job as a JSON file, instead of
just command line options. MemSQL Loader lets you use "spec files" to accomplish this. To generate one, just
append `--print-spec` to the `./memsql-loader load` command. It will generate a spec file that you can
use with `--spec`. Any command line options that you provide along with `--spec` will override options
in the spec file.

Scripts
-------

You can also pipe files through a script before running LOAD DATA with the `--script` flag.

    $ ./bin/memsql-loader load --database test --table test --delimiter ',' test.csv.lzma --script "lzma -d"

TODO
====

* We have a pretty big test suite for the loader, but it's tied closely to MemSQL's internal testing
  infrastructure. We're going to separate these tests out and add them to this repo.
* Right now the loader supports MemSQL and MySQL (via the LOAD DATA command), but does not support
  other database systems. We would love for members of the community to add support for more systems.
* Error reporting and job management is fairly undeveloped in the loader. We'll integrate this further
  into our MemSQL Ops platform over time, but it would be great to see some iteration on this here as well.

Third-party code
================
MemSQL Loader includes a fork of the python-glob2 project (https://github.com/miracle2k/python-glob2/).
The code for this fork can be found in [memsql_loader/vendor/glob2](memsql_loader/vendor/glob2).

