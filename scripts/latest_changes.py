#!/usr/bin/env python
import os, sys
ROOT_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..')

with open(os.path.join(ROOT_PATH, "CHANGELOG"), 'r') as changelog:
    # first line is always latest date and version
    sys.stdout.write(changelog.readline())
    # print the new line
    sys.stdout.write(changelog.readline())
    while True:
        line = changelog.readline()
        if not line.startswith("\t"):
            break
        sys.stdout.write(line.strip())
