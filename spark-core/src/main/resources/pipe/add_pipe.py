#!/usr/bin/python

import sys
from builtins import int

for line in sys.stdin:
    num = int(line)
    print(num * 100)