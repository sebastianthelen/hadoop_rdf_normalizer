#!/usr/bin/env python
import sys
import re
from argparse import ArgumentParser


def read_input(stdin):
    for line in stdin:
        yield line

def map(regex, invert):
    data=read_input(sys.stdin)
    for line in data:
        if(not(invert) and not(re.match(regex,line))):
            print(line.rstrip('\n'))
        if (invert and re.match(regex,line)):
            print(line.rstrip('\n'))


if __name__ == "__main__":
    argParser = ArgumentParser(description='Removes lines from a text file (provided via stdin) based on a given regular expression')
    argParser.add_argument('-r', '--regex', help='regular expression for filtering lines', required=True)
    argParser.add_argument('-i', '--inverted', help='invert filter', dest='invert_filter', action='store_true')
    argParser.set_defaults(invert_filter=False)
    args = argParser.parse_args()
    map(args.regex, args.invert_filter)