#!/usr/bin/env python
import sys
import re
from argparse import ArgumentParser


def read_input(stdin):
    for line in stdin:
        yield line

def map(undo):
    data=read_input(sys.stdin)
    for line in data:
        if(undo):
            print(re.sub(r'<http://genid#([A-Za-z0-9]*)>',r'_:\1',line).rstrip('\n'))
        else:
            print(re.sub(r'_:([A-Za-z0-9]*)(?=([^"]*"[^"]*")*[^"]*$)(?=([^>]*<[^>]*>)*[^>]*$)',r'<http://genid#\1>',line).rstrip('\n'))


if __name__ == "__main__":
    argParser = ArgumentParser(description='Blank node skolemization.')
    argParser.add_argument('-u', '--undo', help='undo skolemization', dest='undo', action='store_true')
    argParser.set_defaults(undo=False)
    args = argParser.parse_args()
    map(args.undo)