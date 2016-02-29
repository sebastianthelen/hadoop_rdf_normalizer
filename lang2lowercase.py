#!/usr/bin/env python
import sys
import re
from argparse import ArgumentParser


def read_input(stdin):
    for line in stdin:
        yield line

def map():
    data=read_input(sys.stdin)
    for line in data:
        print(re.sub(r'"(@[A-Z]{3})','"',line))


if __name__ == "__main__":
    argParser = ArgumentParser(description='')
    map()