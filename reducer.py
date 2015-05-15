#!/usr/bin/env python

from itertools import groupby
from operator import itemgetter
import sys
from rdflib.graph import Graph
from io import StringIO

def reduce():
    # input comes from STDIN
    for line in sys.stdin:
        # remove leading and trailing whitespace
        line = line.strip()
        # parse the input we got from mapper.py
        key, value = line.split('\t', 1)
        triple = value
        # create file like object
        flo = StringIO(triple)
        # convert into ntriples
        graph = Graph()
        graph.parse(flo)
        for subject, predicate, obj in graph:
            if
