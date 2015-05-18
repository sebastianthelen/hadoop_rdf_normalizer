#!/usr/bin/env python
import sys
from rdflib.graph import Graph
from rdflib import Literal
import rdflib
from io import StringIO

# read input file in ntriple format
def read_input(file):
    for line in file:
        yield line


def map():
    ntriples = read_input(sys.stdin)
    for triple in ntriples:
        # get rid of blank lines
        triple = triple.replace('.\n', '.')
        # load triples into rdflib model
        # --> easier processing
        graph = Graph()
        graph.parse(StringIO(triple.decode('utf-8')),
                    format="nt")
        # there is actually just one triple
        # in the graph
        for subject, predicate, obj in graph:
            # <key, value> = <obj, triple>
            if isinstance(obj, rdflib.term.Literal):
                print u'"%s"^^<%s>\t%s' % (obj,
                                          Literal(obj.datatype),
                                          triple)
            else:
                print '<%s>\t%s' % (obj, triple)


if __name__ == "__main__":
    map()
