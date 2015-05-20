#!/usr/bin/env python
import sys
from rdflib.graph import Graph
from rdflib import Literal
import rdflib
from io import StringIO

# read input in ntriple format
def read_input(input):
    for line in input:
        yield line

def map():
    ntriples = read_input(sys.stdin)
    for triple in ntriples:
        # get rid of blank lines
        triple = triple.rstrip('\n')
        # load triples into rdflib model
        # --> easier processing
        graph = Graph()
        graph.parse(data=triple, format="nt")
        # there is actually just one triple
        # in the graph
        for subject, predicate, obj in graph:
            # <key, value> = <obj, triple>
            if isinstance(obj, rdflib.term.Literal):
                if str(Literal(obj.datatype)) != str(None):
                    # typed literals
                    print('"%s"^^<%s>\t%s' % (repr(obj).strip("'"),
                                              repr(Literal(obj.datatype)).strip("'"),
                                              repr(triple).strip("'")))
                else:
                    # untyped literals
                    print('"%s"\t%s' % (repr(obj).strip("'"),
                                        repr(triple).strip("'")))
            else:
                print('<%s>\t%s' % (repr(obj).strip("'"), repr(triple).strip("'")))


if __name__ == "__main__":
    map()
