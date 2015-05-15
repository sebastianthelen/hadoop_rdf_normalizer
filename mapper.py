#!/usr/bin/env python
import sys
from rdflib.graph import Graph
from io import StringIO

def read_input(file):
    for line in file:
        # split the line into words
        yield line

def main():
    #data = read_input(sys.stdin)
    for triple in sys.stdin:
        print("FOOOOO" ,triple)
        # create file like object
        flo = StringIO(triple)
        graph = Graph()
        graph.parse(triple, format="nt")
        for subject, predicate, obj in graph:
            print ('%s\t%s' % (obj, triple))

if __name__ == "__main__":
    main()