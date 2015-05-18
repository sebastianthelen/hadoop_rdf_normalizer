#!/usr/bin/env python

from itertools import groupby
from operator import itemgetter
import sys
from rdflib.term import URIRef
from rdflib.graph import Graph
from io import StringIO

def reduce():
    # input comes from STDIN
    key = None
    graph = None
    for line in sys.stdin:
        # remove leading and trailing whitespace
        line = line.strip()
        # parse the input we got from mapper.py
        print(line.split('\t',1))
        current_key, value = line.split('\t',1)
        if(current_key == key):
            # create file like object
            print 'xxxx'
            flo = StringIO(value)
            # load flo into temporary graph
            tmp_graph = Graph()
            tmp_graph.parse(flo)
            # get triple from temporary graph
            for subject, predicate, obj in tmp_graph:
                # add to current graph
                graph.add(subject,predicate,object)
        if(graph == None):
            graph = Graph()
            key = current_key

        else:
            replace_object_uri(graph)
            key = current_key
            graph = Graph()



def replace_object_uri(graph):
    for subject, predicate, object in graph:
       res = graph.triples(None,OWL.sameAs,object)
       if (len(res)>0):
           print(subject,predicate,res)

if __name__ == "__main__":
    reduce()