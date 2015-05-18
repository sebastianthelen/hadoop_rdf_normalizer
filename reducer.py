#!/usr/bin/env python

import sys
from rdflib import OWL
from rdflib.graph import Graph
from rdflib import Literal
import rdflib
from io import StringIO

# input for the reducer is a set of <obj, triple>
# key/value pairs ordered by obj identifiers
def reduce():
    # input comes from stdin
    previous_key = None
    graph = None
    # load a triples having same previous_key into a model
    for line in sys.stdin:
        line = line.strip()
        current_key, triple = line.split('\t', 1)
        if current_key == previous_key or graph is None:
            if graph is None:
                graph = Graph()
            # load triple triple into temp graph
            # for easier processing
            tmp_graph = Graph()
            tmp_graph.parse(StringIO(triple.decode('utf-8')),
                            format="nt")
            # get triple from temporary graph
            for sub, pred, obj in tmp_graph:
                # add to current graph
                graph.add((sub, pred, obj))
        else:
            replaceObjectUri(graph)
            graph = Graph()
            # load triple triple into temp graph
            # for easier processing
            tmp_graph = Graph()
            tmp_graph.parse(StringIO(triple.decode('utf-8')),
                            format="nt")
            # get triple from temporary graph
            for sub, pred, obj in tmp_graph:
                # add to current graph
                graph.add((sub, pred, obj))
        previous_key = current_key
    replaceObjectUri(graph)


def formatAsUri(resource):
    if isinstance(resource, rdflib.term.Literal):
        return '"%s"^^<%s>' % (resource, Literal(resource.datatype))
    else:
        return '<%s>' % resource


def replaceObjectUri(graph):
    flag = False
    for subj, pred, obj in graph:
        # determine obj's cellar id, if it exists (owl:sameAs)
        results = graph.triples((None, OWL.sameAs, obj))
        sameas_stmnt = None
        try:
            # owl:sameAs statement with cellar id as subject
            sameas_stmnt = results.next()
        except StopIteration:
            # no owl:sameAs statement found
            pass
        # nothing to replace
        if sameas_stmnt is None:
            print formatAsUri(subj), formatAsUri(pred), formatAsUri(obj)
        # replace obj by cellar_id
        else:
            sameas_subj = sameas_stmnt[0]
            # filter reflexive statements
            if str(sameas_subj) != str(subj) and str(pred) != str(OWL.sameAs):
                print formatAsUri(subj), formatAsUri(pred), formatAsUri(sameas_subj)
            # original owl:sameAs statement should be printed once
            if not(flag):
                print formatAsUri(subj), formatAsUri(OWL.sameAs), formatAsUri(obj)
                flag = True


if __name__ == "__main__":
    reduce()
