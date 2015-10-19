#!/usr/bin/env python

# ADD QUAD SUPPORT

import sys
from rdflib import OWL
from rdflib.graph import Graph
from rdflib import Literal
import rdflib
from io import StringIO
from argparse import ArgumentParser

# input for the reducer is a set of ordered
# key/value pairs 
def reduce(mode='object'):
    # input comes from stdin
    previous_key = None
    graph = Graph()
    # load a triples with same _key into a model
    for line in sys.stdin:
        line = line.strip()
        current_key, triple = line.split('\t', 1)
        # all lines having same key have
        # been completely processed
        if current_key != previous_key and previous_key != None:
            if mode == 'object':
                #print('gggg', graph.serialize(format='nt'))
                replaceObjectUri(graph)
            elif mode == 'subject':
                replaceSubjectUri(graph)
            # clear graph
            graph = Graph()
        # load triple triple into temp graph
        # for easier processing
        tmp_graph = Graph()
        tmp_graph.parse(data=triple,
                        format="nt")
        # get triple from temporary graph
        for sub, pred, obj in tmp_graph:
            # add to current graph
            graph.add((sub, pred, obj))
        previous_key = current_key
    if mode == 'object':
        #print('gggg', graph)
        replaceObjectUri(graph)
    elif mode == 'subject':
        replaceSubjectUri(graph)


def replaceObjectUri(graph):
    for subj, pred, obj in graph:
        tmp_graph = Graph()
        # determine obj's cellar id, if it exists (owl:sameAs)
        results = graph.triples((None, OWL.sameAs, obj))
        sameas_stmnt = None
        try:
            # owl:sameAs statement with cellar id as subject
            sameas_stmnt = next(results)
        except StopIteration:
            # no owl:sameAs statement found
            pass
        # nothing to replace
        if sameas_stmnt is None:
            tmp_graph.add((subj, pred, obj))
        # replace obj by cellar_id
        else:
            cellar_id = sameas_stmnt[0]
            # filter reflexive statements
            if str(cellar_id) != str(subj) and str(pred) != str(OWL.sameAs):
                tmp_graph.add((subj, pred, cellar_id))
            # original owl:sameAs statement should be printed once
            else:
                tmp_graph.add((subj, pred, obj))
        print(tmp_graph.serialize(format='nt').decode('ascii').rstrip('\n'))

def replaceSubjectUri(graph):
    for subj, pred, obj in graph:
        tmp_graph = Graph()
        # determine subj's cellar id, if it exists (owl:sameAs)
        results = graph.triples((subj, OWL.sameAs, None))
        sameas_stmnt = None
        try:
            # owl:sameAs statement with cellar id as subject
            sameas_stmnt = next(results)
        except StopIteration:
            # no owl:sameAs statement found
            pass
        # nothing to replace
        if sameas_stmnt is None or pred == OWL.sameAs or '/cellar/' in str(subj):
            tmp_graph.add((subj, pred, obj))
        # replace obj by cellar_id
        else:
            cellar_id = sameas_stmnt[2]
            tmp_graph.add((cellar_id, pred, obj))
        print(tmp_graph.serialize(format='nt').decode('ascii').rstrip('\n'))


if __name__ == "__main__": 
    argParser = ArgumentParser(description='MapReduce RDF normalizer.')
    argParser.add_argument('-m', '--mode', help='subject or object normalization', required=True, choices=['subject', 'object'])
    args = argParser.parse_args()
    reduce(args.mode)
