#!/usr/bin/env python

import sys
from rdflib import OWL
from rdflib.graph import Graph
from rdflib import Literal
import rdflib
from io import StringIO
from argparse import ArgumentParser

# input for the reducer is a set of <obj, triple>
# key/value pairs ordered by obj identifiers
def reduce(mode='object'):
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
            tmp_graph.parse(data=triple,
                            format="nt")
            # get triple from temporary graph
            for sub, pred, obj in tmp_graph:
                # add to current graph
                graph.add((sub, pred, obj))
        else:
            if mode == 'object':
                replaceObjectUri(graph)
            elif mode == 'subject':
                replaceSubjectUri(graph)
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
        replaceObjectUri(graph)
    elif mode == 'subject':
        replaceSubjectUri(graph)


def formatAsUri(resource):
    if isinstance(resource, rdflib.term.Literal):
        if str(Literal(resource.datatype)) != str(None):
            return '"%s"^^<%s>' % (resource, Literal(resource.datatype))
        else:
            return '"%s"' % resource
    elif isinstance(resource, rdflib.term.BNode):
        return '_:%s' % resource
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
            sameas_stmnt = next(results)
        except StopIteration:
            # no owl:sameAs statement found
            pass
        # nothing to replace
        if sameas_stmnt is None:
            print(formatAsUri(subj), formatAsUri(pred), repr(formatAsUri(obj)).strip("'"), '.')
        # replace obj by cellar_id
        else:
            cellar_id = sameas_stmnt[0]
            # filter reflexive statements
            if str(cellar_id) != str(subj) and str(pred) != str(OWL.sameAs):
                print(formatAsUri(subj), formatAsUri(pred), formatAsUri(cellar_id), '.')
            # original owl:sameAs statement should be printed once
            if not(flag):
                print(formatAsUri(subj), formatAsUri(OWL.sameAs), formatAsUri(obj), '.')
                flag = True


def replaceSubjectUri(graph):
    for subj, pred, obj in graph:
        # determine subj's cellar id, if it exists (owl:sameAs)
        results = graph.triples((None, OWL.sameAs, subj))
        sameas_stmnt = None
        try:
            # owl:sameAs statement with cellar id as subject
            sameas_stmnt = next(results)
        except StopIteration:
            # no owl:sameAs statement found
            pass
        # nothing to replace
        if sameas_stmnt is None:
            print(formatAsUri(subj), formatAsUri(pred), repr(formatAsUri(obj)).strip("'"), '.')
        # replace obj by cellar_id
        else:
            cellar_id = sameas_stmnt[0]
            print(formatAsUri(cellar_id), formatAsUri(pred), formatAsUri(obj), '.')


if __name__ == "__main__":
    import sys
    import codecs
    sys.stdout = codecs.getwriter('utf8')(sys.stdout)
    sys.stderr = codecs.getwriter('utf8')(sys.stderr)
    argParser = ArgumentParser(description='MapReduce RDF normalizer.')
    argParser.add_argument('-m', '--mode', help='subject or object normalization', required=True, choices=['subject', 'object'])
    args = argParser.parse_args()
    reduce(args.mode)
