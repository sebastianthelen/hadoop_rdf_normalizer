#!/usr/bin/env python

import sys
import rdflib
from rdflib import OWL
from argparse import ArgumentParser

# input for the reducer is a set of ordered
# key/value pairs 
def reduce(mode='object'):
    # input comes from stdin
    previous_key = None
    ds = rdflib.Dataset()
    # load a quad with same _key into a model
    for line in sys.stdin:
        line = line.strip()
        current_key, triple = line.split('\t', 1)
        # all lines having same key have
        # been completely processed
        if current_key != previous_key and previous_key != None:
            if mode == 'object':
                replaceObjectUri(ds)
            elif mode == 'subject':
                replaceSubjectUri(ds)
            # clear graph
            ds = rdflib.Dataset()
        # load quad triple into temp graph
        # for easier processing
        tmp_graph = rdflib.Dataset()
        tmp_graph.parse(data=triple,
                        format="nquads")
        # get quads from temporary graph
        for sub, pred, obj, name in tmp_graph.quads(
                (None, None, None, None)):
            # add to current graph
            ds.add((sub, pred, obj, name))
        previous_key = current_key
    if mode == 'object':
        replaceObjectUri(ds)
    elif mode == 'subject':
        replaceSubjectUri(ds)


def replaceObjectUri(ds):
    for subj, pred, obj, name in ds.quads((None, None, None, None)):
        tmp_ds = rdflib.Dataset()
        # determine obj's cellar id, if it exists (owl:sameAs)
        results = ds.quads((None, OWL.sameAs, obj, None))
        sameas_stmnt = None
        try:
            # owl:sameAs statement with cellar id as subject
            sameas_stmnt = next(results)
        except StopIteration:
            # no owl:sameAs statement found
            pass
        # nothing to replace
        if sameas_stmnt is None:
            tmp_ds.add((subj, pred, obj, name))
        # replace obj by cellar_id
        else:
            cellar_id = sameas_stmnt[0]
            # filter reflexive statements
            if str(cellar_id) != str(subj) and str(pred) != str(OWL.sameAs):
                tmp_ds.add((subj, pred, cellar_id, name))
            # original owl:sameAs statement should be printed once
            else:
                tmp_ds.add((subj, pred, obj, name))
        print(tmp_ds.serialize(format='nquads').decode('ascii').rstrip('\n'))

def replaceSubjectUri(ds):
    for subj, pred, obj, name in ds.quads((None, None, None, None)):
        tmp_graph = rdflib.Dataset()
        # determine subj's cellar id, if it exists (owl:sameAs)
        results = ds.quads((subj, OWL.sameAs, None, None))
        sameas_stmnt = None
        try:
            # owl:sameAs statement with cellar id as subject
            sameas_stmnt = next(results)
        except StopIteration:
            # no owl:sameAs statement found
            pass
        # nothing to replace
        if sameas_stmnt is None or pred == OWL.sameAs or '/cellar/' in str(subj):
            tmp_graph.add((subj, pred, obj, name))
        # replace obj by cellar_id
        else:
            cellar_id = sameas_stmnt[2]
            tmp_graph.add((cellar_id, pred, obj, name))
        print(tmp_graph.serialize(format='nquads').decode('ascii').rstrip('\n'))


if __name__ == "__main__": 
    argParser = ArgumentParser(description='MapReduce RDF normalizer.')
    argParser.add_argument('-m', '--mode', help='subject or object normalization', required=True, choices=['subject', 'object'])
    args = argParser.parse_args()
    reduce(args.mode)
