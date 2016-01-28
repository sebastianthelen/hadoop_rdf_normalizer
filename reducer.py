#!/usr/bin/env python

import sys
import logging
import rdflib
from rdflib import OWL
from argparse import ArgumentParser


def initLogger(name):
    global LOGGER
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch = logging.StreamHandler()
    LOGGER = logging.getLogger(name)
    ch.setLevel(logging.ERROR)
    ch.setFormatter(formatter)
    LOGGER.addHandler(ch)

# input for the reducer is a set of ordered
# key/value pairs 
def reduce(mode='object'):
    # input comes from stdin
    previous_key = None
    ds = rdflib.Dataset()
    # load a quad with same _key into a model
    for line in sys.stdin:
        line = line.strip()
        try:
            current_key, quad = line.split('\t', 1)

            # all lines having same key have
            # been completely processed
            if current_key != previous_key and previous_key != None:
                if mode == 'object':
                    replaceObjectUri(ds)
                elif mode == 'subject':
                    replaceSubjectUri(ds)
                # clear graph
                ds = rdflib.Dataset()
            # load quad quad into temp graph
            # for easier processing
            tmp_graph = rdflib.Dataset()
            try:
                tmp_graph.parse(data=quad,
                            format="nquads")
            except Exception as e:
                print(quad, e)
                sys.exit()
            # get quads from temporary graph
            for sub, pred, obj, name in tmp_graph.quads(
                    (None, None, None, None)):
                # add to current graph
                ds.add((sub, pred, obj, name))
            previous_key = current_key
        except Exception as e:
            LOGGER.exception('Error when processing input line: %s' % line)
            sys.exit()
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
        results = ds.quads((None, OWL.sameAs, subj, None))
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
        # replace subj by cellar_id
        else:
            cellar_id = sameas_stmnt[0]
            tmp_graph.add((cellar_id, pred, obj, name))
        print(tmp_graph.serialize(format='nquads').decode('ascii').rstrip('\n'))


if __name__ == "__main__": 
    argParser = ArgumentParser(description='MapReduce RDF normalizer.')
    argParser.add_argument('-m', '--mode', help='subject or object normalization', required=True, choices=['subject', 'object'])
    args = argParser.parse_args()
    initLogger("reducer.py")
    reduce(args.mode)
