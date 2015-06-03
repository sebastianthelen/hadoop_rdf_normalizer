#!/usr/bin/env python
import sys
from rdflib.graph import Graph
from rdflib import Literal
import rdflib
from argparse import ArgumentParser
from rdflib import OWL


def formatAsUri(resource):
    if isinstance(resource, rdflib.term.Literal):
        if str(Literal(resource.datatype)) != str(None):
            return (('"%s"^^<%s>' % (resource, Literal(resource.datatype))).encode('unicode_escape').decode('ascii'))
        else:
            return ('"%s"' % resource).encode('unicode_escape').decode('ascii')
    elif isinstance(resource, rdflib.term.BNode):
        return ('_:%s' % resource).encode('unicode_escape').decode('ascii')
    else: 
        return ('<%s>' % resource).encode('unicode_escape').decode('ascii')

def read_input(stdin):
    for line in stdin:
        # split the line into words
        yield line


def map(mode='object'):
    data = read_input(sys.stdin)
    for triple in data:
        # get rid of blank lines
        triple = triple.rstrip('\n')
        # load triples into rdflib model
        # --> easier processing
        graph = Graph()
        try:
            graph.parse(data=triple, format="nt")
        except Exception as e:
            print (triple, e)
            sys.exit()
        # there is actually just one triple
        # in the graph
        for subj, pred, obj in graph:
            # <key, value> = <obj, triple>
            key = None
            if mode=='object':
                key = obj
            elif mode=='subject':
                key = subj
            if isinstance(key, rdflib.term.Literal):
                if str(Literal(key.datatype)) != str(None):
                    # typed literals
                    print(('"%s"^^<%s>' % (key, Literal(key.datatype))).encode('unicode_escape').decode('ascii'),
                          '\t',
                          ('%s' % graph.serialize(format='nt').decode('ascii').rstrip('\n')))
                else:
                    # untyped literals
                    print(('"%s"' % key).encode('unicode_escape').decode('ascii'),
                          '\t',
                          ('%s' % graph.serialize(format='nt').decode('ascii').rstrip('\n')))
            else:
                # blank nodes
                if isinstance(key, rdflib.term.BNode):
                    print(('_:%s' % key).encode('unicode_escape').decode('ascii'),
                          '\t',
                          ('%s' % graph.serialize(format='nt').decode('ascii').rstrip('\n')))
                # regular statements
                else:
                    print(('<%s>' % key).encode('unicode_escape').decode('ascii'),
                          '\t',
                          ('%s' % graph.serialize(format='nt').decode('ascii').rstrip('\n')))
                    # sameAs statements
                    if pred == OWL.sameAs and mode == 'subject':
                        tmp_graph = Graph()
                        tmp_graph.add((obj, pred, subj))
                        print(formatAsUri(obj), '\t', tmp_graph.serialize(format='nt').decode('ascii').rstrip('\n'))

if __name__ == "__main__":
    argParser = ArgumentParser(description='MapReduce RDF normalizer.')
    argParser.add_argument('-m', '--mode', help='subject or object normalization', required=True, choices=['subject', 'object'])
    #argParser.add_argument('-f', '--file', help='filename', required=True)
    args = argParser.parse_args()
    map(args.mode)
