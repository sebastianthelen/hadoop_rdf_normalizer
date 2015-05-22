#!/usr/bin/env python
import sys
from rdflib.graph import Graph
from rdflib import Literal
import rdflib
from argparse import ArgumentParser

def map(filename, mode='object'):
    with open(filename, encoding='utf-8') as file:
        for count, triple in enumerate(file):
            #print('sss', count, triple)
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
                        print(repr('"%s"^^<%s>' % (key, Literal(key.datatype))).strip("'"),
                              '\t',
                              repr('%s' % triple).strip("'"))
                    else:
                        # untyped literals
                        print(repr('"%s"' % key).strip("'"),
                              '\t',
                              repr('%s' % triple).strip("'"))
                else:
                    if isinstance(key, rdflib.term.BNode):
                        print(repr('_:%s' % key).strip("'"),
                              '\t',
                              repr('%s' % triple).strip("'"))
                    else:
                        print(repr('<%s>' % key).strip("'"),
                              '\t',
                              repr('%s' % triple).strip("'"))

if __name__ == "__main__":
    import sys
    import codecs
    sys.stdout = codecs.getwriter('utf8')(sys.stdout)
    sys.stderr = codecs.getwriter('utf8')(sys.stderr)
    argParser = ArgumentParser(description='MapReduce RDF normalizer.')
    argParser.add_argument('-m', '--mode', help='subject or object normalization', required=True, choices=['subject', 'object'])
    argParser.add_argument('-f', '--file', help='filename', required=True)
    args = argParser.parse_args()
    map(args.file, args.mode)
