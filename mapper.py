#!/usr/bin/env python
import sys
import logging
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
        yield line

def initLogger(name):
    global LOGGER
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch = logging.StreamHandler()
    LOGGER = logging.getLogger(name)
    ch.setLevel(logging.ERROR)
    ch.setFormatter(formatter)
    LOGGER.addHandler(ch)

def map(mode):
    data = read_input(sys.stdin)
    for quad in data:
        # get rid of blank lines
        quad = quad.rstrip('\n')
        # load triples into rdflib model
        # --> easier processing
        ds = rdflib.Dataset()
        try:
            ds.parse(data=quad, format='nquads')
                #sys.exit()
            # there is actually just one statement
            # in the graph
            for subj, pred, obj, name in ds.quads((None, None, None, None)):
                # <key, value> = <obj, statement>
                key = None
                if mode=='object':
                    key = obj
                elif mode=='subject':
                    key = subj
                if isinstance(key, rdflib.term.Literal):
                    if str(Literal(key.datatype)) != str(None):
                        # typed literals
                        print(('"%s"^^<%s>' % (key, Literal(key.datatype))).encode('unicode_escape').decode('ascii'),
                              '\t', ('%s' % ds.serialize(format='nquads').decode('ascii').rstrip('\n')))
                    else:
                        # untyped literals
                        print(('"%s"' % key).encode('unicode_escape').decode('ascii'),
                              '\t', ('%s' % ds.serialize(format='nquads').decode('ascii').rstrip('\n')))
                else:
                    # blank nodes
                    if isinstance(key, rdflib.term.BNode):
                        print(('_:%s' % key).encode('unicode_escape').decode('ascii'),
                              '\t', ('%s' % ds.serialize(format='nquads').decode('ascii').rstrip('\n')))
                    # regular statements
                    else:
                        # sameAs statements
                        if pred == OWL.sameAs and mode == 'subject':
                            tmp_graph = rdflib.Dataset()
                            tmp_graph.add((subj, pred, obj, name))
                            print(formatAsUri(obj), '\t', tmp_graph.serialize(format='nquads').decode('ascii').rstrip('\n'))
                        else:
                             print(('<%s>' % key).encode('unicode_escape').decode('ascii'),
                              '\t', ('%s' % ds.serialize(format='nquads').decode('ascii').rstrip('\n')))
        except Exception as e:
            LOGGER.exception('Error when processing quad: %s' % quad)

if __name__ == "__main__":
    argParser = ArgumentParser(description='MapReduce RDF normalizer.')
    argParser.add_argument('-m', '--mode', help='subject or object normalization',
                           required=True, choices=['subject', 'object'])
    #argParser.add_argument('-f', '--file', help='filename', required=True)
    args = argParser.parse_args()
    initLogger("mapper.py")
    map(args.mode)
