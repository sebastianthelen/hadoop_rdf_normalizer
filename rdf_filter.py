#!/usr/bin/env python
import sys
import logging
from rdflib.graph import Graph
from rdflib import Literal
import rdflib
import re
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

def map(inverted_filters, mode='object', regex=None, literal=False, bnode=False):
    data = read_input(sys.stdin)
    for quad in data:
        # get rid of blank lines
        quad = quad.rstrip('\n')
        # load triples into rdflib model
        # --> easier processing
        ds = rdflib.Dataset()
        try:
            ds.parse(data=quad, format='nquads')

            # there is actually just one statement
            # in the graph
            for subj, pred, obj, name in ds.quads((None, None, None, None)):
                # <key, value> = <obj, triple>
                key = None
                if mode=='object':
                    key = obj
                elif mode=='subject':
                    key = subj

                filter = inverted_filters and (lambda l,b,r,k:
                        ((l==True and [isinstance(k, rdflib.term.Literal)] or [True])[0] \
                        or (b==True and [isinstance(k, rdflib.term.BNode)] or [True])[0] \
                        or ((r!=None) and [re.match(r,k)] or [True])[0]))or \
                         (lambda l,b,r,k:
                        ((l==True and [not(isinstance(k, rdflib.term.Literal))] or [True])[0] \
                        and (b==True and [not(isinstance(k, rdflib.term.BNode))] or [True])[0] \
                        and ((r!=None) and [not(re.match(r,k))] or [True])[0])
                                               )
                # see http://www.diveintopython.net/power_of_introspection/and_or.html
                if (filter(literal,bnode,regex,key)):
                    # regular statement
                    print(('%s' % ds.serialize(format='nquads').decode('ascii').rstrip('\n')))
        except Exception as e:
             LOGGER.exception('Error when processing quad: %s' % quad)


if __name__ == "__main__":
    argParser = ArgumentParser(description='RDF filter.')
    argParser.add_argument('-m', '--mode', help='subject or object filtering', required=True, choices=['subject', 'object'])
    argParser.add_argument('-l', '--literal', help='filter statements with literal values in object', dest='literal', action='store_true')
    argParser.add_argument('-b', '--bnode', help='filter statements with blank as subject/object', dest='bnode', action='store_true')
    argParser.add_argument('-r', '--regex', help='regular expression for filtering statements', required=False)
    argParser.add_argument('-i', '--inverted', help='invert filter rules', dest='inverted_filters', action='store_true')
    argParser.set_defaults(literal=False, bnode=False, inverted_filters=False)
    args = argParser.parse_args()
    initLogger("rdf_filter.py")
    map(args.inverted_filters, args.mode, args.regex, args.literal, args.bnode)