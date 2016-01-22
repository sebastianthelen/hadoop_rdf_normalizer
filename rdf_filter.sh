#!/bin/sh
source /opt/rh/python33/enable
./rdf_filter.py -m object -l -b -r "^.*(cdm#|resource/authority).*$"
