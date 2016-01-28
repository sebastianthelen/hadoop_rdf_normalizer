#!/bin/sh
source /opt/rh/python33/enable
./filter.py -r '_:([A-Za-z0-9]*)(?=([^"]*"[^"]*")*[^"]*$)(?=([^>]*<[^>]*>)*[^>]*$)'
