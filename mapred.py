#!/usr/bin/env pypy
# -*- coding: utf-8 -*-
'''
Created on 2013/09/03
 
@author: Shiqiao Du
 
run
cat <input> | ./mapred.py -m mapper | sort | ./mapred.py -m reducer
'''

import sys
from itertools import groupby
from operator import itemgetter

class _BaseMapper():
    
    def __init__(self, separator="\t", *args, **params):
        self.separator = separator
        
    def map(self, k, v):
        yield k, v
     
    def run(self):
        for line in sys.stdin:
            try:
                for k, v in self.map("", line):
                    print self.separator.join((k, v))
            except Exception, e:
                sys.stderr.write(str(e) + ", cannot parse " + line)
                

class _BaseReducer():
    
    def __init__(self, separator="\t", *args, **params):
        self.separator = separator
        
    def _read_mapper_output(self, fp):
        for line in fp:
            try:
                yield line.rstrip().split(self.separator, 1)
            except Exception, e:
                print >> sys.stderr, '_read_mapper_output except ', str(e), line
    def _get_value(self, values):
        for v in values:
            yield v[1]
    
    def reduce(self, k, vs):
        for v in vs:
            yield k, v
    
    def run(self):
        data = self._read_mapper_output(sys.stdin)
        for key, values in groupby(data, itemgetter(0)):
            try:
                for k, v in self.reduce(key, self._get_value(values)):
                    print self.separator.join((k, v))
            except Exception, e:
                print >> sys.stderr, 'reduce except ', str(e)


if __name__ == '__main__':
    from optparse import OptionParser
    parser = OptionParser()
    parser.set_usage("%prog [options] filename")
    parser.add_option("-m", "--mode", dest="mode", default="map", help="map or reduce", metavar="MODE")
     
    opts, args = parser.parse_args()
    
    if opts.mode.lower() in ("map", "mapper"):
        mapper = Mapper()
        mapper.run()
    elif opts.mode.lower() in ("reduce", "reducer"):
        reducer = Reducer()
        reducer.run()

