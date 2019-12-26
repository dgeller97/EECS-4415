#!/usr/bin/python

# Jaskaran Garha, 214314439, garhajas
# David Geller, 214404255, dgeller

import sys
import csv
import re

for row in sys.stdin:
	row = re.sub(r'^\W+|\W+$', '', row) 
	#row = re.sub(r'[0-9]+', '', row) # to only keep words, get rid of numbers
	words = re.split(r'\W+',row)
	ngram = zip(*[words[i:] for i in range(2)]) # to create the ngram by using zip method
	for ng in ngram:
		print(' '.join(ng).lower() + '\t1')

# Refrences used
#http://www.albertauyeung.com/post/generating-ngrams-python/
