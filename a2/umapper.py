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
	for word in words:
		print(word.lower() + '\t1')
