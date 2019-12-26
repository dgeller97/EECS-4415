#!/usr/bin/python

# Jaskaran Garha, 214314439, garhajas
# David Geller, 214404255, dgeller

import sys
import re
import csv 




input_file = csv.reader(sys.stdin)
next(input_file) # skip the first row
for row in input_file:
	bid = row[0] # only keep business_id column
	cat = row[12].split(';') # only keep cateroies column
	for elem in cat:
                # print key value pairs
		print(elem + '\t' +  bid)
	'''
	table = []
	table.append(' : '.join([row[0], row[12]]))
	#table = '\n'.join(table)

	table = re.split(',', str(table))
	#table = [re.sub('^a-zA-Z0-9_-:;+', '', item) for item in table]
	print(table)
	
	for elem in table:
		x = elem.split(' : ')[0]
		x = re.sub("'", '', x)
		x = re.sub('"', '', x)
		y = elem.split(' : ')[1]
		y = re.sub('"', '', y)
		y = re.sub("'", '', y)
		z = y.split(';')
		for i in z:
			print(i.strip() + '\t' + x.strip())
		#print(z)
		#print(elem.split(';'))
	'''
