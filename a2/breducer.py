#!/usr/bin/python

# Jaskaran Garha, 214314439, garhajas
# David Geller, 214404255, dgeller

import sys

previous = None
sum = 0

for line in sys.stdin:
	key, value = line.split('\t') # key and values are split
	if key != previous:
		if previous is not None:
                        # new key so print old
                        print(str(sum) + '\t' + previous)
		previous = key
		sum = 0;
	#increment the values
	sum = sum + int(value)

# for the last iteration
print(str(sum) + '\t' + previous)

