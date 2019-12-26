#!/usr/bin/python

# Jaskaran Garha, 214314439, garhajas
# David Geller, 214404255, dgeller

import sys
import re
def printfunc():
	print("hello")


previous = None
sum = 0
str_out = ''
for line in sys.stdin:
	key, value = line.split('\t')
	if key != previous:
		if previous is not None:
			str_out = str_out.replace("\n","") # get rid of newline character
			print('\n') #print the new index on a new line
			print(str(previous) + ": " + str(str_out[:-2]) , end='') #print the business id's on same line
			#print(str(sum) + '\t' + previous)
		previous = key
		str_out = '' # empty string because new key now
	str_out = str((str_out + value + ', ')) # append to string
	#print(str_out)
	#str_out.append(value)
	#print(previous + ':\t' + str_out, end='')
	#sum = sum + int(value)
print('\n')
str_out = str_out.replace("\n","")
print(str(previous) + ": " +  str(str_out[:-2]) , end='')
print('')
