#!/usr/bin/python

# Jaskaran Garha, 214314439, garhajas
# David Geller, 214404255, dgeller

import sys
import re

previous = None
sum = 0

for line in sys.stdin:
        line = line.strip()
        line = re.sub(r'^\W+|\W+$', '', line)
        key, value = line.split(':')
        #key = bid & days
        #value = #checkins
        if key != previous:
                if previous is not None:
                        #new key arrived, so print the old
                        print(str(previous) + ', ' + str(sum))
                previous = key
                sum = 0
        sum = sum + int(value) # add up the checkins
# print last iteration
print(str(previous) + ', ' + str(sum))
