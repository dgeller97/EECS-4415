#!/usr/bin/python

# Jaskaran Garha, 214314439, garhajas
# David Geller, 214404255, dgeller

import sys
import re
import csv


input_file = csv.reader(sys.stdin)
next(input_file) # skip the first row
for row in input_file:
        bid = row[0] # keep the bid
        days = row[1] # the days
        checkins = row[3] # checkins

        print(bid + ', ' + days + ' : ' + checkins)
	#print format of mapper
