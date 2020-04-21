"""Lab 1. Basic wordcount
"""
from mrjob.job import MRJob

import time
#this is a regular expression that finds all the words inside a String


#This line declares the class Lab1, that extends the MRJob format.
class Part1(MRJob):

# this class will define two additional methods: the mapper method goes here
    def mapper(self, _, line):
        try:
            fields = line.split(",")
            if (len(fields)==6):
                time_epoch = int(fields[6])
                day = time.strftime("%Y-%m",time.gmtime(time_epoch))
                # print(day)
                yield(day,1)
             except:
            pass
#and the reducer method goes after this line
    def reducer(self, day, counts):
        x=sum(counts)
        yield(day,x)
        # print(day+"---"+str(x))

#this part of the python script tells to actually run the defined MapReduce job. Note that Lab1 is the name of the class
if __name__ == '__main__':
    Part1.run()
