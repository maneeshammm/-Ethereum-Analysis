from mrjob.job import MRJob

class PartB_Job3(MRJob):

    def mapper(self, _, line):
        try:
            fields = line.split('\t')
            #one mapper,we need to first differentiate among both types
            if len(fields)==2:
                #address of a company sector line
                address = fields[0][1:-2]
                count = int(fields[1])
                yield (None, (address, count))
        except:
            pass

    def combiner(self, _,values):
        sorted_values = sorted(values,reverse = True, key = lambda tup:tup[1])
        i=0
        for value in sorted_values:
            yield ("top", value)
            i += 1
            if i >= 10:
                 break

    def reducer(self, _,values):
        sorted_values = sorted(values, reverse = True, key = lambda tup:tup[1])
        i = 1
        for value in sorted_values:
            yield (i, ("{} - {}".format(value[0],value[1])))

            i += 1
            if i > 10:
                break


if __name__ =='__main__':
    PartB_Job3.run()
