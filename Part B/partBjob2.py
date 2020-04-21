from mrjob.job import MRJob

class PartB_Job2(MRJob):

    def mapper(self, _, line):
        try:
            if len(line.split(','))==5:#check if the transactions are clean
                #this should be the contracts dataset
                fields = line.split(',')
                jkey = fields[0]
                jvalue = int(fields[3])
                yield (jkey,(jvalue,1))
            #one mapper,we need to first differentiate among both types
            elif len(line.split('\t')) ==2:
                #this should be the transactions aggregate dataset. Output from job 1
                fields = line.split('\t')
                jkey = fields[0]
                jkey = jkey[1:-1]
                jvalue = int(fields[1])
                yield (key, (value,2))

        except:
            pass

    def reducer(self, address, values):
        block_number = False
        counts = 0
        # for value in values:
        yield(address,list(values))
        #     if value[1] == 1:
        #         block_number = True
        #     elif value[1] == 2:
        #         counts = value[0]
        # yield(address,counts)
        # if block_number==True and  counts > 0:
        #     yield(address, counts)

if __name__=='__main__':
    PartB_Job2.run()
