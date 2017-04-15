from pyspark import SparkContext
import sys
from collections import defaultdict



def add(x,y):
    return x+y


def main():
    sc=SparkContext()
    A=sc.textFile(sys.argv[1])
    B=sc.textFile(sys.argv[2])
    output=sys.argv[3]
    fp = open(output, 'w')


    mat_A = A.map(lambda x: (x.split(',')[1],('A',(x.split(',')[0], x.split(',')[2]))))
    
    mat_B = B.map(lambda x: (x.split(',')[0],('B',(x.split(',')[1], x.split(',')[2]))))

    grouping_AB=mat_A.groupWith(mat_B)

    map2=grouping_AB.mapValues(reduce_intermediate).map(lambda x:x[1])
    reduce2=map2.flatMap(lambda x: x.items()).aggregateByKey(0,add,add).collect()

    
    for r in sorted(reduce2):
        r=str(r).replace("(","").replace(")","") 
        fp.write(r+"\n")   
    fp.close()

def reduce_intermediate(x):
    key = list(x[0])
    value=list(x[1])
    intermediate={}
    for i in xrange(0,len(key)):
        for j in xrange(0,len(value)):
            intermediate[(int(key[i][1][0]),int(value[j][1][0]))]=int(key[i][1][1])*int(value[j][1][1])
    return intermediate

if __name__ == "__main__":
    main()


    




   

        
                            
        



    
