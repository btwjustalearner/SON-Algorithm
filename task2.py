from pyspark import SparkContext
import sys
import re
import time
import itertools
from collections import Counter
import copy

start_time = time.time()

filter_threshold = float(sys.argv[1])
support = float(sys.argv[2])
input_file = sys.argv[3]
output_file = sys.argv[4]

sc = SparkContext('local[*]', 'task2')
sc.setLogLevel('ERROR')

smallRDD = sc.textFile(input_file)
header = smallRDD.first()  # extract header
RDD0 = smallRDD.filter(lambda row: row != header)   # filter out header

output = open(output_file, 'w')

RDD1 = RDD0.map(lambda line: (line.split(',')[0], [line.split(',')[1]]))

RDD21 = RDD1.reduceByKey(lambda a, b: a+b).mapValues(lambda v: set(v))

RDD2 = RDD21.filter(lambda x: len(x[1]) > filter_threshold)

users = RDD2.map(lambda x: x[0]).collect()

length_all = RDD2.count()

RDDb2u = RDD1.map(lambda x: (x[1][0], [x[0]])).filter(lambda x: x[1][0] in users)\
    .reduceByKey(lambda a, b: a+b).mapValues(lambda v: set(v))

b2u_list = RDDb2u.collect()
b2u_dict = {}
for pair in b2u_list:
    b2u_dict[pair[0]] = pair[1]


def phase1(data):
    new_support = support
    candidates = []
    output = []
    fq_l = len(freq_tmp)
    for i in range(fq_l - 1):
        for j in range(i+1, fq_l):
            x = freq_tmp[i]
            y = freq_tmp[j]
            if x[:size-2] == y[:size-2]:
                candidate = x+y[-1:]
                candidates.append(candidate)
            else:
                break
    for candidate in candidates:
        c = 0
        for bid in candidate:
            n_uid_set = b2u_dict[bid]
            if c == 0:
                uid_set = copy.deepcopy(n_uid_set)
            uid_set = uid_set.intersection(n_uid_set)
            c += 1
        if len(uid_set) >= new_support:
            output.append(tuple(candidate))
    return output


def phase2(data):
    data_list = [d[1] for d in data]
    counts_dict = {}
    for line in data_list:
        for candidate in cands:
            c_tuple = tuple(sorted(candidate))
            c_set = set(candidate)
            if c_set.issubset(line):
                if c_tuple in counts_dict.keys():
                    counts_dict[c_tuple] += 1
                else:
                    counts_dict[c_tuple] = 1
    return counts_dict.items()


output1 = []
output2 = []
singletons = RDDb2u.filter(lambda x: len(x[1]) >= support)\
    .map(lambda x: x[0]).collect()
singletons = [[x] for x in singletons]
singletons.sort()
output1.extend(singletons)
output2.extend(singletons)

freq_tmp = copy.deepcopy(singletons)
size = 2
while len(freq_tmp) > 1:
    cands = RDD2.repartition(1).mapPartitions(lambda values: phase1(values)).collect()
    cands = set(cands)
    cands = [list(x) for x in cands]
    cands.sort()
    freqs = RDD2.mapPartitions(lambda values: phase2(values))\
        .reduceByKey(lambda x, y: (x+y)).filter(lambda item: item[1] >= support)\
        .sortBy(lambda item: item[0]).map(lambda item: item[0]).collect()
    freqs = [list(x) for x in freqs]
    output1.extend(cands)
    output2.extend(freqs)
    freq_tmp = copy.deepcopy(freqs)
    size += 1

output.write('Candidates:'+'\n')
size = 1
line = ''
for i in range(len(output1)):
    candidate = output1[i]
    if len(candidate) == size:
        if line != '':
            line += ","
        subline = "("
        for j in range(len(candidate)):
            element = candidate[j]
            subline += "'"+element+"'"
            if j == len(candidate)-1:
                subline += ")"
            else:
                subline += ", "
        line += subline
    if len(candidate) != size:
        size += 1
        output.write(line+"\n")
        output.write("\n")
        line = ''
        subline = "("
        for j in range(len(candidate)):
            element = candidate[j]
            subline += "'"+element+"'"
            if j == len(candidate)-1:
                subline += ")"
            else:
                subline += ", "
        line += subline
    if i == len(output1)-1:
        output.write(line+"\n")
output.write("\n")
output.write("Frequent Itemsets:"+"\n")

size = 1
line = ''
for i in range(len(output2)):
    candidate = output2[i]
    if len(candidate) == size:
        if line != '':
            line += ","
        subline = "("
        for j in range(len(candidate)):
            element = candidate[j]
            subline += "'"+element+"'"
            if j == len(candidate)-1:
                subline += ")"
            else:
                subline += ", "
        line += subline
    if len(candidate) != size:
        size += 1
        output.write(line+"\n")
        output.write("\n")
        line = ''
        subline = "("
        for j in range(len(candidate)):
            element = candidate[j]
            subline += "'"+element+"'"
            if j == len(candidate)-1:
                subline += ")"
            else:
                subline += ", "
        line += subline
    if i == len(output2)-1:
        output.write(line)

print('Duration: '+ str(time.time() - start_time))
