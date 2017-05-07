"""
Author: Peng Wu
License: MIT
"""


# Initialize Spark Context: local multi-threads
from pyspark import SparkConf, SparkContext
from blocktools import *
from block import Block


output_folder = './csv/'
import os
if not os.path.exists(output_folder):
    os.makedirs(output_folder)


def parse(blockchain):
    blocks = []
    ######
    continueParsing = True
    counter_blk = 0
    blockchain.seek(0, 2)
    fSize = blockchain.tell() - 80  # Minus last Block header size for partial file
    blockchain.seek(0, 0)
    while continueParsing:
        block = Block(blockchain)
        continueParsing = block.continueParsing
        if continueParsing:
            #block.toString()
            blocks.append(block.toMemory())
        counter_blk += 1

    print ''
    print 'Reached End of Field'
    print 'Parsed blocks:{}'.format(counter_blk)
    return blocks


def main():

    # Initialize Spark Context: local multi-threads
    conf = SparkConf().setAppName("BTC-Parser")
    sc = SparkContext(conf=conf)

    rawfiles = sc.parallelize([sys.argv[i] for i in range(1, len(sys.argv))])

    # Transformations and/or Actions
    blocks = rawfiles.map(lambda filename: parse(open(filename))).flatMap(lambda x:x).cache()

    # End Program
    blocks.map(lambda block:block[0]).saveAsTextFile(output_folder+'inputs_mapping')
    blocks.map(lambda block:block[1]).saveAsTextFile(output_folder+'outputs')
    blocks.map(lambda block:block[2]).saveAsTextFile(output_folder+'transactions')


if __name__ == "__main__":

    import sys
    if len(sys.argv) < 2:
        print "\n\tUSAGE:\n\t\tspark-submit spark_parser.py filename1.dat filename2.dat ..."
        sys.exit()

    import time
    start_time = time.time()
    main()
    print("--- %s seconds ---" % (time.time() - start_time))
