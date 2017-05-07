"""
Filename: block.py
Purpose: definitions blockchain component classes and methods

Authors:
    * Alex (https://github.com/tenthirtyone)
    * Peng Wu (https://github.com/pw2393)

New features (compared with https://github.com/tenthirtyone/blocktools):
    * Identify bloch hash
    * Identify transaction hash
    # CSV file output

Licenses: MIT
"""

from blocktools import *


class BlockHeader:
    def __init__(self, blockchain):
        self.pos_start = blockchain.tell()
        self.hash = double_sha256(blockchain.read(80))[::-1]
        blockchain.seek(self.pos_start)
        ######
        self.version = uint4(blockchain)
        self.previousHash = hash32(blockchain)
        self.merkleHash = hash32(blockchain)
        self.time = uint4(blockchain)
        self.bits = uint4(blockchain)
        self.nonce = uint4(blockchain)


    def toString(self):
        print "Hash:\t %s" %hashStr(self.hash)
        print "Version:\t %d" % self.version
        print "Previous Hash\t %s" % hashStr(self.previousHash)
        print "Merkle Root\t %s" % hashStr(self.merkleHash)
        print "Time\t\t %s" % str(self.time)
        print "Difficulty\t %8x" % self.bits
        print "Nonce\t\t %s" % self.nonce


class Block:
    def __init__(self, blockchain):
        self.continueParsing = True
        self.magicNum = 0
        self.blocksize = 0
        self.blockheader = ''
        self.txCount = 0
        self.Txs = []

        if self.hasLength(blockchain, 8):
            self.magicNum = uint4(blockchain)
            self.blocksize = uint4(blockchain)
        else:
            self.continueParsing = False
            return

        if self.hasLength(blockchain, self.blocksize):
            self.setHeader(blockchain)
            self.txCount = varint(blockchain)
            self.Txs = []

            for i in range(0, self.txCount):
                tx = Tx(blockchain)
                self.Txs.append(tx)
        else:
            self.continueParsing = False

    def continueParsing(self):
        return self.continueParsing

    def getBlocksize(self):
        return self.blocksize

    def hasLength(self, blockchain, size):
        curPos = blockchain.tell()
        blockchain.seek(0, 2)

        fileSize = blockchain.tell()
        blockchain.seek(curPos)

        tempBlockSize = fileSize - curPos
        #print tempBlockSize
        if tempBlockSize < size:
            return False
        return True

    def setHeader(self, blockchain):
        self.blockHeader = BlockHeader(blockchain)

    def toString(self):
        print ""
        print "Magic No: \t%8x" % self.magicNum
        print "Blocksize: \t", self.blocksize
        print ""
        print "#" * 10 + " Block Header " + "#" * 10
        self.blockHeader.toString()
        print "##### Tx Count: %d" % self.txCount
        for t in self.Txs:
            pass
            #t.toString()
            #if hashStr(t.hash) == "3ae43bb0a8e4cc3a345a7a2a688217bcb8d9f7e9001263930cd23e9b3b7364c6":
            #    raise KeyError

    def toMemory(self):
        inputrows = ''
        outputrows = ''
        txrows = ''
        timestamp = blktime2datetime(self.blockHeader.time)
        # if timestamp.startswith('2013-10-25'):
        for tx in self.Txs:
            for m, input in enumerate(tx.inputs):
                inputrows+=('{},{},{},{},{}\n'.format(hashStr(tx.hash),
                                                   m,
                                                   hashStr(input.prevhash),
                                                   input.txOutId,
                                                   timestamp))
            amount = 0
            for n, output in enumerate(tx.outputs):
                amount += output.value
                outputrows+=('{},{},{},{}\n'.format(hashStr(tx.hash),
                                                    n,
                                                    output.value,
                                                    rawpk2addr(output.pubkey)))
            txrows+=('{},{},{},{},{}\n'.format(hashStr(tx.hash),
                                               amount,
                                               timestamp,
                                               tx.inCount,
                                               tx.outCount))
        return inputrows, outputrows, txrows


class Tx:
    def __init__(self, blockchain):
        self.pos_start = blockchain.tell()
        ######
        self.version = uint4(blockchain)
        self.inCount = varint(blockchain)
        self.inputs = []
        for i in range(0, self.inCount):
            input = txInput(blockchain)
            self.inputs.append(input)
        self.outCount = varint(blockchain)
        self.outputs = []
        if self.outCount > 0:
            for i in range(0, self.outCount):
                output = txOutput(blockchain)
                self.outputs.append(output)
        self.lockTime = uint4(blockchain)
        ######
        self.pos_end = blockchain.tell()
        blockchain.seek(self.pos_start)
        self.hash = double_sha256(blockchain.read(self.pos_end - self.pos_start))[::-1]

    def toString(self):
        print ""
        print "=" * 10 + " New Transaction " + "=" * 10
        print "Tx Version:\t %d" % self.version
        print "Inputs:\t\t %d" % self.inCount
        for i in self.inputs:
            i.toString()

        print "Outputs:\t %d" % self.outCount
        for o in self.outputs:
            o.toString()
        print "Lock Time:\t %d" % self.lockTime
        ######
        print "Tx Hash:\t %s" % hashStr(self.hash)


class txInput:
    def __init__(self, blockchain):
        self.prevhash = hash32(blockchain)
        self.txOutId = uint4(blockchain)
        self.scriptLen = varint(blockchain)
        self.scriptSig = blockchain.read(self.scriptLen)
        self.seqNo = uint4(blockchain)

    def toString(self):
        print "Previous Hash:\t %s" % hashStr(self.prevhash)
        print "Tx Out Index:\t %8x" % self.txOutId
        print "Script Length:\t %d" % self.scriptLen
        print "Script Sig:\t %s" % hashStr(self.scriptSig)
        print "Sequence:\t %8x" % self.seqNo


class txOutput:
    def __init__(self, blockchain):
        self.value = uint8(blockchain)
        self.scriptLen = varint(blockchain)
        self.pubkey = blockchain.read(self.scriptLen)

    def toString(self):
        print "Value:\t\t %d" % self.value
        print "Script Len:\t %d" % self.scriptLen
        print "Pubkey:\t\t %s" % hashStr(self.pubkey)

