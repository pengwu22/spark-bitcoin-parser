"""
Filename: blocktools.py
Purpose: basic functions utilized by blockchain parser

Authors:
    * Alex (https://github.com/tenthirtyone)
    * Peng Wu (https://github.com/pw2393)

Licenses: MIT
"""

######
# Section I
# Author: Alex
######

import struct
import hashlib


def uint1(stream):
    return ord(stream.read(1))


def uint2(stream):
    return struct.unpack('H', stream.read(2))[0]


def uint4(stream):
    # type: (object) -> object
    return struct.unpack('I', stream.read(4))[0]


def uint8(stream):
    return struct.unpack('Q', stream.read(8))[0]


def hash32(stream):
    return stream.read(32)[::-1]


def time(stream):
    time = uint4(stream)
    return time


def varint(stream):
    size = uint1(stream)

    if size < 0xfd:
        return size
    if size == 0xfd:
        return uint2(stream)
    if size == 0xfe:
        return uint4(stream)
    if size == 0xff:
        return uint8(stream)
    return -1


def hashStr(bytebuffer):
    # return binascii.hexlify(pk_script)
    return ''.join(('%02x' % ord(a)) for a in bytebuffer)


######
# Section II
# Author: Peng Wu
######

def double_sha256(bytebuffer):
    """
    Dual SHA256 on raw byte string
    """
    return hashlib.sha256(hashlib.sha256(bytebuffer).digest()).digest()


def hashInt(bytebuffer):
    """
    Map a long hash string to an int smaller than power(2, 31)-1
    """
    import binascii
    hex_dig = binascii.hexlify(bytebuffer)
    return int(hex_dig, 16) % 2147483647


def rawpk2hash160(pk_script):
    """
    Locate the raw 20-byte hash160 value of a public key right after 0x14
    """
    head = pk_script.find('\x14') + 1
    return pk_script[head:head + 20]


def rawpk2addr(pk_script):
    """
    Convert a raw bitcoin block script of public key to a common address
    """
    import base58  # reference program: base58.py
    return base58.hash_160_to_bc_address(rawpk2hash160(pk_script))


def blktime2datetime(blktime):
    """
    Convert a bitcoin block timestamp integer to a datetime string.
    Note that current timestamp as seconds since 1970-01-01T00:00 UTC.
    """
    from datetime import timedelta, datetime
    d = datetime(1970, 1, 1, 0, 0, 0) + timedelta(days=int(blktime) / 86400, seconds=int(blktime) % 86400)
    return d.strftime('%Y-%m-%dT%H:%M:%S')
