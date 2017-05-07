"""
Filename: base58.py
Purpose: encode/decode base58 in the same way that Bitcoin does

Author: Gavin Andresen (github.com/gavinandresen/bitcointools)
License: MIT
"""

__b58chars = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz'
__b58base = len(__b58chars)


def b58encode(v):
    """ encode v, which is a string of bytes, to base58."""

    long_value = 0L
    for (i, c) in enumerate(v[::-1]):
        long_value += ord(c) << (8 * i)  # 2x speedup vs. exponentiation

    result = ''
    while long_value >= __b58base:
        div, mod = divmod(long_value, __b58base)
        result = __b58chars[mod] + result
        long_value = div
    result = __b58chars[long_value] + result

    # Bitcoin does a little leading-zero-compression:
    # leading 0-bytes in the input become leading-1s
    nPad = 0
    for c in v:
        if c == '\0':
            nPad += 1
        else:
            break

    return (__b58chars[0] * nPad) + result


def b58decode(v, length):
    """ decode v into a string of len bytes
  """
    long_value = 0L
    for (i, c) in enumerate(v[::-1]):
        long_value += __b58chars.find(c) * (__b58base ** i)

    result = ''
    while long_value >= 256:
        div, mod = divmod(long_value, 256)
        result = chr(mod) + result
        long_value = div
    result = chr(long_value) + result

    nPad = 0
    for c in v:
        if c == __b58chars[0]:
            nPad += 1
        else:
            break

    result = chr(0) * nPad + result
    if length is not None and len(result) != length:
        return None

    return result


try:
    import hashlib

    hashlib.new('ripemd160')
    have_crypto = True
except ImportError:
    have_crypto = False


def hash_160(public_key):
    if not have_crypto:
        return ''
    h1 = hashlib.sha256(public_key).digest()
    r160 = hashlib.new('ripemd160')
    r160.update(h1)
    h2 = r160.digest()
    return h2


def public_key_to_bc_address(public_key, version="\x00"):
    if not have_crypto or public_key is None:
        return ''
    h160 = hash_160(public_key)
    return hash_160_to_bc_address(h160, version=version)


def hash_160_to_bc_address(h160, version="\x00"):
    if not have_crypto:
        return ''
    vh160 = version + h160
    h3 = hashlib.sha256(hashlib.sha256(vh160).digest()).digest()
    addr = vh160 + h3[0:4]
    return b58encode(addr)


def bc_address_to_hash_160(addr):
    bytes = b58decode(addr, 25)
    return bytes[1:21]


if __name__ == '__main__':
    x = '005cc87f4a3fdfe3a2346b6953267ca867282630d3f9b78e64'.decode('hex_codec')
    encoded = b58encode(x)
    print encoded, '19TbMSWwHvnxAKy12iNm3KdbGfzfaMFViT'
    print b58decode(encoded, len(x)).encode('hex_codec'), x.encode('hex_codec')
    print hash_160_to_bc_address('991fd9bf50ad8d6a551efceb5c228d21bcc0fe92'.decode('hex_codec'))
