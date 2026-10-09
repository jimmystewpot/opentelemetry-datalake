import struct
def parse(metadata: bytes):
    v = metadata[0]
    version = v & 0x0F
    offset_size_minus_1 = (v >> 4) & 0x03
    offset_size = offset_size_minus_1 + 1
    # what else?
