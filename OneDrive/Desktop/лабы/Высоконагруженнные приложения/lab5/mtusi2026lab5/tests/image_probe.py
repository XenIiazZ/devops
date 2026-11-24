"""Read JPEG SOF dimensions without external libraries; not a full image decoder."""
from client import Failure, check

def jpeg_size(data):
    check(data[:2]==b'\xff\xd8','thumbnail is not JPEG')
    i = 2
    sof = {0xc0,0xc1,0xc2,0xc3,0xc5,0xc6,0xc7,0xc9,0xca,0xcb,0xcd,0xce,0xcf}
    while i < len(data):
        check(data[i]==0xff,'invalid JPEG marker')
        while i<len(data) and data[i]==0xff:
            i += 1
        check(i<len(data),'truncated JPEG marker')
        marker = data[i]
        i += 1
        if marker in {0xd9,0xda}:
            break
        if marker in {0x01,0xd8} or 0xd0<=marker<=0xd7:
            continue
        check(i+2<=len(data),'truncated JPEG segment')
        length = int.from_bytes(data[i:i+2],'big')
        check(length>=2 and i+length<=len(data),'invalid JPEG segment length')
        if marker in sof:
            check(length>=8,'invalid JPEG SOF')
            return (int.from_bytes(data[i+5:i+7],'big'),int.from_bytes(data[i+3:i+5],'big'))
        i += length
    raise Failure('JPEG dimensions not found')
