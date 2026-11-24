import os, struct, zlib, sys

def chunk(kind, data):
    return struct.pack(">I", len(data)) + kind + data + struct.pack(">I", zlib.crc32(kind + data) & 0xffffffff)

def write_png(path, width, height):
    compressor = zlib.compressobj(6)
    parts = []
    for y in range(height):
        row = bytearray([0])  # PNG filter: None
        for x in range(width):
            row.extend(( (x * 11 + y * 3) & 255,
                         (x * 5 + y * 13) & 255,
                         (x * 17 + y * 7) & 255 ))
        parts.append(compressor.compress(bytes(row)))
    parts.append(compressor.flush())
    raw = b"".join(parts)
    ihdr = struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0)
    with open(path, "wb") as f:
        f.write(b"\x89PNG\r\n\x1a\n")
        f.write(chunk(b"IHDR", ihdr))
        f.write(chunk(b"IDAT", raw))
        f.write(chunk(b"IEND", b""))

out = sys.argv[1]
os.makedirs(out, exist_ok=True)
for size in (512, 1400, 2200):
    path = os.path.join(out, f"image_{size}x{size}.png")
    write_png(path, size, size)
    print(f"{path}\t{os.path.getsize(path)}")