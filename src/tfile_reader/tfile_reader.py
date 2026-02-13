"""
https://github.com/facebookarchive/hadoop-20/blob/master/src/core/org/apache/hadoop/io/file/tfile/TFileDumper.java
"""

import io
import struct
import zlib
from pathlib import Path
from typing import TYPE_CHECKING, BinaryIO, NamedTuple

if TYPE_CHECKING:
    from collections.abc import Generator


class MetaIndexEntry(NamedTuple):
    "BCFile meta index entry."

    algo: str  # compression algorithm: gz, none
    off: int  # offset in file
    c_sz: int  # compressed size
    r_sz: int  # original size


class BlockRegion(NamedTuple):
    "BCFile block."

    off: int  # offset in file
    c_sz: int  # compressed size
    r_sz: int  # original size


class TFileIndexEntry(NamedTuple):
    "TFile index entry."

    key: str  # last key in block
    kv_entries: int  # no. of entries in block


class Log(NamedTuple):
    "Parsed application log from a container."

    container: str  # container name
    type: str  # log type
    content: str  # log text


def read_vlong(f: "io.BytesIO | io.BufferedReader") -> int:
    "Decode the variable-length integer."
    read_ushort = lambda f: struct.unpack(">H", f.read(2))[0]  # noqa: E731
    raw_first_byte = f.read(1)
    if not raw_first_byte:
        raise EOFError
    first_byte = struct.unpack("b", raw_first_byte)[0]
    if first_byte >= -32:  # noqa: PLR2004
        return first_byte
    if -72 <= first_byte <= -33:  # noqa: PLR2004
        return ((first_byte + 52) << 8) | ord(f.read(1))
    if -104 <= first_byte <= -73:  # noqa: PLR2004
        return ((first_byte + 88) << 16) | read_ushort(f)
    if -120 <= first_byte <= -105:  # noqa: PLR2004
        return ((first_byte + 112) << 24) | (read_ushort(f) << 8) | ord(f.read(1))
    if -128 <= first_byte <= -121:  # noqa: PLR2004
        length = first_byte + 129
        if 4 <= length <= 8:  # noqa: PLR2004
            return int.from_bytes(f.read(length), byteorder="big", signed=True)
    msg = "Corrupted VLong encoding"
    raise ValueError(msg)


def decompress(data: "io.BytesIO | bytes", algo: str) -> io.BytesIO:
    "Decompress data using zlib if algo=gz or keep as-is if algo=none."
    if isinstance(data, io.BytesIO):
        data = data.read()
    if algo == "gz":  # gzipped data starts with 0x78
        data = zlib.decompress(data)
    elif algo == "none":
        pass
    else:
        msg = f"Unknown compression type {algo}"
        raise ValueError(msg)
    return io.BytesIO(data)


def read_byte_array(buf: "io.BytesIO | io.BufferedReader") -> bytearray:
    "Read a variable length (possibly chunked) byte array from buffer."
    array = bytearray()
    length = read_vlong(buf)
    try:
        while length < 0:
            array.extend(buf.read(abs(length)))
            # we may end up exactly at EOF
            length = read_vlong(buf)
    except EOFError:
        print(f"WARN: unexpected EOF while reading chunked byte array at {buf.tell()}")
        return array
    array.extend(buf.read(length))
    return array


def read_utf8(data: "io.BytesIO | bytes | bytearray") -> str:
    "Read string in Java writeUTF format: 2-byte length + content."
    if isinstance(data, (bytes, bytearray)):
        buf = io.BytesIO(data)
    elif isinstance(data, io.BytesIO):
        buf = data
    else:
        raise TypeError
    raw_data_len = buf.read(2)
    if not raw_data_len:
        raise EOFError
    data_len = struct.unpack(">H", raw_data_len)[0]
    return buf.read(data_len).decode()


def read_utf8_array(val_bytes: bytes) -> "Generator[str]":
    "Read a list of UTF-8 strings using `read_utf8`."
    buf = io.BytesIO(val_bytes)
    try:
        while True:
            yield read_utf8(buf)
    except EOFError:
        return


class TFileIndex:
    "TFile index data."

    BLOCK_NAME = "TFile.index"

    def __init__(self, buf: BinaryIO, meta_index: "dict[str, MetaIndexEntry]") -> None:
        "Read first key and a list of TFileIndexEntry objects."
        m = meta_index[TFileIndex.BLOCK_NAME]
        buf.seek(m.off)
        buf = decompress(buf.read(m.c_sz), m.algo)
        self.first_key = read_utf8(read_byte_array(buf))
        self.index: "list[TFileIndexEntry]" = []
        try:
            while True:
                _buf = io.BytesIO(read_byte_array(buf))
                key = read_utf8(read_byte_array(_buf))
                entries = read_vlong(_buf)
                self.index.append(TFileIndexEntry(key, entries))
        except EOFError:
            pass


class TFileMeta:
    "TFile metadata."

    BLOCK_NAME = "TFile.meta"

    def __init__(self, buf: BinaryIO, meta_index: "dict[str, MetaIndexEntry]") -> None:
        "Read TFile version, record count and str_comparator."
        m = meta_index[self.BLOCK_NAME]
        buf.seek(m.off)
        buf = decompress(buf.read(m.c_sz), m.algo)
        self._version = struct.unpack(">I", buf.read(4))[0]
        self.record_count = read_vlong(buf)
        self.str_comparator = read_byte_array(buf).decode()

    @property
    def version(self) -> str:
        "Human-readable TFile version."
        return f"{self._version >> 16}.{self._version & 0xFFFF}"


class DataIndex:
    "List of blocks in a BCFile file and a compression algorithm."

    BLOCK_NAME = "BCFile.index"

    def __init__(self, buf: BinaryIO, meta_index: "dict[str, MetaIndexEntry]") -> None:
        "Read compression algorithm and a list of blocks."
        m = meta_index[self.BLOCK_NAME]
        buf.seek(m.off)
        buf = decompress(buf.read(m.c_sz), m.algo)
        self.default_compression_algorithm = read_byte_array(buf).decode()
        n = read_vlong(buf)
        self.list_regions = (
            BlockRegion(
                off=read_vlong(buf),
                c_sz=read_vlong(buf),
                r_sz=read_vlong(buf),
            )
            for _ in range(n)
        )


class TFileReader:
    "Hadoop TFile reader."

    # The Magic Bytes from BCFile.java
    AB_MAGIC_BCFILE = (
        b"\xd1\x11\xd3\x68\x91\xb5\xd7\xb6\x39\xdf\x41\x40\x92\xba\xe1\x50"
    )
    # Log file specific fields
    log_version: int = 0  # VERSION field
    app_acl: "dict[str, str] | None" = None  # APPLICATION_ACL field
    app_owner: str = ""  # APPLICATION_OWNER field

    def __init__(self, path: Path) -> None:
        "Open a TFile and parse metadata."
        self.f = path.open("rb")
        self.f.seek(0, 2)
        size = self.f.tell()

        # Footer: Magic(16), Version(4), MetaIndexOffset(8)
        self.f.seek(size - len(self.AB_MAGIC_BCFILE) - 4 - 8)
        offset_index_meta = struct.unpack(">Q", self.f.read(8))[0]
        self._version = struct.unpack(">I", self.f.read(4))[0]

        magic = self.f.read(16)
        if magic != self.AB_MAGIC_BCFILE:
            msg = f"Magic mismatch! Found: {magic.hex()}"
            raise ValueError(msg)

        self.f.seek(offset_index_meta)
        self.meta_index = self._meta_index(self.f)
        self.meta = TFileMeta(self.f, self.meta_index)
        self.index = TFileIndex(self.f, self.meta_index)

    @property
    def version(self) -> str:
        "Human-readable BCFile version."
        return f"{self._version >> 16}.{self._version & 0xFFFF}"

    @staticmethod
    def _meta_index(buf: io.BufferedReader) -> "dict[str, MetaIndexEntry]":
        "Read BCFile meta data, e.g. location of data blocks index."
        index: "dict[str, MetaIndexEntry]" = {}
        for _ in range(read_vlong(buf)):
            # MetaIndexEntry
            full_name = read_byte_array(buf).decode()
            meta_name = full_name[5:] if full_name.startswith("data:") else full_name
            algo = read_byte_array(buf).decode()
            # BlockRegion
            off = read_vlong(buf)
            c_sz = read_vlong(buf)
            r_sz = read_vlong(buf)
            index[meta_name] = MetaIndexEntry(algo, off, c_sz, r_sz)
        return index

    def __iter__(self) -> "Generator[tuple[str, bytes]]":
        "Iterate over TFile key-value pairs."
        index = DataIndex(self.f, self.meta_index)
        for blk in index.list_regions:
            self.f.seek(blk.off)
            buf = decompress(self.f.read(blk.c_sz), index.default_compression_algorithm)
            try:
                while True:
                    k = read_utf8(read_byte_array(buf))
                    v = read_byte_array(buf)
                    yield k, v
            except EOFError:
                if buf.tell() != blk.r_sz:
                    print(f"WARN: expected {blk.r_sz} bytes, got {buf.tell()}")

    def parse(self) -> "Generator[Log]":
        "Parse loaded TFile."
        for key, val in self:
            if key == "VERSION":
                self.log_version = struct.unpack(">I", val)[0]
            elif key == "APPLICATION_ACL":
                items = [i.strip() for i in read_utf8_array(val)]
                acl = dict(zip(items[::2], items[1::2]))
                self.app_acl = acl
            elif key == "APPLICATION_OWNER":
                self.app_owner = read_utf8(val)
            else:
                buf = io.BytesIO(val)
                try:
                    while True:
                        log_type = read_utf8(buf)
                        log_length = int(read_utf8(buf))
                        if log_length:
                            log_content = buf.read(log_length).decode()
                            yield Log(key, log_type, log_content)
                except EOFError:
                    pass
