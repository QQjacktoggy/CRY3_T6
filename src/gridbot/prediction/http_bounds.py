"""Bound HTTP wire bytes and decoded bytes before parsing untrusted JSON."""
from __future__ import annotations

import zlib

CHUNK_BYTES = 16 * 1024
PREDICTION_BODY_BYTES = 2 * 1024 * 1024  # Paginated markets/orders/books.
ERROR_BODY_BYTES = 64 * 1024
JEV_BODY_BYTES = 256 * 1024
KLINES_BODY_BYTES = 64 * 1024  # Only 17 one-minute candles are requested.


class ResponseBodyError(ValueError):
    pass


class BoundedBody:
    """A streaming decoder; neither compression nor false lengths bypass caps."""
    def __init__(self, headers, limit):
        self.limit = limit
        self.wire_bytes = 0
        self.body = bytearray()
        headers = {str(k).lower(): str(v) for k, v in (headers or {}).items()}
        try:
            length = int(headers.get('content-length', ''))
        except ValueError:
            length = 0  # Missing/malformed lengths never replace actual counting.
        if length > limit:
            raise ResponseBodyError('HTTP response exceeds byte limit')
        encoding = headers.get('content-encoding', 'identity').strip().lower()
        if encoding in ('', 'identity'):
            self.decoder = None
        elif encoding in ('gzip', 'deflate'):
            self.decoder = zlib.decompressobj(31 if encoding == 'gzip' else 15)
        else:
            raise ResponseBodyError('Unsupported HTTP content encoding')

    def feed(self, chunk):
        self.wire_bytes += len(chunk)
        if self.wire_bytes > self.limit:
            raise ResponseBodyError('HTTP response exceeds wire byte limit')
        try:
            decoded = (self.decoder.decompress(chunk, self.limit - len(self.body) + 1)
                       if self.decoder else chunk)
        except zlib.error as exc:
            raise ResponseBodyError('Invalid HTTP compression') from exc
        if len(self.body) + len(decoded) > self.limit:
            raise ResponseBodyError('HTTP response exceeds decoded byte limit')
        self.body.extend(decoded)
        if self.decoder and (self.decoder.unconsumed_tail or self.decoder.unused_data):
            raise ResponseBodyError('HTTP compression exceeds limit or has trailing data')

    def finish(self):
        if self.decoder and not self.decoder.eof:
            raise ResponseBodyError('Incomplete HTTP compressed body')
        return bytes(self.body)


def read_bounded(stream, headers, limit):
    decoder = BoundedBody(headers, limit)
    while True:
        chunk = stream.read(min(CHUNK_BYTES, limit - decoder.wire_bytes + 1))
        if not chunk:
            return decoder.finish()
        decoder.feed(chunk)


async def read_bounded_async(stream, headers, limit):
    decoder = BoundedBody(headers, limit)
    while True:
        chunk = await stream.read(min(CHUNK_BYTES, limit - decoder.wire_bytes + 1))
        if not chunk:
            return decoder.finish()
        decoder.feed(chunk)
