#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Binux<i@binux.me>
#         http://binux.me
# Created on 2013-11-09 22:42:57

from __future__ import print_function
from tornado.simple_httpclient import SimpleAsyncHTTPClient, _HTTPConnection, native_str, re, HTTPHeaders
from time import sleep, clock
import sys

def printf(str, *args):
    print(str % args, end='')
    sys.stdout.flush()

class HTTPProxyClient(SimpleAsyncHTTPClient):
    def __init__(self, *args, **kwargs):
        super(HTTPProxyClient, self).__init__(*args, **kwargs)
        self._closed = False

    def close(self):
        print("HTTPProxyClient.close()")
        sys.stdout.flush()              # affect execution order/behavior?
        self.connection.stream.close()
        self.connection._on_body(b"")   # todo: it's wrong when there's no header yet.
        self.connection._on_close()
        
        super(HTTPProxyClient, self).close()
        self._closed = True

    def _handle_request(self, request, release_callback, final_callback):
        self.connection = HTTPConnection(self.io_loop, self, request, release_callback,
                       final_callback, self.max_buffer_size, self.resolver)

class HTTPConnection(_HTTPConnection):
    def _on_headers(self, data):
        data = native_str(data.decode("latin1"))
        first_line, _, header_data = data.partition("\n")
        match = re.match("HTTP/1.[01] ([0-9]+) ([^\r]*)", first_line)
        assert match
        code = int(match.group(1))
        self.headers = HTTPHeaders.parse(header_data)
        if 100 <= code < 200:
            self._handle_1xx(code)
            return
        else:
            self.code = code
            self.reason = match.group(2)

        if (self.request.follow_redirects and
            self.request.max_redirects > 0 and
                self.code in (301, 302, 303, 307)):
            self._on_body(b"")
            return

        if "Content-Length" in self.headers:
            if "," in self.headers["Content-Length"]:
                # Proxies sometimes cause Content-Length headers to get
                # duplicated.  If all the values are identical then we can
                # use them but if they differ it's an error.
                pieces = re.split(r',\s*', self.headers["Content-Length"])
                if any(i != pieces[0] for i in pieces):
                    raise ValueError("Multiple unequal Content-Lengths: %r" %
                                     self.headers["Content-Length"])
                self.headers["Content-Length"] = pieces[0]
            content_length = int(self.headers["Content-Length"])
        else:
            content_length = None

        if self.request.header_callback is not None:
            # re-attach the newline we split on earlier
            self.request.header_callback(first_line + _)
            for k, v in self.headers.get_all():
                self.request.header_callback("%s: %s\r\n" % (k, v))
            self.request.header_callback('\r\n')

        if self.request.method == "HEAD" or self.code == 304:
            # HEAD requests and 304 responses never have content, even
            # though they may have content-length headers
            self._on_body(b"")
            return
        if 100 <= self.code < 200 or self.code == 204:
            # These response codes never have bodies
            # http://www.w3.org/Protocols/rfc2616/rfc2616-sec4.html#sec4.3
            if ("Transfer-Encoding" in self.headers or
                    content_length not in (None, 0)):
                raise ValueError("Response with code %d should not have body" %
                                 self.code)
            self._on_body(b"")
            return

        if self.request.on_headers_callback:
            self.io_loop.add_callback(self.request.on_headers_callback, self.code, self.headers)
        if self.request.raw_streaming_callback:
            self.stream = patch_iostream(self.stream)
            chunk_size = 256*1024
            def raw_streaming_callback(data):
                self.request.raw_streaming_callback(data, read_more)
                if self.stream.closed():
                    self._on_body(b"")
            def read_more():
                ready_callback()

            def ready_callback():
                #print "ready_callback", self.stream.closed(), self.stream.reading(), self.stream._read_buffer_size
                if not self.stream.closed() and not self.stream.reading():
                    #self.stream._read_to_buffer()
                    self.stream.read_bytes(chunk_size, lambda x: x, raw_streaming_callback)
            ready_callback()
        else:
            if (self.request.use_gzip and
                    self.headers.get("Content-Encoding") == "gzip"):
                self._decompressor = GzipDecompressor()
            if self.headers.get("Transfer-Encoding") == "chunked":
                self.chunks = []
                self.stream.read_until(b"\r\n", self._on_chunk_length)
            elif content_length is not None:
                self.stream.read_bytes(content_length, self._on_body)
            else:
                self.stream.read_until_close(self._on_body)
       
def patch_iostream(iostream):
    self = iostream
    self.n = 0;
    self.max_buffer_size = 32*1024*1024
    self.buffer_limit = 1*1024*1024
    self.bytes_recevied = 0
    self.bytes_sent = 0
    self.bytes_recevied_recently = 0
    self.bytes_sent_recently = 0
    self.time_interval_of_average_speed = 1.0
    self.recent_interval_start_time = clock()
    self.sending_Bps = 0.0
    self.receiving_Bps = 0.0
    self.original_read_to_buffer = self._read_to_buffer
    self.original_consume = self._consume

    def measureSpeed(full):
        now = clock()
        seconds_elapsed = now - self.recent_interval_start_time
        if seconds_elapsed >= self.time_interval_of_average_speed:
            self.receiving_Bps = self.bytes_recevied_recently/seconds_elapsed/1024.0
            self.sending_Bps = self.bytes_sent_recently/seconds_elapsed/1024.0
            #printf(" R byte %d   time %f \n",self.bytes_recevied_recently, seconds_elapsed)
            #printf(" S byte %d   time %f \n",self.bytes_sent_recently, seconds_elapsed)
            self.recent_interval_start_time = now
            self.bytes_recevied_recently = 0
            self.bytes_sent_recently = 0
        if full:
            self.n = (self.n + 1) % 10
            printf("Full%s%s  ", "."*self.n, " "*(10-self.n))
            sleep(0.3)
        else:
            self.n = 0
            printf("                ")
        printf("%10d %10d %10d %8.1f %8.1f     \r", self._read_buffer_size, self.bytes_recevied, self.bytes_sent, self.receiving_Bps, self.sending_Bps)

    def _read_to_buffer():
        if self._read_buffer_size >= self.buffer_limit:
            measureSpeed(True)
            return 0
        length = self.original_read_to_buffer()
        self.bytes_recevied += length
        self.bytes_recevied_recently += length
        measureSpeed(False)
        #printf("+ %10d\n", length)
        return length

    def _consume(loc):
        buf = self.original_consume(loc)
        length = len(buf)
        self.bytes_sent += length
        self.bytes_sent_recently += length
        measureSpeed(False)
        #printf("- %10d\n", length)
        return buf

    self._read_to_buffer = _read_to_buffer
    self._consume = _consume
    return iostream
