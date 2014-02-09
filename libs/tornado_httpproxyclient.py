#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Binux<i@binux.me>
#         http://binux.me
# Created on 2013-11-09 22:42:57

from __future__ import print_function
from tornado.simple_httpclient import SimpleAsyncHTTPClient, _HTTPConnection, native_str, re, HTTPHeaders
from tornado.httpclient import HTTPRequest
from time import clock
from sys import stdout

debug = False
def printf(str, *args):
    print(str % args, end='')
    stdout.flush()

def debug_printf(str, *args):
    if debug:
        printf(str, *args)

class MultiHTTPProxyClient:
    def __init__(self, request_data, output_request, patch_request, on_headers_callback):
        self.request_data = request_data
        self.output_request = output_request
        self.patch_request = patch_request
        self.on_headers_callback = on_headers_callback
        self.block_size = 1000000
        self.num_of_connections = 8
        self.num_of_blocks = 16
        self.slow_start_connection_limit = 2
        self.http_client = None
        self.range_begin = 0
        self.range_end = None
        self.content_length = None
        self.next_request_pos = None
        self.pending_write_pos = None
        self.length_request = None
        self.requests = []
        self.blocks = []
        self.block_finished_callback = None
        self.head_block = None
        self.tail_block = None
        self.final_response = None
        self.partial_content = False
        self.seq = 0
        self._closed = False
        self.start_tick = clock()

    def create_request(self):
        request = HTTPRequest(url=self.request_data['url'], headers=self.request_data['headers'])
        self.patch_request(request)
        request.headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1700.102 Safari/537.36'
        #request.connect_timeout = 0
        request.request_timeout = 0
        request.header_only = False
        return request

    def closed(self):
        return self._closed;

    def close(self):
        printf("# stop #\n")
        self._closed = True
        #for r in self.requests:
        #    del r
        self.requests = []
        for b in self.blocks:
            if b is not None:
                b.close()
                #del b
        self.blocks = []

    def fetch(self, callback):
        self.on_finished_callback = callback

        self.requests = []
        for i in range(self.num_of_connections):
            self.requests.append(self.create_request())

        if self.requests[0].headers.has_key('Range'):
            self.partial_content = True
            bytes_range = self.requests[0].headers['Range'].split('=')[1].split('-')
            self.range_begin = int(bytes_range[0])
            if len(bytes_range[1]) > 0:
                self.range_end = int(bytes_range[1])
        debug_printf("Requested Range: [%d, %s]\n", self.range_begin, str(self.range_end))
        debug_printf("Headers: %s\n", str(self.request_data['headers']))

        self.http_client = MyHTTPClient()
        self.retrieve_content_length(None)

    def retrieve_content_length(self, response):
        if self.range_end is not None:
            self.content_length = self.range_end - self.range_begin + 1
            self.start()
            return
        if response is not None:
            debug_printf("Response code: %s\n", response.code)
            debug_printf("Headers: %s\n", str(response.headers))
            if 200 <= response.code < 300:
                #self.http_client.close()
                self.http_client = None
                self.content_length = int(response.headers['Content-Length'])
                debug_printf("Content-Length: %d\n", self.content_length)
                self.range_end = self.range_begin + self.content_length - 1;
                debug_printf("Range: [%d, %d]\n", self.range_begin, self.range_end)
                self.on_headers_callback(response.code, response.headers, self.start)
                return
            else:
                debug_printf("Error on file size. Retry.\n")
        self.length_request = self.create_request()
        self.length_request.header_only = True
        self.http_client.fetch(self.length_request, self.retrieve_content_length)

    def start(self):
        printf("# start #\n")
        debug_printf("output_request: %s\n", str(self.output_request))
        self.length_request = None
        self.next_request_pos = self.range_begin
        self.pending_write_pos = self.range_begin

        self.blocks = []
        for i in range(self.num_of_blocks):
            self.blocks.append(Block(i, self, self.schedule_callback))
        for i in range(self.num_of_blocks):
            self.blocks[i].nextBlock = self.blocks[(i+1) % self.num_of_blocks]
        self.head_block = self.blocks[self.num_of_blocks-1]
        self.tail_block = self.blocks[0]

        self.schedule_callback()

    def printBufferStats(self):
        printf("    ");
        for b in self.blocks:
            stat = "O" if b.pending else "^" if b.sending else "?" if b.request else "-"
            head_tail_or_sep = "(" if b == self.head_block else ")" if b.nextBlock == self.tail_block else " "
            printf("%s%s", stat, head_tail_or_sep)
        seconds_elapsed = clock() - self.start_tick
        estimated_Bps = (self.pending_write_pos - self.range_begin)/seconds_elapsed
        remaining_seconds = 0.0 if estimated_Bps == 0 else max(0, self.range_end - self.pending_write_pos)/estimated_Bps
        printf("   %8.1f KB/s %4.0f mins %2.0f seconds\r", estimated_Bps/1024.0, remaining_seconds/60, remaining_seconds%60)

    def schedule_callback(self, no_send = False):
        while self.schedule_a_block():
            pass
        if not no_send:
            nextBlock = self.head_block.nextBlock
            if nextBlock != self.tail_block and nextBlock.pending:
                debug_printf("Try to send Block %d/%d\n", nextBlock.id, nextBlock.seq)
                nextBlock.send()

    def schedule_a_block(self):
        debug_printf("# schedule_callback():  %d requests left #\n", len(self.requests))
        debug_printf("head: %d  tail: %d\n", self.head_block.id, self.tail_block.id)
        self.printBufferStats()

        if self.head_block == self.tail_block:
            debug_printf("All blocks are full. wait...\n")
            return False
        if not self.requests:
            debug_printf("All connection slots are downloading. Wait...\n")
            return False

        if self.num_of_connections - len(self.requests) + 1 > self.slow_start_connection_limit:
            debug_printf("\n ## Slow start. seq: %d  connections: %d\n\n", self.seq, self.num_of_connections - len(self.requests))
            return False

        if self.next_request_pos > self.range_end:
            debug_printf("Done. No more blocks.  %d   %d\n", self.next_request_pos, self.range_end)
            return False

        if self.closed():
            printf("Client disconnected. No new downloading.\n")
            return False

        if self.pending_write_pos > self.range_end:
            printf("ALL DONE!!!   %d   %d-%d : %d\n", self.next_request_pos, self.range_begin, self.range_end, self.content_length)
            if self.partial_content:
                self.final_response.code = 206
                self.final_response.reason = 'Partial Content'
            else:
                self.final_response.code = 200
                self.final_response.reason = 'OK'
            del self.final_response.headers['Content-Range']
            self.final_response.headers['Content-Length'] = str(self.content_length)
            self.on_finished_callback(self.final_response)
            return False

        end = min(self.next_request_pos + self.block_size, self.range_end + 1)
        begin = self.next_request_pos
        self.next_request_pos += self.block_size

        block = self.tail_block
        self.tail_block = block.nextBlock
        block.seq = self.seq
        self.seq += 1

        block.fetch(self.requests.pop(0), begin, end - 1)
        return True

class Block:
    def __init__(self, id, parent, schedule_callback):
        self.id = id
        self.seq = -1
        self.parent = parent
        self.request = None
        self.schedule_callback = schedule_callback
        self.bytes_recevied = 0
        self.bytes_sent = 0
        self.range_begin = 0
        self.range_end = 0
        self.http_client = MyHTTPClient()
        self.data = None
        self.nextBlock = None
        self._closed = True
        self.pending = False
        self.sending = False

    def closed(self):
        return self._closed;

    def close(self):
        if not self.closed():
            self._closed = True
            #self.parent = None
            self.request = None
            self.http_client.close()
            #del self.http_client
            self.http_client = None
            #del self.data
            self.data = None
            self.nextBlock = None

    def send(self):
        if self.parent.closed():
            debug_printf("Disconected. Stop sending blocks.\n")
            return
        if self.parent.output_request.connection.stream.closed():
            return
        debug_printf("Block: %d/%d  send ----      %d  %d  %s\n", self.id, self.seq, self.range_begin, self.parent.pending_write_pos, str(self.range_begin <= self.parent.pending_write_pos))
        if self.range_begin <= self.parent.pending_write_pos and self.data is not None:
            data = self.data
            self.data = None
            self.parent.pending_write_pos += len(data)
            self.bytes_sent += len(data)
            self.sending = True
            self.parent.output_request.write(data, self.send_done)
            return
        debug_printf("Pending block %d/%d  wait..\n", self.id, self.seq)
        self.pending = True
        self.parent.printBufferStats()
        self.schedule_callback(no_send = True)
        
    def send_done(self):
        debug_printf("Block: %d/%d  send_done      %d\n", self.id, self.seq, self.parent.pending_write_pos)
        self.sending = False
        self.pending = False
        self.parent.head_block = self
        if self.parent.pending_write_pos <= self.range_end:
            debug_printf("ASSERT halt. not enough data in this block\n")
        seq = "None"
        if self.nextBlock is not None:
            seq = str(self.nextBlock.seq)
        debug_printf("Block: %d/%d->%s  uploading finish  R:%d  S:%d  P:%d\n", self.id, self.seq, seq, self.bytes_recevied, self.bytes_sent, self.parent.pending_write_pos)
        self.schedule_callback()

    def fetch(self, request, range_begin, range_end):
        self.bytes_recevied = 0
        self.bytes_sent = 0
        self.range_begin = range_begin
        self.range_end = range_end
        self.data = None
        self._closed = False
        self.request = request

        self.parent.printBufferStats()
        self.request.headers['Range'] = 'bytes=' + str(range_begin) + '-' + str(range_end)
        debug_printf("Block: %d/%d  Range: [%s]\n", self.id, self.seq, self.request.headers['Range'])
        self.http_client.fetch(self.request, self.on_fetch_finished_callback)

    def on_fetch_finished_callback(self, response):
        if self.parent.closed():
            debug_printf("Disconnected. No more calls on finish_callback.\n")
            return

        if response.buffer is not None:
            data = response.buffer.getvalue()
            self.bytes_recevied = len(data)
        debug_printf("Block: %d/%d  response code: %s   bytes_recevied: %d\n", self.id, self.seq , response.code, self.bytes_recevied)
        if self.bytes_recevied < self.range_end - self.range_begin + 1:
            debug_printf("A partial block(%d/%d) is downloaded. Retry.  %d vs %d\n", self.id, self.seq, self.bytes_recevied, self.range_end - self.range_begin + 1)
            #debug_printf("response headers: %s\n", str(response.headers))
            self.range_begin += self.bytes_sent
            self.fetch(self.request, self.range_begin, self.range_end)
            return
        debug_printf("Block: %d/%d  downloading finish  %s\n", self.id, self.seq, self.request.headers['Range'])

        if self.parent.closed():
            debug_printf("Disconnected. No more in stream.\n")
            return
        if self.parent.slow_start_connection_limit < self.parent.num_of_connections:
            self.parent.slow_start_connection_limit += 1
        self.parent.requests.append(self.request)
        self.request = None
        self.data = data
        self.send()

        #debug_printf("finish response: %s %s\n", type(response), str(response))
        if self.parent.final_response is None:
            self.parent.final_response = response
    
class MyHTTPClient(SimpleAsyncHTTPClient):
    def __init__(self, *args, **kwargs):
        super(MyHTTPClient, self).__init__(*args, **kwargs)
        self._closed = False

    def close(self):
        debug_printf("MyHTTPClient.close()")
        if self._closed:
            return

        self._closed = True
        try:
            self.connection.stream.close()
            self.connection._on_body(b"")   # todo: it's wrong when there's no header yet.
            self.connection._on_close()
        except Exception:
            print("exception catched")
        finally:
            self.connection = None
            super(MyHTTPClient, self).close()

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
            content_length = 0

        if self.request.header_only:
            self.request.header_only = False
            self._on_body(b"")
            return

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
                    content_length != 0):
                raise ValueError("Response with code %d should not have body" %
                                 self.code)
            self._on_body(b"")
            return

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
