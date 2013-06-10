#!/usr/bin/env python

import random

from tornado import web
from tornado import ioloop
import tornado.options 
import msgpack
import logging

logger = logging.getLogger("tornado.application")
logger.setLevel(logging.ERROR)

from cocaine.services import Service
from cocaine.exceptions import ServiceError

tornado.options.define("port", default=8088, type=int, help="listening port number")

SERVICE_CACHE_COUNT = 5

class BasicHandler(web.RequestHandler):

    cache = dict()

    def get_service(self, name):
        if not self.cache.has_key(name):
            try:
                self.cache[name] = [Service(name) for _ in xrange(0,SERVICE_CACHE_COUNT)]
            except Exception as err:
                return None
        return random.choice(self.cache[name])

    @web.asynchronous
    def get(self, name, event):
        s = self.get_service(name)
        if s is None:
            self.send_error(status_code=404)
        else:
            d = dict()
            d['meta'] = {   "cookie" : "",
                            "headers" : {} }
            d['body'] = self.request.body
            d['request'] = dict((param, value[0]) for param, value in self.request.arguments.iteritems())
            fut = s.invoke(event, msgpack.packb(d))
            fut.bind(self.on_done, self.on_error, self.on_error)
            self.fut = fut

    def on_done(self, chunk):
        for header, value in chunk['headers']:
            self.add_header(header, value)
        self.set_status(chunk['code'])
        self.fut.bind(self.on_done2, self.on_error, self.on_done2)

    def on_done2(self, chunk):
        try:
            self.write(chunk)
            self.finish()
        except Exception as err:
            pass

    def on_error(self, exceptions):
        self.send_error(status_code=503)


if __name__ == "__main__":
    tornado.options.parse_command_line()
    application = web.Application([
        (r"/([^/]*)/([^/]*)", BasicHandler)
        ])
    application.listen(tornado.options.options.port, no_keep_alive=False)
    ioloop.IOLoop.instance().start()






