#!/usr/bin/env python

import random
from collections import defaultdict

from tornado import web
from tornado import ioloop
import tornado.options
import msgpack
import logging

logger = logging.getLogger("tornado.application")
logger.setLevel(logging.INFO)

from cocaine.services import Service
from cocaine.futures.chain import ChainFactory
from cocaine.exceptions import ServiceError

SERVICE_CACHE_COUNT = 5
tornado.options.define("port", default=8088, type=int, help="listening port number")
tornado.options.define("count", default=SERVICE_CACHE_COUNT, type=int, help="count of instances per service")


def gen(obj):
    headers = yield
    chunk = headers.get()
    for header, value in chunk['headers']:
        obj.add_header(header, value)
    obj.set_status(chunk['code'])
    while True:
        body = yield
        data = body.get()
        if data is not None:
            obj.write(data)
        else:
            break
    obj.finish()


class BasicHandler(web.RequestHandler):

    cache = defaultdict(list)

    def get_service(self, name):
        while True:
            if len(self.cache['key']) < tornado.options.options.count:
                try:
                    created = [Service(name) for _ in xrange(0, tornado.options.options.count - len(self.cache[name]))]
                    [logger.info("Connect to app: %s endpoint %s " % (app.servicename, app.service_endpoint)) for app in created]
                    self.cache[name].extend(created)
                except Exception as err:
                    logger.error(str(err))
                    return None
            chosen = random.choice(self.cache[name])
            if chosen.connected:
                return chosen
            else:
                logger.warning("Service %s disconnected %s" % (chosen.servicename,
                                                                    chosen.service_endpoint))
                self.cache[name].remove(chosen)


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
            import pprint
            pprint.pprint(d)
            fut = s.invoke(event, msgpack.packb(d))
            g = gen(self)
            g.next()
            fut.then(g.send).run()

    def handle(self, chunk):
        data = chunk.get()
        if data is not None:
            self.write(data)
        else:
            self.finish()


if __name__ == "__main__":
    tornado.options.parse_command_line()
    application = web.Application([
        (r"/([^/]*)/([^/]*)", BasicHandler)
        ])
    application.listen(tornado.options.options.port, no_keep_alive=False)
    ioloop.IOLoop.instance().start()
