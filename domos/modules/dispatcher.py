#!/usr/bin/env python3


import sys
import asyncio
import argparse
import logging
import subprocess

import aiocoap


class dispatcher:
    def __init__(self, ):
        logging.basicConfig()
        logging.getLogger('coap').setLevel(logging.WARNING)
        asyncio.get_event_loop().run_until_complete(self._getendpoint())


    def incoming_observation(self, options, response):
        sys.stdout.buffer.write(b'\f')
        sys.stdout.buffer.write(response.payload)
        sys.stdout.buffer.flush()

    @asyncio.coroutine
    def _getendpoint(self):
        self.endpoint = yield from aiocoap.Endpoint.create_client_endpoint()


    def getRequest(self, url):
        self.sendRequest(code=aiocoap.GET, url=url)

    def postRequest(self, url):
        self.sendRequest(code=aiocoap.POST, url=url)

    def sendRequest(self, code=aiocoap.GET, url="coap://localhost"):
        response_data = asyncio.get_event_loop().run_until_complete(self.dispatchrequest(code=code, url=url))
        if response_data.code.is_successful():
            sys.stdout.buffer.write(response_data.payload)
            sys.stdout.buffer.flush()

    @asyncio.coroutine
    def dispatchrequest(self, code=aiocoap.GET, url="coap://localhost/"):
        request = aiocoap.Message(code=code)
        try:
            yield from request.set_request_uri(url)
        except ValueError as e:
            raise parser.error(e)

        # if options.observe:
        # request.opt.observe = 0
        # observation_is_over = asyncio.Future()

        requester = aiocoap.protocol.Requester(self.endpoint, request)
        responsedata = yield from requester.response
        return responsedata


    @asyncio.coroutine
    def main(self, request):
        if options.observe:
            requester.observation.register_errback(observation_is_over.set_result)
            requester.observation.register_callback(lambda data, options=options: incoming_observation(options, data))

        response_data = yield from requester.response
        if response_data.code.is_successful():
            sys.stdout.buffer.write(response_data.payload)
            sys.stdout.buffer.flush()
        else:
            print(response_data.code, file=sys.stderr)
            if response_data.payload:
                print(response_data.payload.decode('utf-8'), file=sys.stderr)
            sys.exit(1)

        if options.observe:
            exit_reason = yield from observation_is_over
            print(exit_reason, file=sys.stderr)
