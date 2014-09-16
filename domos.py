#!/usr/bin/env python

import socket
import sys
from threading import Thread
import argparse
from domos.server import domos as server
from domos.client import client
from domos.domosweb import domosweb as web
def domosclient(args):
    client = client(args)
    client.main()


def domosserver(args):
    dom = server(args)
    dom.main()
    
def domosweb(args):
    dom=web(args)
    dom.main()



def parsersettings(parser):
    subparsers = parser.add_subparsers(title='subcommands', dest='cmd')
    parser.add_argument('--configfile', '-f',
                              help='location of the configfile to use', default='domos.cfg')
                              
                              
    serverparser = subparsers.add_parser('server', help='Server', description='Domos server')
    serverparser.set_defaults(func=domosserver)
    serverparser = server.parsersettings(serverparser)
    
    clientparser = subparsers.add_parser('client', help='Client, used to query the server',
                                         description='Domos client')
    clientparser.set_defaults(func=domosclient)
    clientparser = client.parsersettings(clientparser)
    
    
    webparser = subparsers.add_parser('web', help='webinterface to control the server',
                                         description='Domos web')
    webparser.set_defaults(func=domosweb)
    webparser = web.parsersettings(webparser)
           
    return parser


if __name__ == "__main__":
    parser = parsersettings(argparse.ArgumentParser(prog='Domos'))
    args = parser.parse_args()
    args.func(args)
