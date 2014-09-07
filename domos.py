#!/usr/bin/env python

import socket
import sys
from threading import Thread
import argparse


def domosclient(args):
    from domos.client import client
    client = client(args)
    client.main()


def domosserver(args):
    from domos.server import domos
    dom = domos(args)
    dom.main()


def parsersettings(parser):
    subparsers = parser.add_subparsers(title='subcommands', dest='cmd')
    serverparser = subparsers.add_parser('server', help='Server', description='Domos server')
    serverparser.set_defaults(func=domosserver)
    clientparser = subparsers.add_parser('client', help='Client, used to query the server',
                                         description='Domos client')
    clientparser.set_defaults(func=domosclient)

    
    serverparser.add_argument('--verbose', '-v', action='count',
                              help='Verbosity of the server')
    serverparser.add_argument('--daemon', '-d', action='store_true',
                              help='Verbosity of the server')
    serverparser.add_argument('--configfile', '-f',
                              help='location of the configfile to use')

    clientcommands = clientparser.add_subparsers(title='client commands', dest='clientcmd')
    clientcommands.add_parser('list_modules', description='List all modules')
    return parser


if __name__ == "__main__":
    parser = parsersettings(argparse.ArgumentParser(prog='Domos'))
    args = parser.parse_args()
    args.func(args)
