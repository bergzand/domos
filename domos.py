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
    
def domosweb(args):
    from domos.domosweb import domosweb
    dom=domosweb(args)
    dom.main()



def parsersettings(parser):
    subparsers = parser.add_subparsers(title='subcommands', dest='cmd')
    parser.add_argument('--configfile', '-f',
                              help='location of the configfile to use', default='domos.cfg')
    serverparser = subparsers.add_parser('server', help='Server', description='Domos server')
    serverparser.set_defaults(func=domosserver)
    clientparser = subparsers.add_parser('client', help='Client, used to query the server',
                                         description='Domos client')
    clientparser.set_defaults(func=domosclient)
    webparser = subparsers.add_parser('web', help='webinterface to control the server',
                                         description='Domos web')
    webparser.set_defaults(func=domosweb)

    
    serverparser.add_argument('--verbose', '-v', action='count',
                              help='Verbosity of the server')
    serverparser.add_argument('--daemon', '-d', action='store_true',
                              help='Verbosity of the server')

    clientcommands = clientparser.add_subparsers(title='client commands', dest='clientcmd')
    clientcommands.add_parser('list_modules', help='List all registered modules')
    protocmds = clientcommands.add_parser('list_prototypes', help='List sensor prototypes')
    protocmds.add_argument('module', nargs=1, help='Display prototypes from this module')

    sensorcmds = clientcommands.add_parser('list_sensors', help='List sensors')
    sensorcmds.add_argument('--module', '-m', nargs='?', help='module to query, all modules if omitted')

    argcmds = clientcommands.add_parser('list_args',  help='List sensor arguments')
    argcmds.add_argument('sensor', nargs=1, help='sensor to query')
    
    
    websubparser = webparser.add_subparsers(title='weboption',dest='webcmd')
    adduserparser = websubparser.add_parser('adduser', help ='Add a user to the webserver',description='add an user')
    runparser = websubparser.add_parser('run', help ='Add a user to the webserver',description='add an user')
    runparser.add_argument('--debug','-D',action='store_true',help='Run the webserver in debug mode')
    
    adduserparser.add_argument('user')
    adduserparser.add_argument('passwd')
    adduserparser.add_argument('--admin','-A',action='store_true',help='Give the user admin priveleges')
    adduserparser.add_argument('--email','-m',action='store',help='The email address of the user')
        
    return parser


if __name__ == "__main__":
    parser = parsersettings(argparse.ArgumentParser(prog='Domos'))
    args = parser.parse_args()
    args.func(args)
