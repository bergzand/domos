from domos.util.rpc import rpc
from domos.util.tabulate import tabulate
from pprint import pprint
import argparse

class client:

    apikey = 'api'

    def __init__(self, args):
        self.commanddict = {'list_modules': self.getModules,
                            'list_sensors': self.getSensors,
                            'list_args': self.getSensorArgs,
                            'list_prototypes': self.getPrototype}
        self.args=args
        self.rpc = rpc('client')

    def getModules(self):
        modules = self.rpc.call(client.apikey, 'getModules')
        headers = ['Name','Queue','Active']
        print(tabulate(modules, headers, tablefmt="orgtbl"))

    def getSensors(self):
        module = self.args.module
        sensors = self.rpc.call(client.apikey, 'getSensors', module=module)
        if not sensors:
            print("Error: Module not found")
        else:
            headers = ['Name', 'Instant', 'Active', 'Module', 'Description']
            print(tabulate(sensors, headers, tablefmt="orgtbl"))
    @staticmethod        
    def parsersettings(parser):
        clientcommands = parser.add_subparsers(title='client commands', dest='clientcmd')
        clientcommands.add_parser('list_modules', help='List all registered modules')
        protocmds = clientcommands.add_parser('list_prototypes', help='List sensor prototypes')
        protocmds.add_argument('module', nargs=1, help='Display prototypes from this module')

        sensorcmds = clientcommands.add_parser('list_sensors', help='List sensors')
        sensorcmds.add_argument('--module', '-m', nargs='?', help='module to query, all modules if omitted')

        argcmds = clientcommands.add_parser('list_args',  help='List sensor arguments')
        argcmds.add_argument('sensor', nargs=1, help='sensor to query')
        return parser
    
    #tabulate recursive dicts to infinite depth
    def _parseArgs(self, argdict, firstrun=False):
        data = []
        for name, value in dict.items(argdict):
            if type(value) == dict:
                tabulated = self._parseArgs(value)
                first = True
                for line in tabulated.splitlines():
                    if first:
                        first = False
                        data.append([name, line])
                    else:
                        data.append(['',line])
            else:
                data.append(['{}:'.format(name),value])
        if firstrun:
            table = tabulate(data, tablefmt='orgtbl')
        else:
            table = tabulate(data, tablefmt='plain')
        return table

    def getSensorArgs(self):
        sensor = self.args.sensor
        args = self.rpc.call(client.apikey, 'getArgs', sensor=sensor)
        args.pop('key')
        args.pop('ident')
        print(self._parseArgs(args, firstrun=True))

    def getPrototype(self):
        module = self.args.module
        proto = self.rpc.call(client.apikey, 'getProtos', module=module)
        headers = ['Name', 'Type','Optional','Description']
        for rpcname, arguments in proto:
            print(rpcname+':')
            print(tabulate(arguments, headers, tablefmt='orgtbl'))
          
    def main(self):
        try:
            command = self.commanddict[self.args.clientcmd]
        except KeyError:
            print("Invalid command specified")
        else:
            command()
