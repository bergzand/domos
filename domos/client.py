from domos.util.rpc import rpc
from domos.util.tabulate import tabulate

class client:
    
    
    def __init__(self, args):
        self.commanddict = {'list_modules': self.getModules}
        self.args = args
        self.rpc = rpc('client')
        
    def getModules(self):
        modules = self.rpc.call('api', 'getModules')
        headers = ['Name','Queue','Active']
        print(tabulate(modules, headers, tablefmt="orgtbl"))
        
        
    def main(self):
        command = self.commanddict[self.args.clientcmd]
        command()
    