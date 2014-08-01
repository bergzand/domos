from dashi import DashiConnection
from threading import Thread
import socket
import domos

SHELLSENSOR_DICT={
    "name": "shellsensor",
    "queue": "shellsensor",
    "rpc": [
        {"key": "addshellinput", "type": "add"},
        {"key": "outputtoshell", "type": "set", "args":
        {"name":"value","type":"string"}}]
    }



class dashiThread(Thread):
    
    def __init__(self):
        Thread.__init__(self);
        self.done = False;
        #Registermessage
        name = "shellsensor"
        self.identifier = "shell";
        self.addaccepted = True;
        self.key = 2;
        dashiconfig = domos.util.domossettings.domosSettings.getDashiConfig()
        self.dashi = DashiConnection(name,dashiconfig['amqp_uri'],dashiconfig['exchange'],sysname = dashiconfig['sysname'])

        self.dashi.fire("domoscore","register",data=SHELLSENSOR_DICT)
        self.dashi.handle(self.listenshell,"addshellinput")
        self.dashi.handle(self.receive,"outputtoshell")
        #wait for add
        
    def sendValue(self,value):
        if self.addaccepted:
            print("sending: "+str(value))
            self.dashi.fire("domoscore","sensorValue",data={'key':self.key,'ident':self.ident,'value':value})
    
    def listenshell(self,key=None,ident=None):
        print("received add: key= ",str(key)," ident=",str(ident))
        self.key = key;
        self.identifier = ident;
        self.addaccepted = True;
    
    def receive(self,key=None,ident=None,value):
        print("received stuff:",str(value));
        print(">>");
    
    def run(self):
        while not self.done:
            try:
                self.dashi.consume(timeout = 2)
            except socket.timeout as blaaat:
                pass
    
    def end(self):
        self.done = True;




def main():
    dashithread = dashiThread()
    
    dashithread.start();
    
    done = False
    while not done:
        line = input(">> ");
        if(line == "quit"):
            done = True;
            dashithread.end()
        else:
            dashithread.sendValue(line.strip());
      
main();