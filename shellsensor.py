from dashi import DashiConnection
from threading import Thread
import socket
import domos

SHELLSENSOR_DICT={
    "name": "shellsensor",
    "queue": "shellsensor",
    "rpc": [
        {"key": "addshellinput", "type": "add", "args":[
        {"name":"prompt" ,"type":"string"}]},
        {"key": "outputtoshell", "type": "set", "args":[
        {"name":"value","type":"string"},
        {"name":"prefix" ,"type":"string"}]}]
    }



class dashiThread(Thread):
    
    def __init__(self):
        Thread.__init__(self);
        self.done = False;
        #Registermessage
        self.name = "shellsensor"
        dashiconfig = domos.util.domossettings.domosSettings.getDashiConfig()
        self.dashi = DashiConnection(self.name,dashiconfig['amqp_uri'],dashiconfig['exchange'],sysname = dashiconfig['sysname'])
        print("connecting to system:{sys}, exhange: {ex}, as {name}".format(sys=dashiconfig['sysname'], ex=dashiconfig['exchange'], name=self.name))
        try:
            response = self.dashi.call("domoscore","register",data=SHELLSENSOR_DICT)
        except Exception as err:
            print("Exception: {0}".format(type(err)))
            self.addaccepted = False
        else:
            print("response:" + str(response))
            self.identifier = response[0]["ident"]
            self.key        = response[0]["key"]
            self.prompt     = response[0]["prompt"]
            print("registered as:{id} with key: {key}".format(id=self.identifier,key=self.key))
            self.dashi.handle(self.listenshell,"addshellinput")
            self.dashi.handle(self.receive,"outputtoshell")
            self.addaccepted = True

            
        #wait for add
        
    def sendValue(self,value):
        if self.addaccepted:
            print("sending: "+str(value))
            self.dashi.fire("domoscore","sensorValue",data={'key':self.key,'ident':self.identifier,'value':value})
    
    def listenshell(self,key=None,ident=None):
        print("received add: key= ",str(key)," ident=",str(ident))
        self.key = key;
        self.identifier = ident;
        self.addaccepted = True;
    
    def receive(self,key=None,ident=None,value=None,prefix=None):
        print("received stuff:",str(value));
        print(str(prefix));
    
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
    if not dashithread.addaccepted:
        print("unable to connect")
        return
    dashithread.start();
    
    done = False
    while not done:
        line = input(dashithread.prompt);
        if(line == "quit"):
            done = True;
            dashithread.end()
        else:
            dashithread.sendValue(line.strip());
      
main();