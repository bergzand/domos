from dashi import DashiConnection
from threading import Thread
from domos.util.rpc import rpc
import socket
import domos

SHELLSENSOR_DICT = {
    "name": "shellsensor",
    "queue": "shellsensor",
    "rpc": [
        {"key": "addshellinput", "type": "add", "args": [
            {"name": "prompt", "type": "string", "optional": "True"}]},
        {"key": "outputtoshell", "type": "set", "args": [
            {"name": "value", "type": "string"},
            {"name": "prefix", "type": "string", "optional": "True"}]}]
    }


class dashiThread(Thread):

    def __init__(self):
        Thread.__init__(self)
        self.name = "shellsensor"
        self.rpc = rpc(self.name)
        self.done = False
        #Registermessage
        self.rpc.log_info("connecting")
        try:
            response = self.rpc.call("domoscore",
                                     "register",
                                     data=SHELLSENSOR_DICT)
        except Exception as err:
            self.rpc.log_error("Exception: {0}".format(type(err)))
            self.addaccepted = False
        else:
            self.rpc.handle(self.addshellinput, "addshellinput")
            self.rpc.handle(self.receive, "outputtoshell")
            for sensor in response:
                self.addshellinput(**sensor)
            #wait for add

    def sendValue(self, value):
        if self.addaccepted:
            self.rpc.log_debug("sending: "+str(value))
            self.rpc.fire("domoscore",
                          "sensorValue",
                          key=self.key,
                          value=value)

    def addshellinput(self, key=None, name=None, prompt=">>"):
        self.identifier = name
        self.key = key
        self.prompt = prompt
        self.rpc.log_info("registered as:{id} with key: {key}".format(id=self. identifier, key=self.key))
        print(("registered as:{id} with key: {key}".format(id=self.identifier, key=self.key)))
        self.addaccepted = True

    def receive(self, key=None, name=None, value=None, prefix="Received: "):
        print(prefix+str(value))
        print(str(self.prompt))

    def run(self):
        while not self.done:
            self.rpc.listen()

    def end(self):
        self.done = True


def main():
    dashithread = dashiThread()
    if not dashithread.addaccepted:
        print("unable to connect")
        return
    dashithread.start()
    done = False
    while not done:
        line = input(dashithread.prompt)
        if(line == "quit"):
            done = True
            dashithread.end()
        else:
            dashithread.sendValue(line.strip())

if __name__ == "__main__":
    main()
