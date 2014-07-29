from lib.domosmq import domosmq
import json

class domosreg:
    def __init__(self):
        self.mq = domosmq()
        self.mq.queue_bind('registration.#')
        self.mq.set_callback(callback)
        self.registrations = []
        
    def callback(self, msg):
        regmsg = json.loads(msg.body)
        
    def _addregistration(self)
    
    def getregistrations(self)