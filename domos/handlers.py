import operator as op
from plyplus import Grammar, STransformer
import threading
import queue as qu
import domos.util.domossettings as ds
from domos.util.domossettings import domosSettings
from domos.util.db import *
import peewee
import pprint
import logging

GRAMMAR = """
    start: lgc;             // This is the top of the hierarchy
    ?lgc: ( lgc lgc_symbol )? eql;
    ?eql: ( eql eql_symbol )? add;
    ?add: ( add add_symbol )? mul;
    ?mul: ( mul mul_symbol )? exp;
    ?exp: ( exp exp_symbol )? atom;
    @atom: neg | number | string | sensor | trigger | true_symbol | false_symbol | parenthesis;
    parenthesis: popen_sym add pclose_sym;
    neg: '-' atom;
    true_symbol: 'True' | 'true' | 'Yes' | 'yes';
    false_symbol: 'False' | 'false' | 'No' | 'no';
    string: '[\"]\w+[\"]';
    number: '[\d.]+';           // Regular expression for a decimal number
    sensor: '__sens\d+__';      // Sensor database match
    trigger: '__trig\d+__';     // trigger database match
    macro: '__macr\d+__';       // macro database match
    mul_symbol: '\*' | '/' | '//' | '%'; // Match * or / or %
    add_symbol: '\+' | '-'; // Match + or -
    exp_symbol: '\*\*';
    eql_symbol: '==' | '<=' | '!=' | '>=' |'<' | '>';
    lgc_symbol: '\|\|' | '\&\&';
    bit_symbol: '\|' | '\&' | '\^';
    popen_sym: '\(';
    pclose_sym: '\)';
    WHITESPACE: '[ \t]+' (%ignore);
"""
    
class triggerChecker(threading.Thread):
    
    def __init__(self, queue=None, loghandler=None, loglevel = None):
        """
        triggerchecker thread class.
        keeps watching a queue for sensor or triggers that changed and
        checks all triggers depending on that item.
         - queue: a queue object to use as a queue to watch. if none 
         given one is created
         - logger: a logger object to use. Usually a rpc object is ok.
        """
        threading.Thread.__init__(self)
        self.actionqueue = None
        self.shutdown = False
        self.logger = logging.getLogger('Trigger')
        if loglevel:
            self.logger.setLevel(loglevel)
        else:
            self.logger.setLevel(domosSettings.getLoggingLevel('Trigger'))
        if loghandler:
            self.logger.addHandler(loghandler)
        self.logger.debug("Initializing trigger checker thread")
        
        self.grammar = Grammar(GRAMMAR)
        if not queue:
            self.q = qu.Queue()
        else:
            self.q = queue
        try:
            self.db = dbhandler()
            self.db.connect()
            self.calculator = matchcalculator(self.db, grammar=GRAMMAR)
        except:
            self.logger.critical("Could not connect to database, shutting down")
            self.shutdown = True

    def getqueue(self):
        """
        returns the queue created by the initialization
        """
        return self.q

    def setactionqueue(self, queue):
        """
        Set the queue to send triggers to for action checking
        """
        self.actionqueue = queue
        
    def _checktrigger(self, trigger, item):
        """
        Checks a single trigger
        """
        type, id, value = item
        match = trigger.Match
        #TODO: add function dict
        triggervalue = self.calculator.resolve(match, item)
        print("New:",triggervalue,"old:",trigger.Lastvalue)
        if trigger.Lastvalue != triggervalue:
            self.logger.debug("Trigger {0} now has value {1}".format(trigger.id, triggervalue))
            self.db.addTriggervalue(trigger, triggervalue)
            self.q.put(("trigger", trigger.id, triggervalue))
            if self.actionqueue:
                self.actionqueue.put(("trigger", trigger.id, triggervalue))

    def processitem(self, item):
        """
        Takes an item and looks the depending triggers up
         - item: a tuple containing an type (trigger or sensor), 
         the ID of the sensor/trigger and the new value.
        """
        type, id, value = item
        self.logger.debug("Receiving item message")
        if type == "sensor":
            triggers = self.db.gettriggersfromsensor(id)
        elif type == "trigger":
            triggers = self.db.gettriggersfromtrigger(id)
        for trigger in triggers:
            self._checktrigger(trigger, item)

    def run(self):
        """
        standard run loop
        """
        while not self.shutdown:
            try:
                item = self.q.get(timeout=2)
                self.processitem(item)
            except qu.Empty:
                pass


class actionhandler(threading.Thread):
    '''
    Action handler thread. Separate thread for handling and activating actions.
    '''

    def __init__(self, rpc, queue=None, loghandler=None, loglevel=None):
        threading.Thread.__init__(self)
        self.shutdown = False
        self.logger = logging.getLogger('Action')
        if loglevel:
            self.logger.setLevel(loglevel)
        else:
            self.logger.setLevel(domosSettings.getLoggingLevel('Action'))
        if loghandler:
            self.logger.addHandler(loghandler)
        self.logger.debug("Initializing action checker thread")
        self.rpc = rpc
        if not queue:
            self.q = qu.Queue()
        else:
            self.q = queue
        try:
            self.db = dbhandler()
            self.db.connect()
            self.calculator = matchcalculator(self.db, GRAMMAR)
        except:
            self.logger.crit("Could not connect to database, shutting down")
            self.shutdown = True

    def getqueue(self):
        return self.q
        
    def _callaction(self, action):
        actionargs = self.db.getActions(action)
        kwargs = {}
        for actionarg in actionargs:
            value = self.calculator.resolve(actionarg.Value)
            rpcarg = actionarg.RPCArg
            kwargs = self.db.getdict(kwargs, rpcarg.name, value)
        kwargs['key'] = action.id
        kwargs['ident'] = action.ident
        module = action.Module
        self.rpc.fire(module.queue,
                      self.db.getRPCCall(module, 'set').Key,
                      **kwargs)

    def processitem(self, item):
        #get all actions attached
        type, trigger, value = item
        actions = self.db.getActionsfromtrigger(trigger)
        self.logger.debug("Found {} action with trigger".format(len(actions)))
        for action in actions:
            #check if action is activated
            active = self.calculator.resolve(action.Match, item)
            #TODO: supply variables for transform
            try:
                active = float(active)
            except:
                self.logger.debug("Parsing action activation condition as string")
            if active:
                self.logger.info("Calling action {}".format(action.Action.ident))
                self._callaction(action.Action)
                

    def run(self):
        while not self.shutdown:
            try:
                item = self.q.get(timeout=2)
                self.processitem(item)
            except qu.Empty:
                pass


class matchcalculator:
    '''
    Class to resolve match objects from the database. resolves a match record to a value.
    '''
    grammarobj = None

    def __init__(self, database, grammar=None):
        '''
         - database: database connection object from peewee, 
         used to look up sensor and trigger variables
         - grammar: grammar to use. if not given, it uses a class variable as grammar
        '''
        self.db = database
        if (not matchcalculator.grammarobj) and grammar:
            matchcalculator.grammarobj = Grammar(GRAMMAR)

    def _fetchvars(self, match, updateditem=None):
        if updateditem:
            type, id, value = updateditem
        sensorfuncs = self.db.getsensorfunctions(match)
        sensorvars = {}
        for func in sensorfuncs:
            if updateditem and (func.Sensor.id == id):
                str(value)
                try:
                    value = float(value)
                except:
                    pass
                sensorvars[str(func.id)] = value
            else:
                dictval = sensorops.operation(func)
                try:
                    dictval = float(dictval)
                except:
                    pass
                sensorvars[str(func.id)] = dictval 
        triggerfuncs = [func for func in self.db.gettriggerfunctions(match)]
        #TODO: add function dict

        def _convtriggervar(trigger):
            value = trigger.last()
            try:
                value = float(value)
            except:
                pass
            return value
        triggervars = {str(func.id):
                           _convtriggervar(func.Trigger)
                                for func in triggerfuncs}
        return (sensorvars, triggervars)

    def resolve(self, match, updateditem=None):
        '''
        resolves a match database object to a value.
         - match: the match object to resolve
         - updateditem: the sensor that triggered the trigger. needed when the sensor is a oneshot sensor.
        '''
        sensvars, trigvars = self._fetchvars(match, updateditem)
        tree = self.grammarobj.parse(match.Matchstring)
        return Calc().transform(tree, sensvars, trigvars)


class Calc(STransformer):
    '''
    STransform class to resolve ASTrees from the parser.
    '''
    def transform(self, tree, sensvars=None, trigvars=None):
        self.sensvars = sensvars
        self.trigvars = trigvars
        return str(float(STransformer.transform(self, tree)))

    def _bin_operator(self, exp):
        arg1, operator_symbol, arg2 = exp.tail
        print(exp.tail)
        operator_func = { '+': op.add,
                          '-': op.sub,
                          '*': op.mul,
                          '/': op.truediv,
                          '//': op.floordiv,
                          '%': op.mod,
                          '**': op.pow,
                          '==': op.eq,
                          '!=': op.ne,
                          '<=': op.le,
                          '>=': op.ge,
                          '<' : op.lt,
                          '>' : op.gt,
                          '||': lambda arg1, arg2: float(op.truth(arg1) or op.truth(arg2)),
                          '&&': lambda arg1, arg2: float(op.truth(arg1) and op.truth(arg2))}[operator_symbol]

        return operator_func(arg1, arg2)

    def _sens_operator(self, exp):
        sensorid = exp.tail[0][6:-2]
        return self.sensvars.get(sensorid,0)

    def _trig_operator(self, exp):
        triggerid = exp.tail[0][6:-2]
        return self.trigvars.get(triggerid,0)
    
    def _macr_operator(self, exp):
        return 4        #random dice roll
    
    number      = lambda self, exp: float(exp.tail[0])
    true_symbol = lambda self, exp: float(1)
    false_symbol = lambda self, exp: float(0)
    neg         = lambda self, exp: op.neg(exp.tail[0])
    __default__ = lambda self, exp: exp.tail[0]
    sensor      = _sens_operator
    trigger     = _trig_operator
    macro       = _macr_operator
    string      = lambda self, exp: str(exp.tail[0][1:-1])
    parenthesis = lambda self, exp: exp.tail[1]
    
    add = _bin_operator
    mul = _bin_operator
    exp = _bin_operator
    eql = _bin_operator
    lgc = _bin_operator

