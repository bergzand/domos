import operator as op
from plyplus import Grammar, STransformer
import threading
import queue as qu
import domos.util.domossettings as ds
from domos.util.domossettings import domosSettings
from domos.util.db import *
import peewee
import pprint


class triggerChecker(threading.Thread):
    
    GRAMMAR = """
        start: lgc;             // This is the top of the hierarchy
        ?lgc: ( lgc lgc_symbol )? eql;
        ?eql: ( eql eql_symbol )? add;
        ?add: ( add add_symbol )? mul;
        ?mul: ( mul mul_symbol )? exp;
        ?exp: ( exp exp_symbol )? atom;
        @atom: neg | number | sensor | trigger | true_symbol | false_symbol | '\(' add '\)';
        neg: '-' atom;
        true_symbol: 'True' | 'true' | 'Yes' | 'yes';
        false_symbol: 'False' | 'false' | 'No' | 'no';
        number: '[\d.]+';       // Regular expression for a decimal number
        sensor: '__sens\d+__';   // Sensor db macro match
        trigger: '__trig\d+__'; //trigger db macro match
        mul_symbol: '\*' | '/' | '//' | '%'; // Match * or / or %
        add_symbol: '\+' | '-'; // Match + or -
        exp_symbol: '\*\*';
        eql_symbol: '==' | '<=' | '!=' | '>=' |'<' | '>';
        lgc_symbol: '\|\|' | '\&\&';
        bit_symbol: '\|' | '\&' | '\^';
        WHITESPACE: '[ \t]+' (%ignore);
    """

    def __init__(self, queue=None, logger=None):
        threading.Thread.__init__(self)
        self.shutdown = False
        if logger:
            self.logger = logger
        else:
            self.logger.log_debug = lambda self, msg: None
            self.logger.log_info = lambda self, msg: None
            self.logger.log_warn = lambda self, msg: None
            self.logger.log_error = lambda self, msg: None
            self.logger.log_crit = lambda self, msg: None
        self.logger.log_debug("Initializing trigger checker thread")
        self.grammar = Grammar(triggerChecker.GRAMMAR)
        if not queue:
            self.q = qu.Queue()
        else:
            self.q = queue
        try:
            self.db = dbhandler()
            self.db.connect()

        except:
            self.logger.log_crit("Could not connect to database, shutting down")
            self.shutdown = True

    def getqueue(self):
        return self.q

    def _checktrigger(self, trigger, item):
        print(item)
        type, id, value = item
        sensorfuncs = self.db.getsensorfunctions(trigger)
        sensorvars = {}
        for func in sensorfuncs:
            if func.Sensor.id == id:
                sensorvars[str(func.id)] = value
            else:
                sensorvars[str(func.id)] = func.Sensor.last()['Value']
        triggerfuncs = self.db.gettriggerfunctions(trigger)
        #TODO: add function dict

        triggervars = {str(func.id): func.UsedTrigger.last() for func in triggerfuncs}
        tree = self.grammar.parse(trigger.Trigger)
        print(tree.pretty())
        triggervalue = Calc().transform(tree, sensvars=sensorvars, trigvars=triggervars)
        print("New:",triggervalue,"old:",trigger.Lastvalue)
        if trigger.Lastvalue != triggervalue:
            self.logger.log_debug("Trigger {0} now has value {1}".format(trigger.id, triggervalue))
            self.db.addTriggervalue(trigger, triggervalue)
            self.q.put(("trigger", trigger.id, triggervalue))

    def processitem(self, item):
        type, id, value = item
        self.logger.log_debug("Receiving item message")
        if type == "sensor":
            triggers = self.db.gettriggersfromsensor(id)
            self.logger.log_debug("Found {0} triggers associated with posted sensor: {1}".format(len(triggers), id))
        elif type == "trigger":
            triggers = self.db.gettriggersfromtrigger(id)
            self.logger.log_debug("Found {0} triggers associated with posted trigger: {1}".format(len(triggers),id))
        for trigger in triggers:
            self._checktrigger(trigger, item)

    def run(self):
        while not self.shutdown:
            try:
                item = self.q.get(timeout=2)
                print("found item")
                self.processitem(item)
            except qu.Empty:
                pass


class Calc(STransformer):
    def transform(self, tree, sensvars=None, trigvars=None):
        self.sensvars = sensvars
        self.trigvars = trigvars
        return str(STransformer.transform(self, tree))
    
    def _bin_operator(self, exp):
        arg1, operator_symbol, arg2 = exp.tail

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
        return float(self.sensvars.get(sensorid,0))

    def _trig_operator(self, exp):
        triggerid = exp.tail[0][6:-2]
        return float(self.trigvars.get(triggerid,0))
    
    number      = lambda self, exp: float(exp.tail[0])
    true_symbol = lambda self, exp: float(1)
    false_symbol = lambda self, exp: float(0)
    neg         = lambda self, exp: op.neg(exp.tail[0])
    __default__ = lambda self, exp: exp.tail[0]
    sensor      = _sens_operator
    trigger     = _trig_operator
    
    
    add = _bin_operator
    mul = _bin_operator
    exp = _bin_operator
    eql = _bin_operator
    lgc = _bin_operator

