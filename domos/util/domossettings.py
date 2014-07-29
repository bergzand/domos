SYSNAME = 'domos'
EXCHANGE = 'domos'
exchange = dict(name=EXCHANGE, type='topic', durable=False, auto_delete=True, internal=False)
AMQP_URI = 'amqp://'

CORE = "domoscore"

LOGNAME = 'log'
LOG_DEBUG_TOPIC = 'log.DEBUG'
LOG_INFO_TOPIC = 'log.info'
LOG_WARN_TOPIC = 'log.warn'
LOG_ERROR_TOPIC = 'log.error'
LOG_CRIT_TOPIC = 'log.critical'

KEY_DEF = 'key'
IDENT_DEF = 'ident'
TYPE_DEF = 'type'
VALUE_DEF = 'value'

TYPE_RANGE = 'toggle'
TYPE_SINGLE = 'single'
