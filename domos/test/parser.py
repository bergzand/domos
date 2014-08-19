#!/usr/bin/env python3
import operator as op
from plyplus import Grammar, STransformer
import unittest

GRAMMAR = """
    start: eql;             // This is the top of the hierarchy
    ?eql: ( eql eql_symbol )? add;
    ?add: ( add add_symbol )? mul;
    ?mul: ( mul mul_symbol )? exp;
    ?exp: ( exp exp_symbol )? atom;
    @atom: neg | number | sensor | true_symbol | false_symbol | '\(' add '\)';
    neg: '-' atom;
    true_symbol: 'True' | 'true' | 'Yes' | 'yes';
    false_symbol: 'False' | 'false' | 'No' | 'no';
    number: '[\d.]+';       // Regular expression for a decimal number
    sensor: '{[\w.\(\)]+}';   // Sensor Macro match
    mul_symbol: '\*' | '/' | '%'; // Match * or / or %
    add_symbol: '\+' | '-'; // Match + or -
    exp_symbol: '\*\*';
    eql_symbol: '==' | '<=' | '!=' | '>=' |'<' | '>';
    WHITESPACE: '[ \t]+' (%ignore);
"""

TESTS = [
    ('2**4*3+5', 53),   # Order of operation test
    ('2*(3+5)', 16),    # Order of operation test
    ]
class Calc(STransformer):

    def _bin_operator(self, exp):
        print(exp.tail)
        arg1, operator_symbol, arg2 = exp.tail

        operator_func = { '+': op.add,
                          '-': op.sub,
                          '*': op.mul,
                          '/': op.truediv,
                          '%': op.mod,
                          '**': op.pow,
                          '==': op.eq,
                          '!=': op.ne,
                          '<=': op.le,
                          '>=': op.ge,
                          '<' : op.lt,
                          '>' : op.gt}[operator_symbol]

        return operator_func(arg1, arg2)

    number      = lambda self, exp: float(exp.tail[0])
    true_symbol = lambda self, exp: True
    false_symbol = lambda self, exp: False
    neg         = lambda self, exp: op.neg(exp.tail[0])
    __default__ = lambda self, exp: exp.tail[0]
    sensor      = lambda self, exp: float(2)
    
    
    add = _bin_operator
    mul = _bin_operator
    exp = _bin_operator
    eql = _bin_operator

class ParserTest(unittest.TestCase):
    
    def setUp(self):
        self.g = Grammar(GRAMMAR)
    
    def test_grammar(self):
        for rule, expect in TESTS:
            with self.subTest(i=rule):
                result = Calc().transform(self.g.parse(rule))
                self.assertEqual(result, expect)

if __name__ == '__main__':
    unittest.main()