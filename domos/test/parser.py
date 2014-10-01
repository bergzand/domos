#!/usr/bin/env python3
from domos.util.trigger import *
import unittest
import pprint

TESTS = [
    ('2**4*3+5', '53.0'),  # Order of operation test
    ('2*(3+5)', '16.0'),  # Order of operation test
    ('13//4', '3.0'),  # floordiv test
    ('5 >= 5', '1.0'),  # inequality test
    ('True && False', '0.0'),
    ('2 && 6', '1.0'),
    ('2 || False', '1.0'),
    ('3 == 5', '0.0'),
    ('__sens3532__', '3.0'),
    ('__trig23__', '2.0'),
    ('"test" == "test"', '1.0'),  # string test
    ('"5" == 5', '0.0')
]


class ParserTest(unittest.TestCase):
    def setUp(self):
        self.g = Grammar(GRAMMAR)
        self.sensorvars = {"3532": 3}
        self.triggervars = {"23": 2}

    def test_grammar(self):
        for rule, expect in TESTS:
            with self.subTest(i=rule):
                print("Parsing: \"", rule, "\"", sep='')
                tree = self.g.parse(rule)
                print(tree.pretty())
                stree = tree.select('sensor *')
                print(stree, len(stree))
                for i in range(len(stree)):
                    print(stree[i - 1])
                result = Calc().transform(tree, sensvars=self.sensorvars, trigvars=self.triggervars)
                print("result:", result)
                self.assertEqual(result, expect)


def parsertest():
    unittest.main()