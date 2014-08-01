#!/usr/bin/env python

import socket
import sys
from threading import Thread
from domos import domos

if __name__ == "__main__":
    dom = domos()
    dom.main()