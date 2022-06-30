#! /usr/bin/env python3

import random
import re
import socket
import threading
import time

import psutil

re_ip = re.compile(r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}")
a = re_ip.match("d1.1.1.1.35")
a = a.group(0)

print(a)
