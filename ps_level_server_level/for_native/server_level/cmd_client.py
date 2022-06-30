#! /usr/bin/env python3


# from sklearn.cluster import KMeans
# import pickle
# from keras.models import load_model
# import random
import socket
import subprocess
from multiprocessing import Process

# import numpy as np
# import pandas as pd
from zeyu_utils import net as znet

ClientSocket = socket.socket()
# host = 'udc-aw29-24a.hpc.virginia.edu'
launcher_host = "udc-ba25-27.hpc.virginia.edu"
launcher_port = 41287


def f(command):
    subprocess.call(command, shell=True)


print("Waiting for connection from s")
ClientSocket = znet.SocketMsger.tcp_connect(launcher_host, launcher_port)


# Response = ClientSocket.recv(1024)
while True:
    # Input = input('Say Something: ')
    # ClientSocket.send(str.encode(Input))
    Response = ClientSocket.recv()
    if Response is None:
        print("CMD Client Ends")
        break
    # print(Response.decode('utf-8'))
    command = Response
    p = Process(target=f, args=(command,))
    p.start()

ClientSocket.close()
