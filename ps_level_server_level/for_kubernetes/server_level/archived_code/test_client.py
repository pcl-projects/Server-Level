#! /usr/bin/env python3

import socket
import sys
import threading
import time


class SocketHandler:
    def __init__(self, socket):
        self.closed = False
        self.rv_buffer = ""
        self.socket = socket
        self.data = None

    def recv_line(self):
        if self.closed:
            return None
        self.rv_buffer = self.rv_buffer.lstrip("\n")
        i = self.rv_buffer.find("\n")
        if i >= 0:
            rt = self.rv_buffer[:i]
            self.rv_buffer = self.rv_buffer[i + 1 :]
            return rt
        else:
            while i < 0:
                new_string = self.socket.recv(1024).decode()
                if new_string == "":
                    self.closed = True
                    return None
                self.rv_buffer = self.rv_buffer + new_string
                i = self.rv_buffer.find("\n")
            rt = self.rv_buffer[:i]
            self.rv_buffer = self.rv_buffer[i + 1 :]
            return rt

    def send_line(self, string):
        if self.closed:
            return None
        s = string + "\n"
        self.socket.sendall(s.encode())

    def close(self):
        self.socket.close()
        self.closed = True


def server1_job1_ps1():
    sock = None
    while True:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect(("127.0.0.1", 20001))  # 尝试连接服务端
            break
        except Exception as e:
            print("Server not found or not open!", e)
            time.sleep(5)
    sh = SocketHandler(sock)
    while True:
        sh.send_line("PS | job1 | 1 | 20 | 0")
        time.sleep(2)


def server1_job2_ps1():
    sock = None
    while True:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect(("127.0.0.1", 20001))  # 尝试连接服务端
            break
        except Exception as e:
            print("Server not found or not open!", e)
            time.sleep(5)
    sh = SocketHandler(sock)
    while True:
        sh.send_line("PS | job2 | 1 | 30 | 0")
        time.sleep(2)


def server2_job1_ps2():
    sock = None
    while True:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect(("127.0.0.1", 20002))  # 尝试连接服务端
            break
        except Exception as e:
            print("Server not found or not open!", e)
            time.sleep(5)
    sh = SocketHandler(sock)
    for i in range(5):
        sh.send_line("PS | job1 | 2 | 250 | 0")
        time.sleep(2)
    while True:
        sh.send_line("PS | job1 | 2 | 250 | 1")
        time.sleep(2)


def server2_job2_ps2():
    sock = None
    while True:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect(("127.0.0.1", 20002))  # 尝试连接服务端
            break
        except Exception as e:
            print("Server not found or not open!", e)
            time.sleep(5)
    sh = SocketHandler(sock)
    for i in range(4):
        sh.send_line("PS | job2 | 2 | 200 | 0")
        time.sleep(2)
    while True:
        sh.send_line("PS | job2 | 2 | 200 | 1")
        time.sleep(2)


def server3_job1_ps3():
    sock = None
    while True:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect(("127.0.0.1", 20003))  # 尝试连接服务端
            break
        except Exception as e:
            print("Server not found or not open!", e)
            time.sleep(5)
    sh = SocketHandler(sock)
    while True:
        sh.send_line("PS | job1 | 3 | 5 | 0")
        time.sleep(2)


def server3_job2_ps3():
    sock = None
    while True:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect(("127.0.0.1", 20003))  # 尝试连接服务端
            break
        except Exception as e:
            print("Server not found or not open!", e)
            time.sleep(5)
    sh = SocketHandler(sock)
    while True:
        sh.send_line("PS | job2 | 3 | -100 | 0")
        time.sleep(2)


if __name__ == "__main__":
    t1 = threading.Thread(target=server1_job1_ps1)
    t2 = threading.Thread(target=server1_job2_ps1)
    t3 = threading.Thread(target=server2_job1_ps2)
    t4 = threading.Thread(target=server2_job2_ps2)
    t5 = threading.Thread(target=server3_job1_ps3)
    t6 = threading.Thread(target=server3_job2_ps3)
    t1.start()
    t2.start()
    t3.start()
    t4.start()
    t5.start()
    t6.start()
    t1.join()
