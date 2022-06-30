import argparse
import collections
import logging
import os
import pickle
import random
import socket
import sys
import threading
import time
from typing import Dict

import numpy as np
import torch
import torch.distributed.rpc as rpc
import torch.nn as nn
from model import alexnet, densenet, googlenet, mobilenetv2, resnet3, vgg
from torch.utils.data import DataLoader
from torchvision import datasets, models, transforms

rand_seed = 123
torch.manual_seed(rand_seed)
torch.cuda.manual_seed(rand_seed)
torch.cuda.manual_seed_all(rand_seed)
np.random.seed(rand_seed)
random.seed(rand_seed)
# torch.backends.cudnn.enabled = False
torch.backends.cudnn.benchmark = False
torch.backends.cudnn.deterministic = True


launcher_address = "udc-ba25-27.hpc.virginia.edu"
launcher_port = 54333


class SocketMsger:
    def __init__(self, socket, is_listener=False):
        self.piggyback_data = None
        self.__socket = socket
        self.__is_listener = is_listener
        self.__is_blocking = True
        self.__recv_buffer = b""
        self.__closed = False

    @property
    def socket(self):
        return self.__socket

    @property
    def is_listener(self):
        return self.__is_listener

    @property
    def is_blocking(self):
        return self.__is_blocking

    @property
    def closed(self):
        if getattr(self.__socket, "_closed") is True and self.__closed is False:
            self.__closed = True
        return self.__closed

    def send(self, data):
        if self.__closed or self.__is_listener:
            return
        if isinstance(data, str):
            data_type = 0
            byte_data = data.encode()
        elif isinstance(data, bytes):
            data_type = 1
            byte_data = data
        else:
            data_type = 2
            byte_data = pickle.dumps(data, protocol=pickle.HIGHEST_PROTOCOL)
        data_length = len(byte_data)
        self.__socket.sendall(f"META({data_type},{data_length})".encode() + byte_data)

    def recv(self, blocking=True):
        if self.__closed or self.__is_listener:
            return
        if blocking:
            if not self.__is_blocking:
                self.__socket.setblocking(True)
                self.__is_blocking = True
        else:
            if self.__is_blocking:
                self.__socket.setblocking(False)
                self.__is_blocking = False
        index = self.__recv_buffer.find(b"META(")
        while index == -1:
            try:
                data = self.__socket.recv(1024)
                if data == b"":
                    self.__closed = True
                    return
                self.__recv_buffer += data
                index = self.__recv_buffer.find(b"META(")
            except BlockingIOError:
                return
        meta_lindex = index + 5
        index = self.__recv_buffer.find(b")", meta_lindex)
        while index == -1:
            try:
                data = self.__socket.recv(1024)
                if data == b"":
                    self.__closed = True
                    return
                self.__recv_buffer += data
                index = self.__recv_buffer.find(b")", meta_lindex)
            except BlockingIOError:
                return
        meta_rindex = index
        meta = self.__recv_buffer[meta_lindex:meta_rindex].split(b",")
        data_type = int(meta[0])
        data_length = int(meta[1])
        body_lindex = meta_rindex + 1
        while len(self.__recv_buffer) - body_lindex < data_length:
            try:
                data = self.__socket.recv(1024)
                if data == b"":
                    self.__closed = True
                    return
                self.__recv_buffer += data
            except BlockingIOError:
                return
        body_rindex = body_lindex + data_length
        recvd_data = self.__recv_buffer[body_lindex:body_rindex]
        self.__recv_buffer = self.__recv_buffer[body_rindex:]
        if data_type == 0:
            return recvd_data.decode()
        elif data_type == 1:
            return recvd_data
        else:
            return pickle.loads(recvd_data)

    def close(self):
        self.__socket.close()
        self.__closed = True

    @staticmethod
    def tcp_listener(listening_ip, listening_port, backlog=100):
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind((listening_ip, listening_port))
        listener.listen(backlog)
        return SocketMsger(listener, True)

    def accept(self):
        if self.__is_listener:
            conn, address = self.__socket.accept()
            connm = SocketMsger(conn)
            return connm, address

    @staticmethod
    def tcp_connect(ip, port, retry=True):
        sock = None
        while True:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                sock.connect((ip, port))
                return SocketMsger(sock)
            except Exception as e:
                print("NOT CONNECTED:", e, file=sys.stderr)
                if not retry:
                    return
                time.sleep(1)


class Logger(object):
    def __init__(self, job_name, file_dir, log_level=logging.INFO, mode="w"):
        self.logger = logging.getLogger(job_name)
        self.logger.setLevel(log_level)
        self.formatter = logging.Formatter("%(asctime)s - %(name)s - %(message)s")

        self.fh = logging.FileHandler(filename=file_dir, mode=mode)
        self.fh.setFormatter(self.formatter)
        self.logger.addHandler(self.fh)

        # self.ch = logging.StreamHandler()
        # self.ch.setFormatter(self.formatter)
        # self.logger.addHandler(self.ch)


class ParameterServer(object):
    def __init__(self, job_name, scheduler_rref, num_ps, num_idle_ps, num_workers, id, worker_rrefs, lr):
        self.logger = Logger(job_name=job_name, file_dir=f"./logs/{job_name}_ps{id}.log").logger
        self.job_name = job_name
        self.num_ps = num_ps
        self.num_workers = num_workers
        self.num_idle_ps = num_idle_ps
        self.lock = threading.Lock()
        self.cm_t1_start = np.zeros(num_workers)
        self.future_model = torch.futures.Future()
        self.batch_update_size = num_workers
        self.curr_update_size = 0
        self.id = id
        self.scheduler_rref = scheduler_rref
        self.ps_rref = rpc.RRef(self)
        self.worker_rrefs = worker_rrefs
        self.para_dict = dict()
        self.new_para_dict = dict()
        self.para_location = {}
        self.num_para = 0
        self.para_size = 0.0
        self.lr = lr
        self.cp_time = collections.deque(maxlen=5)
        self.comm_time_list = np.zeros(num_workers)
        self.grad_add_time_list = np.zeros(num_workers)
        self.pred_time = np.zeros(num_workers)
        self.sned_straggler_flag = 1
        self.send_para_count = 0
        self.comm_with_res_manager_count = 0
        self.get_para_dict_count = 0
        self.reassign_flag = 0
        self.grad_para_time_0 = time.time()

        self.res_manager_connm = SocketMsger.tcp_connect("127.0.0.1", 20103)

        self.res_stats = None

    def get_res_stats(ps_rref):
        self = ps_rref.local_value()
        return self.res_stats

    def get_para_dict(ps_rref, para_dict: dict, para_location):
        self = ps_rref.local_value()
        with self.lock:
            self.get_para_dict_count += 1
            if self.get_para_dict_count == self.num_workers:
                self.get_para_dict_count = 0
            if self.get_para_dict_count == 1:
                self.para_dict = para_dict
                self.para_location = {}

                for key in self.para_dict:
                    self.para_location[key] = self.id
                    self.para_dict[key].grad = torch.zeros_like(self.para_dict[key])
                    self.num_para += self.para_dict[key].numel()

                self.para_size = 32 * self.num_para / 1024 / 1024

    def get_para(ps_rref, keyvalues):
        self = ps_rref.local_value()
        with self.lock:
            for key, value in keyvalues.items():
                self.num_para += value.numel()
                self.para_dict[key] = value
                self.para_dict[key].grad = torch.zeros_like(value)
                self.para_location[key] = self.id

    def send_para(ps_rref, ps_rrefs, target_ps_info: Dict[int, float]):
        self: ParameterServer = ps_rref.local_value()
        for target_ps_id in target_ps_info:
            trg_ps = target_ps_id
            para_size = target_ps_info[trg_ps]
            num_para = para_size * 1024 * 1024 / 32
            sorted_para_list = sorted(self.para_dict.items(), key=lambda x: x[1].numel(), reverse=True)
            keyvalues = {}
            for key, paras in sorted_para_list:
                if num_para > 0:
                    numel = paras.numel()
                    num_para -= numel
                    self.num_para -= numel
                    keyvalues[key] = paras
                    # rpc.rpc_sync(
                    #     to=ps_rrefs[trg_ps].owner(),
                    #     func=ParameterServer.get_para,
                    #     args=(ps_rrefs[trg_ps], key, self.para_dict[key]),
                    # )
                    del self.para_dict[key]
                    del self.para_location[key]
            rpc.rpc_sync(to=ps_rrefs[trg_ps].owner(), func=ParameterServer.get_para, args=(ps_rrefs[trg_ps], keyvalues))

    def send_para_location(ps_rref):
        self = ps_rref.local_value()
        return self.para_location, self.num_para

    def comm_with_res_manager(ps_rref):
        self = ps_rref.local_value()
        with self.lock:
            self.comm_with_res_manager_count += 1
            if self.comm_with_res_manager_count == self.num_workers:
                self.comm_with_res_manager_count = 0
            if self.comm_with_res_manager_count == 1:
                job_name = self.job_name
                rank = self.id
                para_num = self.num_para
                self.para_size = float(32 * self.num_para / 1024 / 1024)
                para_size = self.para_size
                self.res_manager_connm.send({"job_name": job_name, "rank": rank, "para_num": para_num, "para_size": para_size})
                if not para_num > 0:
                    self.reassign_flag = 0
                    recvd_data = self.res_manager_connm.recv()
                    assert isinstance(recvd_data, dict)
                    self.res_stats = recvd_data
                    return self.reassign_flag
                recvd_data = self.res_manager_connm.recv()
                assert isinstance(recvd_data, dict)
                self.res_stats = recvd_data
                bd_s = recvd_data["bandwd"][0]
                cpu_s = recvd_data["cpu"][0]
                straggling_degree = max(bd_s, cpu_s)
                if straggling_degree > 1.5:
                    self.logger.info(f"PS{self.id} becomes a straggler")
                    self.reassign_flag = 1
                else:
                    self.reassign_flag = 0
            return self.reassign_flag

    @staticmethod
    @rpc.functions.async_execution
    def update_and_fetch_model(ps_rref, grad_dict, worker_id, epoch, batch_idx, comm_time_0):
        self: ParameterServer = ps_rref.local_value()
        self.comm_time_list[worker_id] = (time.time() - comm_time_0) * 1000
        # logging.info(f"PS received gradients from Worker{worker_id}")

        with self.lock:
            if self.curr_update_size == 0:
                self.grad_para_time_0 = time.time()

            grad_add_time_0 = time.time()
            for key in self.para_dict:
                self.para_dict[key].grad += grad_dict[key]
            self.grad_add_time_list[worker_id] = 1000 * (time.time() - grad_add_time_0)
            # t2 = time.time()
            # logging.info('Time for adding gradients: {:6.2f} ms'.format(1000 * (t2 - t1)))

            self.curr_update_size += 1
            fut = self.future_model

            if self.curr_update_size >= self.batch_update_size:
                self.curr_update_size = 0

                log_str = ""
                for w_id in range(self.num_workers):
                    log_str += f"Worker{w_id} to PS{self.id} communication time {self.comm_time_list[w_id]:.3f} ms\n"
                self.logger.info(log_str)

                log_str = ""
                for w_id in range(self.num_workers):
                    log_str += f"Worker{w_id} grad add time {self.grad_add_time_list[w_id]:.3f} ms\n"
                self.logger.info(log_str)

                para_update_time_0 = time.time()
                for key in self.para_dict:
                    self.para_dict[key].grad /= self.batch_update_size
                    self.para_dict[key] = self.para_dict[key] + self.para_dict[key].grad * -self.lr
                    self.para_dict[key].grad = torch.zeros_like(self.para_dict[key])
                self.logger.info(f"Para update time {(1000 * (time.time() - para_update_time_0)):.3f} ms.")

                grad_para_time_1 = time.time()
                grad_para_time = 1000 * (grad_para_time_1 - self.grad_para_time_0)
                self.logger.info(f"Gradient computation, waiting, and para updating time is {grad_para_time} ms.")

                # msg = f"PS | {self.job_name} | {self.rank} | {self.para_size}\0"
                # tt0 = time.time()
                # self.agent_conn.send(msg.encode("utf-8"))
                # while True:
                #     try:
                #         resource = self.agent_conn.recv(102400)
                #     except BlockingIOError as e:
                #         resource = None

                #     if resource is not None:
                #         break

                # tt1 = time.time()
                # self.logger.info("Time with agent: {:6.2f}".format(1000 * (tt1 - tt0)))
                # resource = resource.spilt("|")
                # bd_d = resource[0]
                # bd_r = resource[1]
                # cpu_d = resource[2]
                # cpu_r = resource[3]
                # bd_s = bd_d / bd_r
                # cpu_s = cpu_d / cpu_r
                # bd_l = bd_r - bd_d
                # cpu_l = cpu_r - cpu_d
                # s = min(bd_s, cpu_s)
                # if s > 1.5:
                #     self.logger.info(f"PS{self.id} becomes a straggler")
                #     if self.send_straggler_flag == 0:
                #         msg = f"PS | {self.job_name} | {self.rank} | {bd_d} | {bd_r} | {cpu_d} | {cpu_r}\0"
                #         self.scheduler_conn.send(msg.encode("utf-8"))
                #         self.send_straggler_flag = 1

                # try:
                #     result = self.scheduler_conn.recv(102400)
                # except BlockingIOError as e:
                #     result = None

                # self.reassign_flag = 0
                # if result is not None:
                #     self.send_straggler_flag = 0
                #     self.reassign_flag = 1
                #     self.assign_msg = result

                # t3 = time.time()
                # self.cp_time.append(t3 - t2)

                # for i in range(self.num_workers):
                #     self.cm_timestamp[i] += -t3
                # logging.info('Time for updating: {:6.2f} ms'.format(1000 * (t3 - t2)))

                # self.logger.info("PS{:2d} | Epoch: {:4d}| Batch: {:4d}".format(self.id, (epoch + 1), (batch_idx + 1)))

                self.logger.info("PS sending updated parameters to workers.")

                fut.set_result([self.para_dict, time.time(), float(32 * self.num_para / 1024 / 1024)])
                self.future_model = torch.futures.Future()

        return fut


class Worker(object):
    def __init__(
        self,
        job_name,
        scheduler_rref,
        num_ps,
        num_idle_ps,
        num_workers,
        model,
        id,
        data_dir,
        batch_size,
        num_epochs,
        para_location,
    ):
        self.logger = Logger(job_name=job_name, file_dir=f"./logs/{job_name}_worker{id}.log").logger
        self.id = id
        self.num_ps = num_ps
        self.num_idle_ps = num_idle_ps
        self.num_workers = num_workers
        self.active_ps = [i for i in range(num_ps)]
        if model == "resnet20":
            self.model = resnet3.resnet20()
            self.test_model = resnet3.resnet20()
        elif model == "resnet56":
            self.model = resnet3.resnet56()
            self.test_model = resnet3.resnet56()
        elif model == "vgg13":
            self.model = vgg.VGG13()
            self.test_model = vgg.VGG13()
        elif model == "vgg16":
            self.model = vgg.VGG16()
            self.test_model = vgg.VGG16()
        elif model == "densenet121":
            self.model = densenet.DenseNet121()
            self.test_model = densenet.DenseNet121()
        elif model == "alexnet":
            self.model = alexnet.AlexNet()
            self.test_model = alexnet.AlexNet()
        elif model == "googlenet":
            self.model = googlenet.GoogLeNet()
            self.test_model = googlenet.GoogLeNet()
        elif model == "mobilenet":
            self.model = mobilenetv2.MobileNetV2()
            self.test_model = mobilenetv2.MobileNetV2()

        self.model_size = sum(p.numel() for p in self.model.parameters())
        self.worker_rref = rpc.RRef(self)
        self.ps_rrefs = [None for _ in range(num_ps + num_idle_ps)]
        self.worker_rrefs = None  # Set when calling run_worker()
        self.scheduler_rref = scheduler_rref
        self.para_dict_list = [dict() for _ in range(self.num_ps + self.num_idle_ps)]
        self.para_location = para_location
        self.new_para_location = [dict() for _ in range(self.num_ps + self.num_idle_ps)]
        self.cm_time_t0 = np.zeros(self.num_ps + self.num_idle_ps)
        self.cm_time_t1 = np.zeros(self.num_ps + self.num_idle_ps)
        self.cm_time = np.zeros(self.num_ps + self.num_idle_ps)
        self.pred_time = np.zeros(self.num_ps + self.num_idle_ps)
        self.data_dir = data_dir
        self.num_epochs = num_epochs
        self.batch_size = batch_size
        self.straggler_flag = 0
        self.reassigned_flag = 0
        self.reassign_flag = np.zeros(self.num_ps + self.num_idle_ps)
        self.stop_flag = False
        self.num_para = np.zeros(self.num_ps + self.num_idle_ps)
        # For para reassignment sync among workers, used by worker0 only
        self.para_reassign_complete_future = torch.futures.Future()
        self.ps_res_stats = {}
        self.lock = threading.Lock()
        self.para_reassign_count = 0

    # For para reassignment sync among workers, used by all workers except worker0
    @staticmethod
    @rpc.functions.async_execution
    def sync_para_reassign(worker_rref):
        self: Worker = worker_rref.local_value()
        fut = self.para_reassign_complete_future
        with self.lock:
            self.para_reassign_count += 1
        return fut

    def get_stop_flag(self):
        return self.stop_flag

    def get_test_model_and_stop_flag(self):
        return self.test_model, self.stop_flag

    def get_res_stats(self, p):
        self.ps_res_stats[p] = rpc.rpc_sync(
            to=self.ps_rrefs[p].owner(), func=ParameterServer.get_res_stats, args=(self.ps_rrefs[p],)
        )

    def get_new_location(self, p):
        self.new_para_location[p], self.num_para[p] = rpc.rpc_sync(
            to=self.ps_rrefs[p].owner(), func=ParameterServer.send_para_location, args=(self.ps_rrefs[p],)
        )

    def reassign_para(self, p):
        rpc.rpc_sync(to=self.ps_rrefs[p].owner(), func=ParameterServer.send_para, args=(self.ps_rrefs[p], self.ps_rrefs))

    def comm_with_res_manager(self, p):
        self.reassign_flag[p] = rpc.rpc_sync(
            to=self.ps_rrefs[p].owner(), func=ParameterServer.comm_with_res_manager, args=(self.ps_rrefs[p],)
        )

    def push_pull(self, p, grad_dict_list, epoch, batch_idx):
        comm_time_0 = None
        para_size = None
        self.para_dict_list[p], comm_time_0, para_size = rpc.rpc_sync(
            to=self.ps_rrefs[p].owner(),
            func=ParameterServer.update_and_fetch_model,
            args=(self.ps_rrefs[p], grad_dict_list[p], self.id, epoch, batch_idx, time.time()),
        )
        comm_time = (time.time() - comm_time_0) * 1000
        self.logger.info(f"PS{p} to Worker{self.id} communication time is {comm_time:.3f} ms | Para size: {para_size:.3f} Mbit")

        # logging.info(f'receive from {p}')
        # self.cm_time_t1[p] = self.temp_time[self.id] + time.time()
        # # print(f'communication time with ps{p}: {1000 * self.cm_time_t1[p]}')
        # self.pred_time[p] = 1000 * (time.time() - self.cm_time_t0[p])
        # self.cm_time[p] = 1000 * self.cm_time_t1[p]

    def run_worker(worker_rref, ps_rrefs, worker_rrefs, gpu_id):
        self: Worker = worker_rref.local_value()
        self.ps_rrefs = ps_rrefs
        self.worker_rrefs = worker_rrefs

        transform = transforms.Compose(
            [
                transforms.RandomCrop(32, padding=4),
                transforms.RandomHorizontalFlip(),
                transforms.ToTensor(),
                transforms.Normalize((0.4914, 0.4822, 0.4465), (0.2023, 0.1994, 0.2010)),
            ]
        )
        train_dataset = datasets.CIFAR10(root=self.data_dir, train=True, download=True, transform=transform)
        train_loader = DataLoader(train_dataset, batch_size=self.batch_size, shuffle=True)

        device = torch.device(f"cuda:{gpu_id}" if torch.cuda.is_available() else "cpu")
        criterion = nn.CrossEntropyLoss()
        criterion = criterion.to(device)

        grad_dict_list = [dict() for _ in range(self.num_ps + self.num_idle_ps)]
        # self.model = self.model

        # Distribute parameters among active PSs
        for key, value in self.model.named_parameters():
            self.para_dict_list[self.para_location[key]][key] = value

        # Push assigned parameters to PSs
        for p in range(self.num_ps):
            rpc.rpc_sync(
                to=self.ps_rrefs[p].owner(),
                func=ParameterServer.get_para_dict,
                args=(self.ps_rrefs[p], self.para_dict_list[p], self.para_location),
            )

        total_time_0 = time.time()
        for epoch in range(self.num_epochs):
            for batch_idx, (data, target) in enumerate(train_loader):
                comp_time_0 = time.time()
                self.model = self.model.to(device)
                data, target = data.to(device), target.to(device)
                output = self.model(data)
                loss = criterion(output, target)
                loss.backward()
                self.model = self.model.cpu()
                comp_time_1 = time.time()
                comp_time = 1000 * (comp_time_1 - comp_time_0)
                self.logger.info(
                    "Worker{:1d} | Epoch: {:4d} | Batch: {:4d} | Loss: {:7.3f} | Computation Time: {:8.3f} ms".format(
                        self.id, (epoch + 1), (batch_idx + 1), loss.item(), comp_time
                    )
                )

                for key, value in self.model.named_parameters():
                    grad_dict_list[self.para_location[key]][key] = value.grad

                self.para_dict_list = [dict() for _ in range(self.num_ps + self.num_idle_ps)]
                self.thread_list = []
                for p in self.active_ps:
                    self.thread_list.append(threading.Thread(target=self.push_pull, args=(p, grad_dict_list, epoch, batch_idx)))
                for t in self.thread_list:
                    t.start()
                for t in self.thread_list:
                    t.join()

                overhead_time_start = time.time()
                thread_list = []
                for p in range(self.num_ps + self.num_idle_ps):
                    thread_list.append(threading.Thread(target=self.comm_with_res_manager, args=(p,)))
                for t in thread_list:
                    t.start()
                for t in thread_list:
                    t.join()

                # Do load-reassignment check
                self.reassigned_flag = 0
                ps_stragglers = []
                # self.thread_list = []
                for p in self.active_ps:
                    if self.reassign_flag[p] == 1:
                        self.reassigned_flag = 1
                        # self.thread_list.append(threading.Thread(target=self.reassign_para, args=(p,)))
                        ps_stragglers.append(p)
                # for t in self.thread_list:
                #     t.start()
                # for t in self.thread_list:
                #     t.join()

                para_reassign_time_0 = time.time()
                if self.reassigned_flag == 1:
                    # self.reassigned_flag = 0
                    if self.id == 0:
                        while self.para_reassign_count < self.num_workers - 1:
                            time.sleep(0.01)
                        self.para_reassign_count = 0

                        thread_list = []
                        for p in range(self.num_ps + self.num_idle_ps):
                            thread_list.append(threading.Thread(target=self.get_res_stats, args=(p,)))
                        for t in thread_list:
                            t.start()
                        for t in thread_list:
                            t.join()

                        log_str = "**********\n"
                        for key in self.ps_res_stats:
                            log_str += f"********** PS{key} Res Stats: {self.ps_res_stats[key]}\n"
                        log_str += "**********"
                        self.logger.info(log_str)

                        # ps_straggler_excess_load = []  # [[ps_id, excess_load]]
                        # {res_type: [[ps_id, ava_res]]} no ps straggler here
                        active_ps_available_res = {"bandwd": [], "cpu": []}
                        # {res_type: [[ps_id, ava_res]]}
                        idle_ps_available_res = {"bandwd": [], "cpu": []}
                        for p in self.active_ps:
                            if p not in ps_stragglers:
                                active_ps_available_res["bandwd"].append([p, self.ps_res_stats[p]["bandwd"][2]])
                                active_ps_available_res["cpu"].append([p, self.ps_res_stats[p]["cpu"][2]])
                        for p in range(self.num_ps + self.num_idle_ps):
                            if p not in self.active_ps:
                                idle_ps_available_res["bandwd"].append([p, self.ps_res_stats[p]["bandwd"][2]])
                                idle_ps_available_res["cpu"].append([p, self.ps_res_stats[p]["cpu"][2]])

                        decision_result = {}  # {straggler_ps_id: {target_ps_id: para_size_to_move}}
                        for p in ps_stragglers:
                            if self.ps_res_stats[p]["bandwd"][0] >= self.ps_res_stats[p]["cpu"][0]:
                                res_type = "bandwd"
                            else:
                                res_type = "cpu"
                            excess_load = self.ps_res_stats[p][res_type][1]
                            active_ps_available_res[res_type].sort(key=lambda x: x[1], reverse=True)
                            idle_ps_available_res[res_type].sort(key=lambda x: x[1], reverse=True)
                            target_ps = {}  # {ps_id: load_to_move}
                            for ps_id, ava_res in active_ps_available_res[res_type]:
                                if excess_load > ava_res:
                                    if ava_res > 0.0:
                                        target_ps[ps_id] = ava_res
                                        excess_load -= ava_res
                                        continue
                                    else:
                                        break
                                else:
                                    target_ps[ps_id] = excess_load
                                    excess_load = 0.0
                                    break
                            idle_to_active_ps_id = set()
                            if excess_load > 0.0:
                                for ps_id, ava_res in idle_ps_available_res[res_type]:
                                    if excess_load > ava_res:
                                        if ava_res > 0.0:
                                            target_ps[ps_id] = ava_res
                                            excess_load -= ava_res
                                            idle_to_active_ps_id.add(ps_id)
                                            continue
                                        else:
                                            break
                                    else:
                                        target_ps[ps_id] = excess_load
                                        excess_load = 0.0
                                        idle_to_active_ps_id.add(ps_id)
                                        break
                            # move idle ps to active ps
                            for ps_id in idle_to_active_ps_id:
                                for index in range(len(idle_ps_available_res["bandwd"])):
                                    p_id = idle_ps_available_res["bandwd"][index][0]
                                    if p_id == ps_id:
                                        active_ps_available_res["bandwd"].append(idle_ps_available_res["bandwd"].pop(index))
                                        break
                                for index in range(len(idle_ps_available_res["cpu"])):
                                    p_id = idle_ps_available_res["cpu"][index][0]
                                    if p_id == ps_id:
                                        active_ps_available_res["cpu"].append(idle_ps_available_res["cpu"].pop(index))
                                        break

                            # update active_ps_available_res and idle_ps_available_res
                            if res_type == "bandwd":
                                for i in range(len(active_ps_available_res["bandwd"])):
                                    ps_id = active_ps_available_res["bandwd"][i][0]
                                    ava_res = active_ps_available_res["bandwd"][i][1]
                                    if ps_id in target_ps:
                                        active_ps_available_res["bandwd"][i][1] = ava_res - target_ps[ps_id]
                                for i in range(len(active_ps_available_res["cpu"])):
                                    ps_id = active_ps_available_res["cpu"][i][0]
                                    ava_res = active_ps_available_res["cpu"][i][1]
                                    if ps_id in target_ps:
                                        active_ps_available_res["cpu"][i][1] = ava_res - target_ps[ps_id] / self.num_workers
                                for i in range(len(idle_ps_available_res["bandwd"])):
                                    ps_id = idle_ps_available_res["bandwd"][i][0]
                                    ava_res = idle_ps_available_res["bandwd"][i][1]
                                    if ps_id in target_ps:
                                        idle_ps_available_res["bandwd"][i][1] = ava_res - target_ps[ps_id]
                                for i in range(len(idle_ps_available_res["cpu"])):
                                    ps_id = idle_ps_available_res["cpu"][i][0]
                                    ava_res = idle_ps_available_res["cpu"][i][1]
                                    if ps_id in target_ps:
                                        idle_ps_available_res["cpu"][i][1] = ava_res - target_ps[ps_id] / self.num_workers
                            else:
                                for i in range(len(active_ps_available_res["bandwd"])):
                                    ps_id = active_ps_available_res["bandwd"][i][0]
                                    ava_res = active_ps_available_res["bandwd"][i][1]
                                    if ps_id in target_ps:
                                        active_ps_available_res["bandwd"][i][1] = ava_res - target_ps[ps_id] * self.num_workers
                                for i in range(len(active_ps_available_res["cpu"])):
                                    ps_id = active_ps_available_res["cpu"][i][0]
                                    ava_res = active_ps_available_res["cpu"][i][1]
                                    if ps_id in target_ps:
                                        active_ps_available_res["cpu"][i][1] = ava_res - target_ps[ps_id]
                                for i in range(len(idle_ps_available_res["bandwd"])):
                                    ps_id = idle_ps_available_res["bandwd"][i][0]
                                    ava_res = idle_ps_available_res["bandwd"][i][1]
                                    if ps_id in target_ps:
                                        idle_ps_available_res["bandwd"][i][1] = ava_res - target_ps[ps_id] * self.num_workers
                                for i in range(len(idle_ps_available_res["cpu"])):
                                    ps_id = idle_ps_available_res["cpu"][i][0]
                                    ava_res = idle_ps_available_res["cpu"][i][1]
                                    if ps_id in target_ps:
                                        idle_ps_available_res["cpu"][i][1] = ava_res - target_ps[ps_id]

                            # update target_ps load to para_size
                            if res_type == "bandwd":
                                for key in target_ps:
                                    target_ps[key] = target_ps[key] / self.num_workers

                            decision_result[p] = target_ps
                            log_str = f"Straggler PS{p}' target PSs: "
                            for ps_id in target_ps:
                                log_str += f"| Target PS{ps_id}, move {target_ps[ps_id]:.3f} Mbit para size | "
                            self.logger.info(log_str)

                        # Parameter reassignment
                        for p in decision_result:
                            rpc.rpc_sync(
                                to=self.ps_rrefs[p].owner(),
                                func=ParameterServer.send_para,
                                args=(self.ps_rrefs[p], self.ps_rrefs, decision_result[p]),
                            )

                        self.para_reassign_complete_future.set_result(True)
                        self.para_reassign_complete_future = torch.futures.Future()
                    else:
                        rpc.rpc_sync(
                            to=self.worker_rrefs[0].owner(), func=Worker.sync_para_reassign, args=(self.worker_rrefs[0],),
                        )

                    self.reassigned_flag = 0
                    grad_dict_list = [dict() for _ in range(self.num_ps + self.num_idle_ps)]
                    self.thread_list = []
                    for p in range(self.num_ps + self.num_idle_ps):
                        self.thread_list.append(threading.Thread(target=self.get_new_location, args=(p,)))
                    for t in self.thread_list:
                        t.start()
                    for t in self.thread_list:
                        t.join()

                    self.active_ps = []
                    self.para_location = dict()
                    for p in range(self.num_ps + self.num_idle_ps):
                        if self.num_para[p] > 0:
                            self.active_ps.append(p)
                        for key in self.new_para_location[p]:
                            self.para_location[key] = p

                if self.id == 0:
                    para_reassign_time = 1000 * (time.time() - overhead_time_start)
                    self.logger.info(f"Para reassign time {para_reassign_time:.4f} ms")

                # Update parameters by copying updated values to model parameters
                for i in self.para_dict_list:
                    for key in i:
                        self.model.state_dict()[key].copy_(i[key])
                        self.test_model.state_dict()[key].copy_(i[key])
                self.model.zero_grad()

        self.stop_flag = True
        total_time_1 = time.time()
        self.logger.info("Total Time: {:.3f} s".format((total_time_1 - total_time_0)))


class Scheduler(object):
    def __init__(self, job_name, num_ps, num_idle_ps, num_workers, model):
        self.logger = Logger(job_name=job_name, file_dir=f"./logs/{job_name}_scheduler.log").logger
        self.model_name = model
        self.num_ps = num_ps
        self.num_idle_ps = num_idle_ps
        self.num_workers = num_workers
        self.active_ps = [i for i in range(num_ps)]
        self.lock = threading.Lock()
        self.scheduler_rref = rpc.RRef(self)
        if model == "resnet20":
            self.model = resnet3.resnet20()
        elif model == "resnet56":
            self.model = resnet3.resnet56()
        elif model == "vgg13":
            self.model = vgg.VGG13()
        elif model == "vgg16":
            self.model = vgg.VGG16()
        elif model == "densenet121":
            self.model = densenet.DenseNet121()
        elif model == "alexnet":
            self.model = alexnet.AlexNet()
        elif model == "googlenet":
            self.model = googlenet.GoogLeNet()
        elif model == "mobilenet":
            self.model = mobilenetv2.MobileNetV2()

        self.ps_rrefs = [None for _ in range(num_ps + num_idle_ps)]
        self.worker_rrefs = [None for _ in range(num_workers)]
        self.num_ps_para = np.zeros(num_ps + num_idle_ps)

        self.para_location = dict()
        self.new_para_location = dict()
        self.para_dict_list = [dict() for _ in range(num_ps)]
        self.para_location = self.aver_assign()

    def round_robin(self):
        """
        assign parameters in a round-robin way
        """
        i = 0
        for key, value in self.model.named_parameters():
            # self.para_dict_list[i][key] = value
            self.num_ps_para[i] += value.numel()
            self.para_location[key] = i
            i += 1
            if i == self.num_ps:
                i = 0
        self.logger.info(self.num_ps_para)

        return self.para_location

    def aver_assign(self):
        para_dict = {}

        for k, v in self.model.named_parameters():
            para_dict[k] = v.numel()

        para_dict = sorted(para_dict.items(), key=lambda x: x[1], reverse=True)
        for j in range(len(para_dict)):
            i = np.argmin(self.num_ps_para[0 : self.num_ps])
            self.num_ps_para[i] += para_dict[j][1]
            self.para_location[para_dict[j][0]] = i

        for k, v in self.model.named_parameters():
            self.para_dict_list[self.para_location[k]][k] = v

        self.logger.info(self.num_ps_para)

        return self.para_location


def get_accuracy(worker_rref, data_dir, test_batch_size, job_name, gpu_id):
    logger = Logger(job_name=job_name, file_dir=f"./logs/{job_name}_acc_tester.log").logger

    # logger.info("get acc enter!")

    transform = transforms.Compose(
        [transforms.ToTensor(), transforms.Normalize((0.4914, 0.4822, 0.4465), (0.2023, 0.1994, 0.2010))]
    )
    test_dataset = datasets.CIFAR10(root=data_dir, train=False, download=True, transform=transform)
    test_loader = DataLoader(test_dataset, batch_size=test_batch_size, shuffle=False)

    # logger.info("get acc enter 4!")

    device = torch.device(f"cuda:{gpu_id}" if torch.cuda.is_available() else "cpu")
    criterion = nn.CrossEntropyLoss()
    criterion = criterion.to(device)

    t0 = time.time()
    logger.info("Start!")
    init = t0
    while True:
        t1 = time.time()
        if t1 - t0 > 60:
            t0 = t1
            m, stop_flag = worker_rref.rpc_sync().get_test_model_and_stop_flag()
            m = m.to(device)

            test_loss = 0
            correct = 0

            with torch.no_grad():
                for j, (data, target) in enumerate(test_loader):
                    data, target = data.to(device), target.to(device)
                    output = m(data)
                    loss = criterion(output, target)

                    test_loss += loss.item()
                    _, predicted = output.max(1)
                    correct += predicted.eq(target).sum().item()

            test_loss = test_loss * test_batch_size / len(test_loader.dataset)
            accuracy = 100.0 * correct / len(test_dataset)
            logger.info(
                "Test Loss: {:6.3f} | Accuracy: {:5.2f} % | Time: {:7.2f} seconds".format(test_loss, accuracy, (t1 - init))
            )

            if stop_flag:
                break


def get_worker(
    job_name, scheduler_rref, num_ps, num_idle_ps, num_workers, model, id, data_dir, batch_size, num_epochs, para_location
):
    worker = Worker(
        job_name, scheduler_rref, num_ps, num_idle_ps, num_workers, model, id, data_dir, batch_size, num_epochs, para_location
    )
    return worker


def get_ps(job_name, scheduler_rref, num_ps, num_idle_ps, num_workers, id, worker_rrefs, lr, para_location):
    ps = ParameterServer(job_name, scheduler_rref, num_ps, num_idle_ps, num_workers, id, worker_rrefs, lr)
    return ps


def run(job_name, rank, num_ps, num_workers, data_dir, model, batch_size, lr, num_epochs, job_id, gpu_id):
    logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)
    num_idle_ps = 0
    world_size = num_workers + num_ps + num_idle_ps + 2
    options = rpc.TensorPipeRpcBackendOptions(num_worker_threads=16, rpc_timeout=0)

    if rank == 0:
        logging.info(f"Scheduler initializing")
        rpc.init_rpc(f"scheduler", rank=rank, world_size=world_size, rpc_backend_options=options)
        logging.info(f"Scheduler initialized")

        scheduler = Scheduler(job_name, num_ps, num_idle_ps, num_workers, model)

        for i in range(num_workers):
            worker = f"worker{i}"
            scheduler.worker_rrefs[i] = rpc.remote(
                to=worker,
                func=get_worker,
                args=(
                    job_name,
                    scheduler.scheduler_rref,
                    num_ps,
                    num_idle_ps,
                    num_workers,
                    model,
                    i,
                    data_dir,
                    batch_size,
                    num_epochs,
                    scheduler.para_location,
                ),
            )

        for i in range(num_ps + num_idle_ps):
            ps = f"ps{i}"
            scheduler.ps_rrefs[i] = rpc.remote(
                to=ps,
                func=get_ps,
                args=(
                    job_name,
                    scheduler.scheduler_rref,
                    num_ps,
                    num_idle_ps,
                    num_workers,
                    i,
                    scheduler.worker_rrefs,
                    lr,
                    scheduler.para_location,
                ),
            )

        futs = []
        futs.append(
            rpc.rpc_async(
                to="acc_tester", func=get_accuracy, args=(scheduler.worker_rrefs[0], data_dir, batch_size, job_name, gpu_id)
            )
        )
        for worker in range(num_workers):
            futs.append(
                rpc.rpc_async(
                    to=scheduler.worker_rrefs[worker].owner(),
                    func=Worker.run_worker,
                    args=(scheduler.worker_rrefs[worker], scheduler.ps_rrefs, scheduler.worker_rrefs, gpu_id),
                )
            )

        # scheduler.run_scheduler()
        scheduler.logger.info(f"Model Name: {scheduler.model_name}")
        connm = SocketMsger.tcp_connect(launcher_address, launcher_port)
        connm.send(job_id)
        while True:
            time.sleep(45)
            stop_flag = True
            for worker_ref in scheduler.worker_rrefs:
                flag = worker_ref.rpc_sync().get_stop_flag()
                if flag is False:
                    stop_flag = False
                    break
            if stop_flag:
                connm.send("STOP")
                break

        torch.futures.wait_all(futs)
        logging.info("Finish training")

    elif rank > num_workers and rank != num_ps + num_idle_ps + num_workers + 1:
        logging.info(f"PS{rank - num_workers - 1} initializing")
        rpc.init_rpc(f"ps{rank - num_workers - 1}", rank=rank, world_size=world_size, rpc_backend_options=options)
        logging.info(f"PS{rank - num_workers - 1} initialized")

    elif rank == num_ps + num_idle_ps + num_workers + 1:
        logging.info(f"acc_tester initializing")
        rpc.init_rpc("acc_tester", rank=rank, world_size=world_size, rpc_backend_options=options)
        logging.info(f"acc_tester initialized")

    else:
        logging.info(f"Worker{rank - 1} initializing")
        rpc.init_rpc(f"worker{rank - 1}", rank=rank, world_size=world_size, rpc_backend_options=options)
        logging.info(f"Worker{rank - 1} initialized")

    rpc.shutdown()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Distributed PS Self-Managing")
    parser.add_argument("-j", "--job_name", type=str, default="jobname", help="The job's name.")
    parser.add_argument("-m", "--model", type=str, default="modelname", help="The model's name.")
    parser.add_argument("-r", "--rank", type=int, default=2, help="Global rank of this process.")
    parser.add_argument("-w", "--num_workers", type=int, default=4, help="Total number of workers.")
    parser.add_argument("-p", "--num_ps", type=int, default=4, help="Total number of PSs.")
    parser.add_argument("-d", "--data_dir", type=str, default="./training_data", help="The location of dataset.")
    parser.add_argument("-a", "--master_addr", type=str, default="localhost", help="Address of master.")
    parser.add_argument("--master_port", type=str, default="29600", help="Port that master is listening on.")
    parser.add_argument("-b", "--batch_size", type=int, default=128, help="Batch size of each worker during training.")
    parser.add_argument("--lr", type=float, default=0.005, help="Learning rate.")
    parser.add_argument("-e", "--num_epochs", type=int, default=1, help="Number of epochs.")
    parser.add_argument("--job_id", type=int, default=0, help="Number of epochs.")
    parser.add_argument("--gpu_id", type=int, default=0, help="Number of epochs.")

    args = parser.parse_args()

    os.environ["MASTER_ADDR"] = args.master_addr
    os.environ["MASTER_PORT"] = args.master_port

    run(
        args.job_name,
        args.rank,
        args.num_ps,
        args.num_workers,
        args.data_dir,
        args.model,
        args.batch_size,
        args.lr,
        args.num_epochs,
        args.job_id,
        args.gpu_id,
    )
