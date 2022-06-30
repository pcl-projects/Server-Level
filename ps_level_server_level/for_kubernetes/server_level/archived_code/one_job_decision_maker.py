#! /usr/bin/env python3

import argparse
import queue
import re
import socket
import subprocess
import threading
import time

import psutil

COMM_NIC = "en0"
NAME = "Server 01"
LISTEN_PORT = 20001


active_ps_names = {}
stragglers = set()
countdown_flag = False
countdown_start = time.time()
countdown_time = 3
pss_para_size = {}


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


def get_total_active_ps_para_size():
    global active_ps_names
    global pss_para_size
    sum = 0.0
    for name in active_ps_names.keys():
        if name in pss_para_size:
            sum = sum + pss_para_size[name]
    return sum


class DMakerMates:
    def __init__(self, init_pool={}):
        self.__pool = init_pool.copy()

    def add(self, dmaker_name, ip_port):
        self.__pool[dmaker_name] = ip_port

    def delete(self, dmaker_name):
        self.__pool.pop(dmaker_name)

    def get_ip(self, dmaker_name):
        ip_port = self.__pool[dmaker_name]
        return ip_port.split(":")[0]

    def get_port(self, dmaker_name):
        ip_port = self.__pool[dmaker_name]
        return ip_port.split(":")[1]

    def get_strg_rank_pairs(self, strg_to_move, threshold):
        dmaker_names = []
        dmaker_sockeths = []
        for server_name, ip_port in self.__pool.items():
            arr = ip_port.split(":")
            ip = arr[0]
            port = int(arr[1])
            sock = None
            while True:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                try:
                    sock.connect((ip, port))  # 尝试连接服务端
                    break
                except Exception as e:
                    print("Server not found or not open!", e)
                    time.sleep(5)
            sh = SocketHandler(sock)
            dmaker_names.append(server_name)
            dmaker_sockeths.append(sh)
        for i in range(len(dmaker_names)):
            sh = dmaker_sockeths[i]
            sh.send_line("DM | GETSYSCPUPERCENT")
        sortlist = []
        for i in range(len(dmaker_names)):
            name = dmaker_names[i]
            sh = dmaker_sockeths[i]
            rp = sh.recv_line()
            sortlist.append([name, float(rp), sh])
        sortlist.sort(key=lambda x: x[1])
        print("remote server resources:", sortlist)
        strg_server_pairs = {}
        for s, para_size in strg_to_move.items():
            strg_server_pairs[s] = sortlist[0][2]
        # index = 0
        # for s, para_size in strg_to_move.items():
        #     while index < len(sortlist):
        #         if para_size + sortlist[index][1] <= threshold:
        #             sortlist[index][1] = para_size + sortlist[index][1]
        #             strg_server_pairs[s] = sortlist[index][2]
        #             break
        #         else:
        #             index = index + 1
        print("strg_server_pairs", strg_server_pairs)
        strg_rank_pairs = {}
        for s, sh in strg_server_pairs.items():
            job_name = s.split("-")[0]
            print(job_name)
            sh.send_line(f"DM | GETIDLEPSRANK | {job_name}")
            rank = sh.recv_line()
            strg_rank_pairs[s] = rank
        print("strg_rank_pairs", strg_rank_pairs)
        return strg_rank_pairs

    # # TODO: get the idle PS on other servers for the job
    # def get_idle_ps(self):
    #     """Send req to all other decision makers,
    #     determin which server is underloaded and which PS
    #     on it should be selected as the target PS for migration.
    #     """
    #     dmaker_names = []
    #     dmaker_sockeths = []
    #     dmaker_sysinfos = []
    #     for server_name, ip_port in self.__pool.items():
    #         arr = ip_port.split(":")
    #         ip = arr[0]
    #         port = int(arr[1])
    #         sock = None
    #         while True:
    #             sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #             try:
    #                 sock.connect((ip, port))  # 尝试连接服务端
    #                 break
    #             except Exception as e:
    #                 print("Server not found or not open!", e)
    #                 time.sleep(5)
    #         sh = SocketHandler(sock)
    #         dmaker_names.append(server_name)
    #         dmaker_sockeths.append(sh)
    #     for i in range(len(dmaker_names)):
    #         sh = dmaker_sockeths[i]
    #         sh.send_line("DM | GETSYSLOAD")
    #     for i in range(len(dmaker_names)):
    #         name = dmaker_names[i]
    #         sh = dmaker_sockeths[i]
    #         rp = sh.recv_line()
    #         arr = rp.split("|")
    #         net_send = arr[1]
    #         net_recv = arr[2]
    #         cpu = arr[3]
    #         mem = arr[4]
    #         dmaker_sysinfos.append(
    #             (name, float(net_send), float(net_recv), float(cpu), float(mem))
    #         )
    #         # print(name, net_send, net_recv, cpu, mem)
    #         # sh.close()
    #     # sort_type = 1, 2, 3, 4
    #     sort_type = 3
    #     min = float("inf")
    #     index = None
    #     for i in range(len(dmaker_names)):
    #         value = dmaker_sysinfos[i][sort_type]
    #         if value < min:
    #             min = value
    #             index = i
    #     sh = dmaker_sockeths[index]
    #     sh.send_line("DM | GETIDLEPS")
    #     idle_ps = sh.recv_line()
    #     print(idle_ps)
    #     for sh in dmaker_sockeths:
    #         sh.close()


def run_cmd(cmd):
    return subprocess.check_output(cmd, universal_newlines=True, shell=True).strip("\n")


stof_reg = re.compile(r"[\+\-]?\d*\.\d+")


def stof(s):
    return float(stof_reg.match(s).group(0))


def sys_net_send_ratio(nic_name):
    send1 = psutil.net_io_counters(pernic=True)[nic_name][0]
    time.sleep(0.5)
    send2 = psutil.net_io_counters(pernic=True)[nic_name][0]
    ratio = (send2 - send1) * 2 / 1024 / 1024
    return ratio


def sys_net_recv_ratio(nic_name):
    recv1 = psutil.net_io_counters(pernic=True)[nic_name][1]
    time.sleep(0.5)
    recv2 = psutil.net_io_counters(pernic=True)[nic_name][1]
    ratio = (recv2 - recv1) * 2 / 1024 / 1024
    return ratio


def get_stragglers_resinfo(stragglers):
    docker_cmd = "docker stats --no-stream"
    grep_cmd = "grep default | grep k8s | grep -v POD"
    awk_cmd = "awk '{print $2, $3, $7}'"
    cmd = f"{docker_cmd} | {grep_cmd} | {awk_cmd}"
    res_stats = run_cmd(cmd)
    result = []
    rt = []
    for line in res_stats.split("\n"):
        arr = line.split()
        result.append([arr[0], stof(arr[1]), stof(arr[2])])
    for name in stragglers:
        for i in range(len(result)):
            if name in result[i][0]:
                result[i][0] = name
                rt.append(result[i])
                break
    return rt


def msg_processing_ps(socketh: SocketHandler, msg_arr):
    global countdown_flag
    global countdown_start
    job_name = msg_arr[1]
    rank = msg_arr[2]
    para_size = msg_arr[3]
    strglr_flag = msg_arr[4]

    ps_name = f"{job_name}-ps{rank}"
    if ps_name not in pss_para_size:
        pss_para_size[ps_name] = float(para_size)
    if ps_name not in active_ps_names:
        active_ps_names[ps_name] = socketh
        socketh.data = ps_name
    if int(strglr_flag) == 1:
        if ps_name not in stragglers:
            stragglers.add(ps_name)
        if countdown_flag is False:
            countdown_flag = True
            countdown_start = time.time()
    else:
        if ps_name in stragglers:
            stragglers.remove(ps_name)


re_ps_keyword = re.compile(r"(\-)(ps)(\d+)(\-)")


def get_idle_ps_rank(job_name):
    print("step into get_idle_ps_rank()")
    docker_cmd = "docker stats --no-stream"
    grep_cmd = "grep default | grep k8s | grep -v POD | grep ps"
    for psname in active_ps_names.keys():
        grep_cmd = grep_cmd + f" | grep -v {psname}"
    awk_cmd = "awk '{print $2}'"
    cmd = f"{docker_cmd} | {grep_cmd} | {awk_cmd}"
    output = run_cmd(cmd)
    print("rank output:", output)
    for ori_name in output.split("\n"):
        if job_name in ori_name:
            r = re_ps_keyword.search(ori_name)
            rank = r.group(3)
            return rank


def get_sys_cpu_percent():
    rt = psutil.cpu_percent(interval=1)
    return rt


def msg_processing_dmaker(socketh: SocketHandler, msg_arr):
    control = msg_arr[1]
    if control == "GETSYSLOAD":
        # cmd = "docker stats --no-stream | grep -v POD | awk '{print $3, $4}' "
        # output = subprocess.check_output(
        #     cmd, universal_newlines=True, shell=True
        # ).strip("\n")
        net_send = sys_net_send_ratio(COMM_NIC)
        net_recv = sys_net_recv_ratio(COMM_NIC)
        cpu = psutil.cpu_percent(interval=0.5)
        mem = psutil.virtual_memory().percent

        rp = f"RP | {net_send} | {net_recv} | {cpu} | {mem}"  # bandwidth, cpu, mem, gpu percent
        socketh.send_line(rp)
    elif control == "GETTOTALPARASIZE":
        socketh.send_line(str(get_total_active_ps_para_size()))
    elif control == "GETIDLEPSRANK":
        job_name = msg_arr[2]
        socketh.send_line(get_idle_ps_rank(job_name))
    elif control == "GETSYSCPUPERCENT":
        socketh.send_line(str(get_sys_cpu_percent()))


def msg_processing(socketh: SocketHandler, msg):
    msg_arr = list(map(lambda x: x.strip(), msg.split("|")))
    t = msg_arr[0]
    if t == "PS":
        msg_processing_ps(socketh, msg_arr)
    elif t == "DM":
        msg_processing_dmaker(socketh, msg_arr)
    else:
        print("Invalid msg!")


def conn_thread(socketh: SocketHandler, addr, queue):
    while True:
        s = socketh.recv_line()
        if s is None:
            if socketh.data is not None:
                active_ps_names.pop(socketh.data)
                if socketh.data in stragglers:
                    stragglers.remove(socketh.data)
                if socketh.data in pss_para_size:
                    pss_para_size.pop(socketh.data)
                socketh.data = None
            return
        elif s == "EXIT":
            if socketh.data is not None:
                active_ps_names.pop(socketh.data)
                if socketh.data in stragglers:
                    stragglers.remove(socketh.data)
                if socketh.data in pss_para_size:
                    pss_para_size.pop(socketh.data)
                socketh.data = None
            socketh.close()
            return
        msg_processing(socketh, s)


def listener_thread(listen_ip, listen_port, queue):
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind((listen_ip, listen_port))
    listener.listen(10)
    while True:
        conn, addr = listener.accept()
        connh = SocketHandler(conn)
        t = threading.Thread(target=conn_thread, args=(connh, addr, queue))
        t.start()


def start_decision_making(dmakermates):
    print("start decision making")
    global stragglers
    global pss_para_size
    print("active ps names", active_ps_names)
    print("ps para size", pss_para_size)
    print("stragglers", stragglers)
    threshold_percent = 0.75
    # stragglers_resinfo = get_stragglers_resinfo(stragglers)
    # local_total_active_ps_para_size
    t_ps_para_size = get_total_active_ps_para_size()
    print(t_ps_para_size)
    threshold = threshold_percent * t_ps_para_size
    sortlist = []
    for s in stragglers:
        para_size = pss_para_size[s]
        sortlist.append([s, para_size])
    sortlist.sort(key=lambda x: x[1], reverse=True)
    s_to_move = {}
    remain = t_ps_para_size
    for i in sortlist:
        s_to_move[i[0]] = i[1]
        remain = remain - i[1]
        if remain > threshold:
            continue
        else:
            break
    print(s_to_move)
    if len(s_to_move) == 0:
        return
    strg_rank_pairs = dmakermates.get_strg_rank_pairs(s_to_move, threshold)
    if len(strg_rank_pairs) == 0:
        print("remote server resources not available")
        return
    for psname, rank in strg_rank_pairs.items():
        sh = active_ps_names[psname]
        sh.send_line(rank)


def main_thread(listen_ip, listen_port, dmakermates):
    global countdown_flag
    global countdown_start
    q = queue.Queue()
    t = threading.Thread(target=listener_thread, args=(listen_ip, listen_port, q))
    t.start()

    while True:
        print("active ps names", active_ps_names)
        print("ps para size", pss_para_size)
        print("stragglers", stragglers)
        if countdown_flag:
            if time.time() - countdown_start >= countdown_time:
                start_decision_making(dmakermates)
                countdown_flag = False
        time.sleep(1)

    t.join()


if __name__ == "__main__":
    listen_ip = "0.0.0.0"
    dmakers_info = {
        "01": "172.31.26.137:20000",
        "02": "172.31.22.26:20000",
        "03": "172.31.23.112:20000",
        "04": "172.31.19.9:20000",
    }
    parser = argparse.ArgumentParser()
    parser.add_argument("server_name")
    # parser.add_argument("port", type=int)
    args = parser.parse_args()
    dmakers_info.pop(args.server_name)
    dmakermates = DMakerMates(dmakers_info)
    main_thread(listen_ip, 20000, dmakermates)
