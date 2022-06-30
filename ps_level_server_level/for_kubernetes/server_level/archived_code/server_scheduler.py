#! /usr/bin/env python3


import argparse
import queue
import re
import socket
import subprocess
import threading
import time

import global_data as gd
import pcl_utils.cmd as zcmd
import pcl_utils.net as znet
import res_reporter as rr

COMM_NIC = "en0"
SERVER_NAME = None
LISTEN_PORT = 20001

STRAGGLERS = {}

IS_WAITING = False
WAITING_START_TIME = time.time()
WAITING_TIME = 3


# def get_total_active_ps_para_size():
#     global ACTIVE_PSS
#     global pss_para_size
#     sum = 0.0
#     for name in ACTIVE_PSS.keys():
#         if name in pss_para_size:
#             sum = sum + pss_para_size[name]
#     return sum


class DMakerMates:
    def __init__(self, init_pool={}):
        self.__pool = init_pool.copy()
        self.__re_rank_in_ps_name = re.compile(r"-ps(\d+)$")

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

    def select_target_ps_and_respond(self, ps_name, socketm: znet.SocketMsger):
        job_name = ps_name.split("-")[0]
        ps_band_sdegree = gd.JC_STATS[job_name][ps_name]["bandwd"][0]
        ps_band_excess = gd.JC_STATS[job_name][ps_name]["bandwd"][1]
        ps_cpu_sdegree = gd.JC_STATS[job_name][ps_name]["cpu"][0]
        ps_cpu_excess = gd.JC_STATS[job_name][ps_name]["cpu"][1]
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
            sh = znet.SocketMsger(sock)
            dmaker_names.append(server_name)
            dmaker_sockeths.append(sh)
        for i in range(len(dmaker_names)):
            sh = dmaker_sockeths[i]
            sh.send(f"SS | GET_JOB_PS_STATS | {job_name}")
        target_ps_list = []
        for i in range(len(dmaker_names)):
            sname = dmaker_names[i]
            server_sh = dmaker_sockeths[i]
            rp = server_sh.recv()
            if rp != "None":
                for each in rp.split("|"):
                    eles = each.split(":")
                    ps_name = eles[0]
                    band_a = float(eles[2])
                    cpu_a = float(eles[3])
                    gpu_a = float(eles[4])
                    mem_a = float(eles[5])
                    target_ps_list.append([ps_name, band_a, cpu_a, gpu_a, mem_a])
        print(f"target ps list: {target_ps_list}")
        result_target = []
        remain_excess = 0
        ratio = 0
        if ps_band_sdegree >= ps_cpu_sdegree:
            ratio = 1 / 8
            remain_excess = ps_band_excess
            target_ps_list.sort(key=lambda x: x[1], reverse=True)
            for each in target_ps_list:
                print(f"bandwidth remain excess: {remain_excess}")
                if remain_excess <= each[1]:
                    result_target.append([each[0], remain_excess * ratio])
                    remain_excess = 0
                    break
                else:
                    result_target.append([each[0], each[1] * ratio])
                    remain_excess = remain_excess - each[1]
                    continue
        else:
            ratio = 1
            remain_excess = ps_cpu_excess
            target_ps_list.sort(key=lambda x: x[2], reverse=True)
            for each in target_ps_list:
                print(f"cpu remain excess: {remain_excess}")
                if remain_excess <= each[2]:
                    result_target.append([each[0], remain_excess * ratio])
                    remain_excess = 0
                    break
                else:
                    result_target.append([each[0], each[2] * ratio])
                    remain_excess = remain_excess - each[2]
                    continue
        if remain_excess > 0:
            socketm.send(f"SS | GET_ONE_IDLE_PS | {job_name}")
            idle_ps_name = socketm.recv()
            if idle_ps_name != "None":
                result_target.append([idle_ps_name, remain_excess * ratio])
        n = 0
        result_string = ""
        print("Target PSs:", result_target)
        for each in result_target:
            n += 1
            ps_name = each[0]
            para_size = each[1]
            rank = self.__re_rank_in_ps_name.search(ps_name).group(1)
            result_string = result_string + f"|{rank}|{para_size}"
        return_str = f"RP|{n}" + result_string
        socketm.send(return_str)

        # sortlist = []
        # for i in range(len(dmaker_names)):
        #     name = dmaker_names[i]
        #     sh = dmaker_sockeths[i]
        #     rp = sh.recv()
        #     sortlist.append([name, float(rp), sh])
        # sortlist.sort(key=lambda x: x[1])
        # print("remote server resources:", sortlist)
        # strg_server_pairs = {}
        # for s, para_size in strg_to_move.items():
        #     strg_server_pairs[s] = sortlist[0][2]
        # index = 0
        # for s, para_size in strg_to_move.items():
        #     while index < len(sortlist):
        #         if para_size + sortlist[index][1] <= threshold:
        #             sortlist[index][1] = para_size + sortlist[index][1]
        #             strg_server_pairs[s] = sortlist[index][2]
        #             break
        #         else:
        #             index = index + 1
        # print("strg_server_pairs", strg_server_pairs)
        # strg_rank_pairs = {}
        # for s, sh in strg_server_pairs.items():
        #     job_name = s.split("-")[0]
        #     print(job_name)
        #     sh.send(f"SS | GET_JOB_PS_STATS | {job_name}")
        #     rank = sh.recv()
        #     strg_rank_pairs[s] = rank
        # print("strg_rank_pairs", strg_rank_pairs)
        # return strg_rank_pairs

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
    #         sh.send("DM | GETSYSLOAD")
    #     for i in range(len(dmaker_names)):
    #         name = dmaker_names[i]
    #         sh = dmaker_sockeths[i]
    #         rp = sh.recv()
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
    #     sh.send("DM | GETIDLEPS")
    #     idle_ps = sh.recv()
    #     print(idle_ps)
    #     for sh in dmaker_sockeths:
    #         sh.close()


# stof_reg = re.compile(r"[\+\-]?\d*\.\d+")


# def stof(s):
#     return float(stof_reg.match(s).group(0))


# def sys_net_send_ratio(nic_name):
#     send1 = psutil.net_io_counters(pernic=True)[nic_name][0]
#     time.sleep(0.5)
#     send2 = psutil.net_io_counters(pernic=True)[nic_name][0]
#     ratio = (send2 - send1) * 2 / 1024 / 1024
#     return ratio


# def sys_net_recv_ratio(nic_name):
#     recv1 = psutil.net_io_counters(pernic=True)[nic_name][1]
#     time.sleep(0.5)
#     recv2 = psutil.net_io_counters(pernic=True)[nic_name][1]
#     ratio = (recv2 - recv1) * 2 / 1024 / 1024
#     return ratio


# def get_stragglers_resinfo(stragglers):
#     docker_cmd = "docker stats --no-stream"
#     grep_cmd = "grep default | grep k8s | grep -v POD"
#     awk_cmd = "awk '{print $2, $3, $7}'"
#     cmd = f"{docker_cmd} | {grep_cmd} | {awk_cmd}"
#     res_stats = zcmd.run_cmd(cmd)
#     result = []
#     rt = []
#     for line in res_stats.split("\n"):
#         arr = line.split()
#         result.append([arr[0], stof(arr[1]), stof(arr[2])])
#     for name in stragglers:
#         for i in range(len(result)):
#             if name in result[i][0]:
#                 result[i][0] = name
#                 rt.append(result[i])
#                 break
#     return rt


def msg_processing_ps(msg_eles, socketm: znet.SocketMsger):
    global IS_WAITING
    global WAITING_START_TIME

    job_name = msg_eles[1]
    rank = msg_eles[2]
    is_straggler = int(msg_eles[3])

    ps_name = f"{job_name}-ps{rank}"

    if is_straggler:
        if ps_name not in STRAGGLERS:
            STRAGGLERS[ps_name] = socketm
        if IS_WAITING is False:
            IS_WAITING = True
            WAITING_START_TIME = time.time()
    else:
        if ps_name in STRAGGLERS:
            STRAGGLERS.pop(ps_name)


# re_ps_keyword = re.compile(r"(\-)(ps)(\d+)(\-)")


# def get_idle_ps_rank(job_name):
#     print("step into get_idle_ps_rank()")
#     docker_cmd = "docker stats --no-stream"
#     grep_cmd = "grep default | grep k8s | grep -v POD | grep ps"
#     for psname in ACTIVE_PSS.keys():
#         grep_cmd = grep_cmd + f" | grep -v {psname}"
#     awk_cmd = "awk '{print $2}'"
#     cmd = f"{docker_cmd} | {grep_cmd} | {awk_cmd}"
#     output = zcmd.run_cmd(cmd)
#     print("rank output:", output)
#     for ori_name in output.split("\n"):
#         if job_name in ori_name:
#             r = re_ps_keyword.search(ori_name)
#             rank = r.group(3)
#             return rank


# def get_sys_cpu_percent():
#     rt = psutil.cpu_percent(interval=1)
#     return rt

__RE_PS_NAME = re.compile(r"_(([0-9A-Za-z]+)\-ps([0-9]+))\-")


def __get_all_pss_of_job(job_name):
    rt = set()
    docker_cmd = "docker ps --format '{{.Names}}'"
    grep_cmd = f"grep '{job_name}-ps' | grep -v POD | grep default | grep k8s"
    output = zcmd.run_cmd(f"{docker_cmd} | {grep_cmd}")
    if output is not None:
        con_names = output.split("\n")
        for con_name in con_names:
            match = __RE_PS_NAME.search(con_name)
            if match is not None:
                ps_name = match.group(1)
                rt.add(ps_name)
    return rt


def msg_processing_ss(msg_eles, socketm: znet.SocketMsger):
    control = msg_eles[1]
    if control == "GET_JOB_PS_STATS":
        response = "None"
        job_name = msg_eles[2]
        if job_name in gd.JC_STATS:
            jcs = gd.JC_STATS[job_name]
            for ps_name, value in jcs.items():
                string = f"{ps_name}:{SERVER_NAME}:{value['bandwd'][2]}:{value['cpu'][2]}:{value['gpu'][2]}:{value['mem'][2]}"
                if response == "None":
                    response = string
                else:
                    response = response + "|" + string
        print("Response to server:", response)
        socketm.send(response)
    elif control == "GET_ONE_IDLE_PS":
        job_name = msg_eles[2]
        all_pss = __get_all_pss_of_job(job_name)
        for each in all_pss:
            if each not in gd.ACTIVE_PSS:
                socketm.send(each)
                return
        socketm.send("None")


def msg_processing(msg, socketm: znet.SocketMsger):
    msg_eles = list(map(lambda x: x.strip(), msg.split("|")))
    msg_type = msg_eles[0]
    if msg_type == "PS":
        msg_processing_ps(msg_eles, socketm)
    elif msg_type == "SS":
        msg_processing_ss(msg_eles, socketm)
    else:
        print("Invalid msg!")


def conn_thread(socketm: znet.SocketMsger):
    while True:
        recv = socketm.recv()
        if recv == "EXIT" or recv is None:
            ps_name = socketm.data
            if ps_name is not None:
                if ps_name in STRAGGLERS:
                    STRAGGLERS.pop(ps_name)
                socketm.data = None
            if recv is not None:
                socketm.close()
            return
        msg_processing(recv, socketm)


def listener_thread(listen_ip, listen_port):
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind((listen_ip, listen_port))
    listener.listen(10)
    while True:
        conn, _ = listener.accept()
        connm = znet.SocketMsger(conn)
        t = threading.Thread(target=conn_thread, args=(connm,))
        t.start()


def schedule_ps_stragglers(dmakermates: DMakerMates):
    global STRAGGLERS
    print("schedule_ps_stragglers")
    print("STRAGGLERS", STRAGGLERS)
    # stragglers_resinfo = get_stragglers_resinfo(stragglers)
    # local_total_active_ps_para_size
    for ps_name, socketm in STRAGGLERS.items():
        dmakermates.select_target_ps_and_respond(ps_name, socketm)


def main_thread(listen_ip, listen_port, dmakermates):
    global IS_WAITING
    global WAITING_START_TIME

    threading.Thread(target=listener_thread, args=(listen_ip, listen_port)).start()
    threading.Thread(target=rr.run_res_reporter, args=("0.0.0.0", 20003)).start()

    while True:
        print("STRAGGLERS", STRAGGLERS)
        if IS_WAITING:
            if time.time() - WAITING_START_TIME >= WAITING_TIME:
                schedule_ps_stragglers(dmakermates)
                time.sleep(3)
                IS_WAITING = False
        time.sleep(1)


if __name__ == "__main__":
    listen_ip = "0.0.0.0"
    dmakers_info = {
        "S01": "172.31.26.137:20004",
        "S02": "172.31.22.26:20004",
        # "S03": "172.31.88.123:20004",
        # "S04": "172.31.95.127:20004",
        # "S05": "172.31.26.89:20004",
        # "S06": "172.31.21.13:20004",
        # "S07": "172.31.23.35:20004",
        # "S08": "172.31.25.132:20004",
    }
    parser = argparse.ArgumentParser()
    parser.add_argument("server_name")
    # parser.add_argument("port", type=int)
    args = parser.parse_args()
    dmakers_info.pop(args.server_name)
    SERVER_NAME = args.server_name
    dmakermates = DMakerMates(dmakers_info)
    main_thread(listen_ip, 20004, dmakermates)
