#! /usr/bin/env python3

import queue
import random
import re
import threading
import time

import global_data as gd
import pcl_utils.cmd as zcmd

__RE_JC_NAME = re.compile(r"_(([0-9A-Za-z]+)\-(ps|worker)([0-9]+))\-")

__BANDWD_LIM_INITED = False
__NUMIFBS = 30
__IFB_INDEX_CURSOR = 0
__AVA_IFB_INDICES = set()
__BANDWD_LIM_VETHS_IFB_INDICES = {}

__CPU_LIM_INITED = False
__CPU_LIM_CON_PIDS = {}

__ACTIVE_JOBS = {}  # Job name - timer starting time
__STRAGGLER_PERIOD = 40  # Unit second


def __probability(percent) -> bool:
    r = random.randint(0, 100)
    if r < percent:
        return True
    else:
        return False


def __init_bandwidth_limiter():
    global __BANDWD_LIM_INITED
    zcmd.run_cmd("modprobe -r ifb")
    zcmd.run_cmd(f"modprobe ifb numifbs={__NUMIFBS}")
    for i in range(__NUMIFBS):
        zcmd.run_cmd(f"ip link set dev ifb{i} up")
    __BANDWD_LIM_INITED = True


def __recycle_ifb_index(index):
    __AVA_IFB_INDICES.add(index)


def __clear_unused_ifb_index():
    veths = set()
    output = zcmd.run_cmd("ip -o addr | grep ': veth' | awk '{print $2}'")
    if output is not None:
        veths = set(output.split("\n"))
    for key, value in __BANDWD_LIM_VETHS_IFB_INDICES.items():
        if value[0] not in veths:
            print(f"Recycle ifb index: {value[1]}")
            __recycle_ifb_index(value[1])
            __BANDWD_LIM_VETHS_IFB_INDICES.pop(key)


def __assign_ifb_index():
    global __IFB_INDEX_CURSOR
    index = None
    __clear_unused_ifb_index()
    if len(__AVA_IFB_INDICES) == 0:
        if __IFB_INDEX_CURSOR == __NUMIFBS:
            return None
        index = __IFB_INDEX_CURSOR
        __IFB_INDEX_CURSOR += 1
    else:
        index = __AVA_IFB_INDICES.pop()
    return index


def limit_bandwidth(jc_name, rate):
    global __BANDWD_LIM_INITED
    global __BANDWD_LIM_VETHS_IFB_INDICES
    print("Limit bandwidth:", jc_name, rate)
    # if down_rate is None and up_rate is None:
    #     return

    docker_cmd = (
        "docker ps --format '{{.Names}}' | grep default | grep -v POD | grep k8s"
    )
    con_names = zcmd.run_cmd(docker_cmd).split("\n")
    container_name = None
    for con_name in con_names:
        if jc_name in con_name:
            container_name = con_name
            break
    if container_name is None:
        return

    ifb_index = None
    if jc_name in __BANDWD_LIM_VETHS_IFB_INDICES:
        ifb_index = __BANDWD_LIM_VETHS_IFB_INDICES[jc_name][1]
    else:
        ifb_index = __assign_ifb_index()
        print(f"ifb_index {ifb_index}")
        __BANDWD_LIM_VETHS_IFB_INDICES[jc_name] = [None, ifb_index]

    iflink = zcmd.run_cmd(
        f"docker exec {container_name} cat /sys/class/net/eth0/iflink"
    )
    veth = zcmd.run_cmd(
        f"ip -o addr | grep '{iflink}: veth' | awk 'NR==1 {{print $2}}'"
    )
    __BANDWD_LIM_VETHS_IFB_INDICES[jc_name][0] = veth
    if not __BANDWD_LIM_INITED:
        __init_bandwidth_limiter()

    zcmd.run_cmd(f"tc qdisc del dev {veth} root")
    zcmd.run_cmd(f"tc qdisc del dev {veth} ingress")
    zcmd.run_cmd(f"tc qdisc del dev ifb{ifb_index} root")
    zcmd.run_cmd(f"tc qdisc del dev ifb{ifb_index} ingress")

    zcmd.run_cmd(f"tc qdisc add dev {veth} root handle 1: htb")
    zcmd.run_cmd(
        f"tc class add dev {veth} parent 1: classid 1:1 htb rate {rate}mbit ceil {rate}mbit"
    )
    zcmd.run_cmd(
        f"tc filter add dev {veth} parent 1: protocol ip prio 1 u32 match ip src 0.0.0.0/0 flowid 1:1"
    )

    zcmd.run_cmd(f"tc qdisc add dev {veth} handle ffff: ingress")
    zcmd.run_cmd(
        f"tc filter add dev {veth} parent ffff: protocol ip u32 match u32 0 0 action mirred egress redirect dev ifb{ifb_index}"
    )
    zcmd.run_cmd(f"tc qdisc add dev ifb{ifb_index} root handle 2: htb")
    zcmd.run_cmd(
        f"tc class add dev ifb{ifb_index} parent 2: classid 2:1 htb rate {rate}mbit ceil {rate}mbit"
    )
    zcmd.run_cmd(
        f"tc filter add dev ifb{ifb_index} parent 2: protocol ip prio 1 u32 match ip src 0.0.0.0/0 flowid 2:1"
    )


def unlimit_bandwidth(jc_name):
    global __BANDWD_LIM_VETHS_IFB_INDICES
    print("Unlimit bandwidth:", jc_name)
    if jc_name in __BANDWD_LIM_VETHS_IFB_INDICES:
        veth = __BANDWD_LIM_VETHS_IFB_INDICES[jc_name][0]
        ifb_index = __BANDWD_LIM_VETHS_IFB_INDICES[jc_name][1]
        zcmd.run_cmd(f"tc qdisc del dev {veth} root")
        zcmd.run_cmd(f"tc qdisc del dev {veth} ingress")
        zcmd.run_cmd(f"tc qdisc del dev ifb{ifb_index} root")
        zcmd.run_cmd(f"tc qdisc del dev ifb{ifb_index} ingress")
        __BANDWD_LIM_VETHS_IFB_INDICES.pop(jc_name)
        __recycle_ifb_index(ifb_index)


def __init_cpu_limiter():
    global __CPU_LIM_INITED
    if zcmd.run_cmd("which cpulimit") is None:
        zcmd.run_cmd("apt install -y cpulimit")
    __CPU_LIM_INITED = True


def _cpulimit_thread(con_pid, percent):
    zcmd.run_cmd(f"cpulimit -p {con_pid} -l {percent} -b -z")


def limit_cpu(jc_name, percent):
    global __CPU_LIM_INITED
    global __CPU_LIM_CON_PIDS
    print("Limit CPU:", jc_name, percent)
    # nameids = zcmd.run_cmd("docker ps --format '{{.Names}} {{.ID}}'").split("\n")
    nameids = zcmd.run_cmd(
        "docker ps --format '{{.Names}} {{.ID}}' | grep default | grep -v POD | grep k8s"
    ).split("\n")
    for nameid in nameids:
        if nameid != "":
            name_id = nameid.split()
            con_name = name_id[0]
            con_id = name_id[1]
            if jc_name in con_name:
                con_pid = zcmd.run_cmd(
                    "docker inspect -f '{{.State.Pid}}' " + f"{con_id}"
                )
                if not __CPU_LIM_INITED:
                    __init_cpu_limiter()
                threading.Thread(
                    target=_cpulimit_thread, args=(con_pid, percent)
                ).start()
                print(f"-----------------finish limiting---------------")
                limiter_pid = zcmd.run_cmd(
                    f"ps aux | grep 'cpulimit -p {con_pid} -l' | grep -v grep | awk '{{print $2}}'"
                )

                if limiter_pid is None:
                    print(f"limiter pid is none")
                else:
                    print(f"limiter pid: {limiter_pid}")
                __CPU_LIM_CON_PIDS[jc_name] = con_pid
                break


def unlimit_cpu(jc_name):
    global __CPU_LIM_CON_PIDS
    print("Unlimit CPU:", jc_name)
    if jc_name in __CPU_LIM_CON_PIDS:
        con_pid = __CPU_LIM_CON_PIDS[jc_name]
        limiter_pid = zcmd.run_cmd(
            f"ps aux | grep 'cpulimit -p {con_pid} -l' | grep -v grep | awk '{{print $2}}'"
        )
        zcmd.run_cmd(f"kill {limiter_pid}")
        __CPU_LIM_CON_PIDS.pop(jc_name)


def __get_jc_names_by_job_name(job_name):
    global __RE_JC_NAME
    jc_names = []
    docker_cmd = "docker ps --format '{{.Names}}'"
    grep_cmd = "grep default | grep k8s | grep -v POD"
    con_names = zcmd.run_cmd(f"{docker_cmd} | {grep_cmd}").split("\n")
    for con_name in con_names:
        if con_name != "":
            match = __RE_JC_NAME.search(con_name)
            if match is not None:
                jc_name = match.group(1)
                jc_names.append(jc_name)
    return jc_names


unlimit_bandwidth("test-ps1")
limit_bandwidth("test-ps1", 10)
