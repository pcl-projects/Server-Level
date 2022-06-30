#! /usr/bin/env python3


import queue
import random
import re
import threading
import time

import zeyu_utils.os as zos

import global_vars as gvs

__BANDWD_LIM_INITED = False
__NUMIFBS = 100
__IFB_INDEX_CURSOR = 0
__AVA_IFB_INDICES = set()
__JC_TO_VETH_IFB = {}  # {jc_name: [veth_name, ifb_index]}


__CPU_LIM_INITED = False
__JC_TO_CON_PIDS = {}  # {jc_name: container_pid}


__ACTIVE_JOBS = {}  # Job name - timer starting time
__STRAGGLER_PERIOD = 120  # Unit second


# JC Name is Job Container Name: JOBNAME-ps1 or JOBNAME-worker1.
__RE_JC_NAME = re.compile(r"_(([\-0-9A-Za-z]+)\-(ps|worker)([0-9]+))\-")


def __probability(percent) -> bool:
    r = random.randint(0, 100)
    if r < percent:
        return True
    else:
        return False


def __init_bandwidth_limiter():
    global __BANDWD_LIM_INITED
    zos.run_cmd("modprobe -r ifb")
    zos.run_cmd(f"modprobe ifb numifbs={__NUMIFBS}")
    for i in range(__NUMIFBS):
        zos.run_cmd(f"ip link set dev ifb{i} up")
    __BANDWD_LIM_INITED = True


def __recycle_ifb_index(index):
    __AVA_IFB_INDICES.add(index)


def __clear_unused_ifb_index():
    veths = set()
    output = zos.run_cmd("ip -o addr | grep ': veth' | awk '{print $2}'")
    if output is not None:
        veths = set(output.split("\n"))
    jc_names_to_del = set()
    for key, value in __JC_TO_VETH_IFB.items():
        if value[0] not in veths:
            __recycle_ifb_index(value[1])
            jc_names_to_del.add(key)
    for jc_name in jc_names_to_del:
        __JC_TO_VETH_IFB.pop(jc_name)


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
    global __JC_TO_VETH_IFB
    print("Limit bandwidth:", jc_name, rate)
    # if down_rate is None and up_rate is None:
    #     return

    docker_cmd = "docker ps --format '{{.Names}}' | grep default | grep -v POD | grep k8s"
    con_names = zos.run_cmd(docker_cmd).split("\n")
    container_name = None
    for con_name in con_names:
        if jc_name in con_name:
            container_name = con_name
            break
    if container_name is None:
        return

    ifb_index = None
    if jc_name in __JC_TO_VETH_IFB:
        ifb_index = __JC_TO_VETH_IFB[jc_name][1]
    else:
        ifb_index = __assign_ifb_index()
        __JC_TO_VETH_IFB[jc_name] = [None, ifb_index]

    iflink = zos.run_cmd(f"docker exec {container_name} cat /sys/class/net/eth0/iflink")
    veth = zos.run_cmd(f"ip -o addr | grep '{iflink}: veth' | awk 'NR==1 {{print $2}}'")
    __JC_TO_VETH_IFB[jc_name][0] = veth
    if not __BANDWD_LIM_INITED:
        __init_bandwidth_limiter()

    zos.run_cmd(f"tc qdisc del dev {veth} root")
    zos.run_cmd(f"tc qdisc del dev {veth} ingress")
    zos.run_cmd(f"tc qdisc del dev ifb{ifb_index} root")
    zos.run_cmd(f"tc qdisc del dev ifb{ifb_index} ingress")

    zos.run_cmd(f"tc qdisc add dev {veth} root handle 1: htb")
    zos.run_cmd(f"tc class add dev {veth} parent 1: classid 1:1 htb rate {rate}mbit ceil {rate}mbit")
    zos.run_cmd(f"tc filter add dev {veth} parent 1: protocol ip prio 1 u32 match ip src 0.0.0.0/0 flowid 1:1")

    zos.run_cmd(f"tc qdisc add dev {veth} handle ffff: ingress")
    zos.run_cmd(
        f"tc filter add dev {veth} parent ffff: protocol ip u32 match u32 0 0 action mirred egress redirect dev ifb{ifb_index}"
    )
    zos.run_cmd(f"tc qdisc add dev ifb{ifb_index} root handle 2: htb")
    zos.run_cmd(f"tc class add dev ifb{ifb_index} parent 2: classid 2:1 htb rate {rate}mbit ceil {rate}mbit")
    zos.run_cmd(f"tc filter add dev ifb{ifb_index} parent 2: protocol ip prio 1 u32 match ip src 0.0.0.0/0 flowid 2:1")


def unlimit_bandwidth(jc_name):
    global __JC_TO_VETH_IFB
    print("Unlimit bandwidth:", jc_name)
    if jc_name in __JC_TO_VETH_IFB:
        veth = __JC_TO_VETH_IFB[jc_name][0]
        ifb_index = __JC_TO_VETH_IFB[jc_name][1]
        zos.run_cmd(f"tc qdisc del dev {veth} root")
        zos.run_cmd(f"tc qdisc del dev {veth} ingress")
        zos.run_cmd(f"tc qdisc del dev ifb{ifb_index} root")
        zos.run_cmd(f"tc qdisc del dev ifb{ifb_index} ingress")
        __JC_TO_VETH_IFB.pop(jc_name)
        __recycle_ifb_index(ifb_index)


def __init_cpu_limiter():
    global __CPU_LIM_INITED
    if zos.run_cmd("which cpulimit") is None:
        zos.run_cmd("apt install -y cpulimit > /dev/null 2>&1")
    __CPU_LIM_INITED = True


def __cpulimit_thread(con_pid, percent):
    zos.run_cmd(f"cpulimit -p {con_pid} -l {percent} -b -z")


def limit_cpu(jc_name, percent):
    global __CPU_LIM_INITED
    global __JC_TO_CON_PIDS
    print("Limit CPU:", jc_name, percent)
    # nameids = zos.run_cmd("docker ps --format '{{.Names}} {{.ID}}'").split("\n")
    nameids = zos.run_cmd("docker ps --format '{{.Names}} {{.ID}}' | grep default | grep -v POD | grep k8s").split("\n")
    for nameid in nameids:
        if nameid != "":
            name_id = nameid.split()
            con_name = name_id[0]
            con_id = name_id[1]
            if jc_name in con_name:
                con_pid = zos.run_cmd("docker inspect -f '{{.State.Pid}}' " + f"{con_id}")
                if not __CPU_LIM_INITED:
                    __init_cpu_limiter()
                threading.Thread(target=__cpulimit_thread, args=(con_pid, percent)).start()

                # print("-----------------finish limiting---------------")
                # limiter_pid = zos.run_cmd(f"ps aux | grep 'cpulimit -p {con_pid} -l' | grep -v grep | awk '{{print $2}}'")
                # if limiter_pid is None:
                #     print("limiter pid is none")
                # else:
                #     print(f"limiter pid: {limiter_pid}")

                __JC_TO_CON_PIDS[jc_name] = con_pid
                break


def unlimit_cpu(jc_name):
    global __JC_TO_CON_PIDS
    print("Unlimit CPU:", jc_name)
    if jc_name in __JC_TO_CON_PIDS:
        con_pid = __JC_TO_CON_PIDS[jc_name]
        limiter_pid = zos.run_cmd(f"ps aux | grep 'cpulimit -p {con_pid} -l' | grep -v grep | awk '{{print $2}}'")
        zos.run_cmd(f"kill {limiter_pid}")
        __JC_TO_CON_PIDS.pop(jc_name)


def __get_job_jcs_by_job_name(job_name):
    global __RE_JC_NAME
    job_jcs = []
    docker_cmd = "docker ps --format '{{.Names}}'"
    grep_cmd = "grep default | grep k8s | grep -v POD"
    con_names = zos.run_cmd(f"{docker_cmd} | {grep_cmd}").split("\n")
    for con_name in con_names:
        if job_name in con_name:
            match = __RE_JC_NAME.search(con_name)
            jc_name = match.group(1)
            job_jcs.append(jc_name)
    return job_jcs


def job_timer_thread(msg_q: queue.Queue):
    global __ACTIVE_JOBS
    global __STRAGGLER_PERIOD
    global __RE_JC_NAME
    while True:
        current_jobs = set()
        docker_cmd = "docker ps --format '{{.Names}}'"
        grep_cmd = "grep default | grep k8s | grep -v POD"
        output = zos.run_cmd(f"{docker_cmd} | {grep_cmd}")
        if output is None:
            output = ""
        con_names = output.split("\n")
        for con_name in con_names:
            if con_name != "":
                match = __RE_JC_NAME.search(con_name)
                if match is not None:
                    job_name = match.group(2)
                    if job_name not in current_jobs:
                        current_jobs.add(job_name)
        active_jobs = set(__ACTIVE_JOBS.keys())
        jobs_to_add = current_jobs.difference(active_jobs)
        jobs_to_del = active_jobs.difference(current_jobs)
        for job_name in jobs_to_del:
            __ACTIVE_JOBS.pop(job_name)
        for job_name in jobs_to_add:
            __ACTIVE_JOBS[job_name] = time.time() - __STRAGGLER_PERIOD + 3 + 5
        for job_name, reset_time in __ACTIVE_JOBS.items():
            last_time = time.time()
            if last_time - reset_time >= __STRAGGLER_PERIOD:
                __ACTIVE_JOBS[job_name] = time.time()
                msg_q.put(__get_job_jcs_by_job_name(job_name))
        time.sleep(5)
        print("Active jobs:", __ACTIVE_JOBS)
        print("Active PSs:", gvs.ACTIVE_PSS)


def run_res_limiter():
    msg_q = queue.Queue()
    threading.Thread(target=job_timer_thread, args=(msg_q,)).start()
    while True:
        job_jcs = msg_q.get()
        print("Res limiter timer:", job_jcs)
        for jc_name in job_jcs:
            if "-ps" in jc_name:
                if jc_name in gvs.LIMITED_JCS:
                    gvs.LIMITED_JCS.pop(jc_name)
                unlimit_bandwidth(jc_name)
                unlimit_cpu(jc_name)
                if jc_name in gvs.ACTIVE_PSS:
                    limit_info = [None, None]
                    if __probability(20):
                        limit_bandwidth(jc_name, 50)
                        limit_info[0] = 50
                    # if __probability(28):
                    #     limit_cpu(jc_name, 20)
                    #     limit_info[1] = 20
                    if limit_info[0] is not None or limit_info[1] is not None:
                        gvs.LIMITED_JCS[jc_name] = limit_info
                        print(f"Limit res info: {jc_name} {limit_info}")
                    print(f"------------------Limit JCS: {gvs.LIMITED_JCS} ---------------------------")
