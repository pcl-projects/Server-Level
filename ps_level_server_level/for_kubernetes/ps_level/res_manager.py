#! /usr/bin/env python3


import threading
import time

import zeyu_utils.net as znet

import global_vars as gvs
import res_limiter as rl

__WORKER_NUM = 4


__BANDWD_MAX_SV_RES = 13.0 * 1024  # Max server resource for bandwidth, Mbit
__CPU_MAX_SV_RES = (5 * 2 + 5) * 500.0  # Max CPU resource for bandwidth, Mbit, 500 for each core, total
__BANDWD_AVA_SV_RES = __BANDWD_MAX_SV_RES  # Available bandwidth server resrouce.
__CPU_AVA_SV_RES = __CPU_MAX_SV_RES  # Available CPU server resrouce.


# Those may be deprecated, because we never use mem and GPU.
__BANDWD_ALLOC = 2.0 * 1024  # Unit Mbit
__CPU_ALLOC = 500.0  # Unit Mbit
__GPU_ALLOC = 0.0
__MEM_ALLOC = float(10 * 8 * 1024)  # Unit Mbit


def __respond_to_ps(socketm: znet.SocketMsger, para_size):
    global __WORKER_NUM
    global __BANDWD_MAX_SV_RES
    global __CPU_MAX_SV_RES
    global __BANDWD_AVA_SV_RES
    global __CPU_AVA_SV_RES
    ps_name = socketm.piggyback_data
    bandwd_demand = __WORKER_NUM * para_size
    cpu_demand = para_size
    gpu_demand = 0.0
    mem_demand = __WORKER_NUM * para_size
    bandwd_recv = bandwd_demand + __BANDWD_AVA_SV_RES
    cpu_recv = cpu_demand + __CPU_AVA_SV_RES
    gpu_recv = __GPU_ALLOC
    mem_recv = __MEM_ALLOC
    limited_jc_info = [None, None]
    print(f"Res Manager - Limited JCS: {gvs.LIMITED_JCS}")
    if ps_name in gvs.LIMITED_JCS:
        limited_jc_info = gvs.LIMITED_JCS[ps_name]
    if limited_jc_info[0] is not None:
        bandwd_recv = int(limited_jc_info[0])
    if limited_jc_info[1] is not None:
        cpu_recv = __CPU_ALLOC * limited_jc_info[1] / 100
    job_name = ps_name.rsplit("-", 1)[0]
    if job_name not in gvs.JOB_TO_JC_STAT:
        gvs.JOB_TO_JC_STAT[job_name] = {
            ps_name: {
                "bandwd": [bandwd_demand / bandwd_recv, bandwd_demand - bandwd_recv, bandwd_recv - bandwd_demand],
                "cpu": [cpu_demand / cpu_recv, cpu_demand - cpu_recv, cpu_recv - cpu_demand],
                "gpu": [0.0, gpu_demand - gpu_recv, gpu_recv - gpu_demand],
                "mem": [mem_demand / mem_recv, mem_demand - mem_recv, mem_recv - mem_demand],
            }
        }
    else:
        jc_s = gvs.JOB_TO_JC_STAT[job_name]
        if ps_name not in jc_s:
            jc_s[ps_name] = {
                "bandwd": [bandwd_demand / bandwd_recv, bandwd_demand - bandwd_recv, bandwd_recv - bandwd_demand],
                "cpu": [cpu_demand / cpu_recv, cpu_demand - cpu_recv, cpu_recv - cpu_demand],
                "gpu": [0.0, gpu_demand - gpu_recv, gpu_recv - gpu_demand],
                "mem": [mem_demand / mem_recv, mem_demand - mem_recv, mem_recv - mem_demand],
            }
        else:
            jc_s[ps_name]["bandwd"] = [
                bandwd_demand / bandwd_recv,
                bandwd_demand - bandwd_recv,
                bandwd_recv - bandwd_demand,
            ]
            jc_s[ps_name]["cpu"] = [
                cpu_demand / cpu_recv,
                cpu_demand - cpu_recv,
                cpu_recv - cpu_demand,
            ]
            jc_s[ps_name]["gpu"] = [0.0, gpu_demand - gpu_recv, gpu_recv - gpu_demand]
            jc_s[ps_name]["mem"] = [
                mem_demand / mem_recv,
                mem_demand - mem_recv,
                mem_recv - mem_demand,
            ]
    print(
        f"Para size: {para_size} --- {bandwd_demand} | {bandwd_recv} | {cpu_demand} | {cpu_recv} | {gpu_demand} | {gpu_recv} | {mem_demand} | {mem_recv}"
    )
    socketm.send(gvs.JOB_TO_JC_STAT[job_name][ps_name])


def msg_processing(msg, socketm: znet.SocketMsger):
    assert isinstance(msg, dict)
    job_name = msg["job_name"]
    rank = msg["rank"]
    para_num = msg["para_num"]
    para_size = msg["para_size"]
    ps_name = f"{job_name}-ps{rank}"
    # msg_eles = list(map(lambda x: x.strip(), msg.split("|")))
    # job_name = msg_eles[0]
    # rank = int(msg_eles[1])
    # para_size = float(msg_eles[2])
    # ps_name = f"{job_name}-ps{rank}"
    if socketm.piggyback_data is None:
        socketm.piggyback_data = ps_name
    if not para_num > 0:
        if ps_name in gvs.ACTIVE_PSS:
            gvs.ACTIVE_PSS.pop(ps_name)
        __respond_to_ps(socketm, 0.0)
        return
    print(f"Res manager - PS name: {ps_name}")
    if ps_name not in gvs.ACTIVE_PSS:
        value = []
        value.append(socketm)
        value.append(para_size)
        gvs.ACTIVE_PSS[ps_name] = value
    else:
        gvs.ACTIVE_PSS[ps_name][1] = para_size
    print(f"Res manager - Active PSs: {gvs.ACTIVE_PSS}")
    __respond_to_ps(socketm, para_size)


def conn_thread(socketm: znet.SocketMsger):
    while True:
        recvd_data = socketm.recv()
        if recvd_data is None:
            ps_name = socketm.piggyback_data
            if ps_name is not None:
                if ps_name in gvs.ACTIVE_PSS:
                    gvs.ACTIVE_PSS.pop(ps_name)
                socketm.piggyback_data = None
                job_name = ps_name.rsplit("-", 1)[0]
                if job_name in gvs.JOB_TO_JC_STAT:
                    jc_s = gvs.JOB_TO_JC_STAT[job_name]
                    if ps_name in jc_s:
                        jc_s.pop(ps_name)
                    if len(jc_s) == 0:
                        gvs.JOB_TO_JC_STAT.pop(job_name)
                if ps_name in gvs.LIMITED_JCS:
                    gvs.LIMITED_JCS.pop(ps_name)
                rl.unlimit_bandwidth(ps_name)
                rl.unlimit_cpu(ps_name)
            return
        msg_processing(recvd_data, socketm)


def listener_thread(listening_ip, listening_port):
    listener = znet.SocketMsger.tcp_listener(listening_ip, listening_port)
    while True:
        connm, _ = listener.accept()
        threading.Thread(target=conn_thread, args=(connm,)).start()


def ava_server_res_checker_thread():
    global __WORKER_NUM
    global __BANDWD_MAX_SV_RES
    global __CPU_MAX_SV_RES
    global __BANDWD_AVA_SV_RES
    global __CPU_AVA_SV_RES
    while True:
        occupied_bandwidth = 0.0
        occupied_cpu = 0.0
        for ps_name in gvs.ACTIVE_PSS:
            para_size = gvs.ACTIVE_PSS[ps_name][1]
            occupied_bandwidth += __WORKER_NUM * para_size
            occupied_cpu += para_size
        __BANDWD_AVA_SV_RES = __BANDWD_MAX_SV_RES - occupied_bandwidth
        __CPU_AVA_SV_RES = __CPU_MAX_SV_RES - occupied_cpu
        time.sleep(45)


def run_res_manager(listening_ip, listening_port):
    listener_t = threading.Thread(target=listener_thread, args=(listening_ip, listening_port))
    res_limiter_t = threading.Thread(target=rl.run_res_limiter)
    ava_server_res_checker_t = threading.Thread(target=ava_server_res_checker_thread)
    listener_t.start()
    res_limiter_t.start()
    ava_server_res_checker_t.start()
    listener_t.join()
    res_limiter_t.join()
    ava_server_res_checker_t.join()


if __name__ == "__main__":
    run_res_manager("0.0.0.0", 20003)
