#! /usr/bin/env python3


import threading

import global_data as gd
import pcl_utils.net as znet
import res_limiter as rl

__BANDWD_ALLOC = 2.5 * 1024  # Unit Mbit
__CPU_ALLOC = 400  # Unit Mbit
__GPU_ALLOC = 0
__MEM_ALLOC = 10 * 8 * 1024  # Unit Mbit


def __respond(socketm: znet.SocketMsger, para_size):
    ps_name = socketm.data
    bandwd_demand = 8 * para_size
    cpu_demand = para_size
    gpu_demand = 0
    mem_demand = 8 * cpu_demand
    bandwd_recv = __BANDWD_ALLOC
    cpu_recv = __CPU_ALLOC
    gpu_recv = __GPU_ALLOC
    mem_recv = __MEM_ALLOC
    value = [None, None]
    print(gd.LIMITED_JCS)
    if ps_name in gd.LIMITED_JCS:
        value = gd.LIMITED_JCS[ps_name]
    if value[0] is not None:
        bandwd_recv = int(value[0])
    if value[1] is not None:
        cpu_recv = __CPU_ALLOC * value[1] / 100
    job_name = ps_name.split("-")[0]
    if job_name not in gd.JC_STATS:
        gd.JC_STATS[job_name] = {
            ps_name: {
                "bandwd": [
                    bandwd_demand / bandwd_recv,
                    bandwd_demand - bandwd_recv,
                    bandwd_recv - bandwd_demand,
                ],
                "cpu": [
                    cpu_demand / cpu_recv,
                    cpu_demand - cpu_recv,
                    cpu_recv - cpu_demand,
                ],
                "gpu": [0, gpu_demand - gpu_recv, gpu_recv - gpu_demand],
                "mem": [
                    mem_demand / mem_recv,
                    mem_demand - mem_recv,
                    mem_recv - mem_demand,
                ],
            }
        }
    else:
        jcs = gd.JC_STATS[job_name]
        if ps_name not in jcs:
            jcs[ps_name] = {
                "bandwd": [
                    bandwd_demand / bandwd_recv,
                    bandwd_demand - bandwd_recv,
                    bandwd_recv - bandwd_demand,
                ],
                "cpu": [
                    cpu_demand / cpu_recv,
                    cpu_demand - cpu_recv,
                    cpu_recv - cpu_demand,
                ],
                "gpu": [0, gpu_demand - gpu_recv, gpu_recv - gpu_demand],
                "mem": [
                    mem_demand / mem_recv,
                    mem_demand - mem_recv,
                    mem_recv - mem_demand,
                ],
            }
        else:
            jcs[ps_name]["bandwd"] = [
                bandwd_demand / bandwd_recv,
                bandwd_demand - bandwd_recv,
                bandwd_recv - bandwd_demand,
            ]
            jcs[ps_name]["cpu"] = [
                cpu_demand / cpu_recv,
                cpu_demand - cpu_recv,
                cpu_recv - cpu_demand,
            ]
            jcs[ps_name]["gpu"] = [0, gpu_demand - gpu_recv, gpu_recv - gpu_demand]
            jcs[ps_name]["mem"] = [
                mem_demand / mem_recv,
                mem_demand - mem_recv,
                mem_recv - mem_demand,
            ]
    print(
        f"Para size: {para_size} --- {bandwd_demand} | {bandwd_recv} | {cpu_demand} | {cpu_recv} | {gpu_demand} | {gpu_recv} | {mem_demand} | {mem_recv}"
    )
    socketm.send(
        f"{bandwd_demand} | {bandwd_recv} | {cpu_demand} | {cpu_recv} | {gpu_demand} | {gpu_recv} | {mem_demand} | {mem_recv}"
    )


def msg_processing(msg, socketm: znet.SocketMsger):
    msg_eles = list(map(lambda x: x.strip(), msg.split("|")))
    job_name = msg_eles[0]
    rank = int(msg_eles[1])
    para_size = float(msg_eles[2])
    ps_name = f"{job_name}-ps{rank}"
    if para_size < 0.0001:
        if ps_name in gd.ACTIVE_PSS:
            gd.ACTIVE_PSS.pop(ps_name)
        return
    print(f"PS name: {ps_name}")
    if ps_name not in gd.ACTIVE_PSS:
        value = []
        socketm.data = ps_name
        value.append(socketm)
        value.append(para_size)
        gd.ACTIVE_PSS[ps_name] = value
    else:
        gd.ACTIVE_PSS[ps_name][1] = para_size
    print(f"Active PSs: {gd.ACTIVE_PSS}")
    __respond(socketm, para_size)


def conn_thread(socketm: znet.SocketMsger):
    while True:
        recv = socketm.recv()
        if recv == "EXIT" or recv is None:
            ps_name = socketm.data
            if ps_name is not None:
                gd.ACTIVE_PSS.pop(ps_name)
                socketm.data = None
                job_name = ps_name.split("-")[0]
                if job_name in gd.JC_STATS:
                    jcs = gd.JC_STATS[job_name]
                    if ps_name in jcs:
                        jcs.pop(ps_name)
                    if len(jcs) == 0:
                        gd.JC_STATS.pop(job_name)
            if recv is not None:
                socketm.close()
            return
        msg_processing(recv, socketm)


def listener_thread(listen_ip, listen_port):
    listener = znet.SocketMsger.tcp_listener(listen_ip, listen_port)
    while True:
        conn, _ = listener.accept()
        t = threading.Thread(target=conn_thread, args=(conn,))
        t.start()


def run_res_reporter(listen_ip, listen_port):
    t1 = threading.Thread(target=listener_thread, args=(listen_ip, listen_port))
    t2 = threading.Thread(target=rl.run_res_limiter)
    t1.start()
    t2.start()
    t1.join()
    t2.join()
