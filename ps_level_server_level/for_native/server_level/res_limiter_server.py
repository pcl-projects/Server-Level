#! /usr/bin/env python3


import threading

from zeyu_utils import net as znet
from zeyu_utils import os as zos

import global_vars as gvs

PORT = 37889
CPULIMIT_PATH = "/home/qxc4fh/zeyu_workspace/bin/cpulimit"


PID_INFO = {}


# Below are variables from AWS code
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
# __RE_JC_NAME = re.compile(r"_(([\-0-9A-Za-z]+)\-(ps|worker)([0-9]+))\-")
# End


def limit_cpu(job_id, ps_id, percent):
    job_ps_name = f"job{job_id}-ps{ps_id}"
    key_str = f"\\-\\-job_name=job{job_id} \\-\\-rank={ps_id + 10 + 1}"
    cmd = f"ps aux | grep '{key_str}' | grep -v grep | awk '{{print $2}}'"
    pid = zos.run_cmd(cmd)
    limit_cmd = f"{CPULIMIT_PATH} -p {pid} -l {percent} -z"
    if f"{job_id}:{ps_id}" in PID_INFO:
        pass
    else:
        PID_INFO[f"{job_id}:{ps_id}"] = pid
    threading.Thread(target=zos.run_cmd, args=(limit_cmd,)).start()
    gvs.LIMITED_JCS[job_ps_name] = [None, percent]
    print(f"Limit Job {job_id}'s PS {ps_id} with PID {pid}")


def unlimit_cpu(job_id, ps_id, percent):
    pid = None
    if f"{job_id}:{ps_id}" not in PID_INFO:
        return
    else:
        job_ps_name = f"job{job_id}-ps{ps_id}"
        pid = PID_INFO[f"{job_id}:{ps_id}"]
        key_str = f"\\-p {pid} \\-l {percent} \\-z"
        cmd = f"ps aux | grep '{key_str}' | grep -v grep | awk '{{print $2}}'"
        limit_cmd_pid = zos.run_cmd(cmd)
        zos.run_cmd(f"kill {limit_cmd_pid}")
        PID_INFO.pop(f"{job_id}:{ps_id}", 0)
        gvs.LIMITED_JCS.pop(job_ps_name, 0)
    print(f"Unlimit Job {job_id}'s PS {ps_id} with PID {pid}")


def connm_thread(connm: znet.SocketMsger):
    data = connm.recv()
    if data is not None:
        job_id = data[0]
        ps_id = data[1]
        cmd = data[2]
        percent = data[3]
        if cmd == "LIMIT":
            limit_cpu(job_id, ps_id, percent)
        elif cmd == "UNLIMIT":
            unlimit_cpu(job_id, ps_id, percent)


def run_res_limiter_server():
    listener = znet.SocketMsger.tcp_listener("0.0.0.0", PORT)
    while True:
        connm, _ = listener.accept()
        threading.Thread(target=connm_thread, args=(connm,)).start()


# if __name__ == "__main__":
#     listener = znet.SocketMsger.tcp_listener("0.0.0.0", PORT)
#     while True:
#         connm, _ = listener.accept()
#         threading.Thread(target=connm_thread, args=(connm,)).start()
