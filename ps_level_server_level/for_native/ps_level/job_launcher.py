#! /usr/bin/env python3

import socket
import threading
import time

from zeyu_utils import net as znet

# import threading
# from _thread import *


# def threaded_client(connection):
#     thread_starter = True
host_name = "udc-ba25-27.hpc.virginia.edu"
file_path_prefix = "/home/qxc4fh/zeyu_workspace/dpsm/"

max_jobs = 24
running_jobs = 1
available_gpu_id = set([0, 1, 2, 3])
job_info = {}  # job id: [gpu_id, connm, job_port]

job_port_in_use = set()

job_listener_port = 54333

res_limiter_server_port = 37889

server_addresses = [
    host_name,
    # "udc-ba26-24.hpc.virginia.edu",
    # "udc-ba25-27.hpc.virginia.edu",
    # "udc-ba25-28.hpc.virginia.edu",
    "udc-ba27-24.hpc.virginia.edu",
    "udc-ba27-23.hpc.virginia.edu",
    "udc-ba26-26.hpc.virginia.edu",
    "udc-ba26-24.hpc.virginia.edu",
    "udc-ba25-28.hpc.virginia.edu",
    host_name,
]


__model_list = [
    # "resnet20",
    "resnet56",
    # "vgg13",
    "vgg16",
    "densenet121",
    # "alexnet",
    # "googlenet",
    "mobilenet",
]
__model_list_index = 0


def get_model_name():
    global __model_list
    global __model_list_index
    index = __model_list_index
    __model_list_index = (__model_list_index + 1) % len(__model_list)
    return __model_list[index]


__epoch_list = [
    # 2,
    2,
    # 2,
    2,
    1,
    # 2,
    # 1,
    1,
]
__epoch_list_index = 0


def get_epoch():
    global __epoch_list
    global __epoch_list_index
    index = __epoch_list_index
    __epoch_list_index = (__epoch_list_index + 1) % len(__epoch_list)
    return __epoch_list[index]


def send_cpu_limit(server_addr, job_id, ps_id, percent):
    connm = znet.SocketMsger.tcp_connect(server_addr, res_limiter_server_port)
    connm.send((job_id, ps_id, "LIMIT", percent))
    connm.close()


def send_cpu_unlimit(server_addr, job_id, ps_id, percent):
    connm = znet.SocketMsger.tcp_connect(server_addr, res_limiter_server_port)
    connm.send((job_id, ps_id, "UNLIMIT", percent))
    connm.close()


def connm_thread(connm: znet.SocketMsger):
    job_id = connm.recv()
    job_info[job_id][1] = connm
    # Managing resource limitation
    # limit_flag = True
    time.sleep(60)
    while True:
        # time.sleep(90)
        # if job_id in job_info:
        #     if limit_flag:
        #         limit_flag = False
        #         send_cpu_unlimit(server_addresses[4], job_id, 3, 20)
        #         time.sleep(3)
        #         send_cpu_limit(server_addresses[1], job_id, 0, 20)
        #     else:
        #         limit_flag = True
        #         send_cpu_unlimit(server_addresses[1], job_id, 0, 20)
        #         time.sleep(3)
        #         send_cpu_limit(server_addresses[4], job_id, 3, 20)
        # else:
        #     print(f"Stop job{job_id}'s resource limitation")
        #     break
        if job_id in job_info:
            # send_cpu_unlimit(server_addresses[1], job_id, 0, 20)
            # send_cpu_unlimit(server_addresses[1], job_id, 0, 90)
            # send_cpu_unlimit(server_addresses[4], job_id, 3, 20)
            # send_cpu_unlimit(server_addresses[4], job_id, 3, 90)
            time.sleep(3)
            send_cpu_limit(server_addresses[1], job_id, 0, 20)
            # send_cpu_limit(server_addresses[4], job_id, 3, 90)

            time.sleep(60)

            send_cpu_unlimit(server_addresses[1], job_id, 0, 20)
            # send_cpu_unlimit(server_addresses[1], job_id, 0, 90)
            send_cpu_unlimit(server_addresses[4], job_id, 3, 20)
            # send_cpu_unlimit(server_addresses[4], job_id, 3, 90)

            time.sleep(60)

            # send_cpu_unlimit(server_addresses[1], job_id, 0, 20)
            # send_cpu_unlimit(server_addresses[1], job_id, 0, 90)
            # send_cpu_unlimit(server_addresses[4], job_id, 3, 20)
            # send_cpu_unlimit(server_addresses[4], job_id, 3, 90)
            time.sleep(3)
            send_cpu_limit(server_addresses[4], job_id, 3, 20)
            # send_cpu_limit(server_addresses[1], job_id, 0, 90)

            time.sleep(60)

            send_cpu_unlimit(server_addresses[1], job_id, 0, 20)
            # send_cpu_unlimit(server_addresses[1], job_id, 0, 90)
            send_cpu_unlimit(server_addresses[4], job_id, 3, 20)
            # send_cpu_unlimit(server_addresses[4], job_id, 3, 90)

            time.sleep(60)
        else:
            print(f"Stop job{job_id}'s resource limitation")
            break


def listener_thread():
    listener = znet.SocketMsger.tcp_listener("0.0.0.0", job_listener_port)
    while True:
        connm, _ = listener.accept()
        threading.Thread(target=connm_thread, args=(connm,)).start()


def get_free_port():
    sock = socket.socket()
    sock.bind(("", 0))
    ip, port = sock.getsockname()
    sock.close()
    return port


def run_job(client_addresses, job_id, gpu_id, job_port):
    model_name = get_model_name()
    epoch_num = get_epoch()

    cmd_scheduler = (
        f"python {file_path_prefix}/dpsm.py"
        + " --model="
        + f"{model_name}"
        + " --job_name="
        + f"job{job_id}"
        + " --rank="
        + str(0)
        + " --batch_size="
        + str(128)
        + " --num_workers="
        + str(10)
        + " --num_ps="
        + str(4)
        + " --master_addr="
        + host_name
        + " --master_port="
        + str(job_port)
        + f" --num_epochs={epoch_num} --lr="
        + str(0.005)
        + " --job_id="
        + str(job_id)
        + " --gpu_id="
        + str(gpu_id)
    )
    client_addresses[0].send(cmd_scheduler)

    for i in range(10):
        index = i + 1
        gpu_id_ = 0
        if i <= 4:
            gpu_id_ = 0
        else:
            gpu_id_ = 1
        cmd_worker = (
            f"python {file_path_prefix}/dpsm.py"
            + " --model="
            + f"{model_name}"
            + " --job_name="
            + f"job{job_id}"
            + " --rank="
            + str(index)
            + " --batch_size="
            + str(128)
            + " --num_workers="
            + str(10)
            + " --num_ps="
            + str(4)
            + " --master_addr="
            + host_name
            + " --master_port="
            + str(job_port)
            + f" --num_epochs={epoch_num} --lr="
            + str(0.005)
            + " --job_id="
            + str(job_id)
            + " --gpu_id="
            + str(gpu_id_)
        )
        client_addresses[(i % 5) + 1].send(cmd_worker)

    for i in range(4):
        index = 10 + i + 1
        cmd_ps = (
            f"python {file_path_prefix}/dpsm.py"
            + " --model="
            + f"{model_name}"
            + " --job_name="
            + f"job{job_id}"
            + " --rank="
            + str(index)
            + " --batch_size="
            + str(128)
            + " --num_workers="
            + str(10)
            + " --num_ps="
            + str(4)
            + " --master_addr="
            + host_name
            + " --master_port="
            + str(job_port)
            + f" --num_epochs={epoch_num} --lr="
            + str(0.005)
            + " --job_id="
            + str(job_id)
            + " --gpu_id="
            + str(gpu_id)
        )
        client_addresses[i + 1].send(cmd_ps)

    tester_scheduler = (
        f"python {file_path_prefix}/dpsm.py"
        + " --model="
        + f"{model_name}"
        + " --job_name="
        + f"job{job_id}"
        + " --rank="
        + str(15)
        + " --batch_size="
        + str(128)
        + " --num_workers="
        + str(10)
        + " --num_ps="
        + str(4)
        + " --master_addr="
        + host_name
        + " --master_port="
        + str(job_port)
        + f" --num_epochs={epoch_num} --lr="
        + str(0.005)
        + " --job_id="
        + str(job_id)
        + " --gpu_id="
        + str(0)
    )
    client_addresses[6].send(tester_scheduler)

    # global_port += 1


def run_launcher(client_addresses):
    cur_job_num = 0
    latest_job_id = -1
    while True:
        while cur_job_num < running_jobs and latest_job_id < max_jobs - 1 and len(available_gpu_id) > 0:
            gpu_id = available_gpu_id.pop()
            cur_job_num += 1
            latest_job_id += 1
            job_port = get_free_port()
            while job_port in job_port_in_use:
                job_port = get_free_port()
            job_port_in_use.add(job_port)
            job_info[latest_job_id] = [gpu_id, None, job_port]
            run_job(client_addresses, latest_job_id, gpu_id, job_port)

        if cur_job_num == 0 and latest_job_id >= max_jobs - 1:
            print("All Jobs Finished")
            return
        # check completed jobs
        jobs_to_delete = []
        for job_id in job_info.keys():
            gpu_id, connm, job_port = job_info[job_id]
            if connm is not None:
                data = connm.recv(False)
                if data is not None and data == "STOP":
                    cur_job_num -= 1
                    jobs_to_delete.append(job_id)
                    available_gpu_id.add(gpu_id)
                    job_port_in_use.discard(job_port)
        for id in jobs_to_delete:
            job_info.pop(id, 0)

        time.sleep(10)


if __name__ == "__main__":
    cmd_host = host_name
    cmd_port = 41287
    num_clients = 7

    client_addresses = []

    cmd_listener = znet.SocketMsger.tcp_listener(cmd_host, cmd_port)

    number_of_clients = 0
    # ThreadCount = 0
    while True:
        Client, address = cmd_listener.accept()
        print("Connected to: " + address[0] + ":" + str(address[1]))
        # start_new_thread(threaded_client, (Client,))
        # ThreadCount += 1
        # print("Thread Number: " + str(ThreadCount))
        client_addresses.append(Client)

        number_of_clients = number_of_clients + 1

        if number_of_clients == num_clients:
            break

    threading.Thread(target=listener_thread).start()

    run_launcher(client_addresses)
