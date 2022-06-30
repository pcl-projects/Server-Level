#! /usr/bin/env python3


import logging

# import argparse
import os
import time

import jinja2
import zeyu_utils.os as zos

# from kubernetes import client, config


__JOB_TIMES = {}


class Logger(object):
    def __init__(self, logger_name, file_dir, log_level=logging.INFO, mode="w"):
        self.logger = logging.getLogger(logger_name)
        self.logger.setLevel(log_level)
        self.formatter = logging.Formatter("%(asctime)s - %(name)s - %(message)s")
        self.fh = logging.FileHandler(filename=file_dir, mode=mode)
        self.fh.setFormatter(self.formatter)
        self.logger.addHandler(self.fh)


def generate_job_yaml(
    job_name, model_name, server_num, worker_num, active_idle_ps_num, batch_size=128, epoch_num=1, port=29600
):
    # service
    with open("./jinja2_templates/job_service.j2", "r") as file:
        buffer = file.read()
    service = {"job_name": job_name, "port": port}
    template = jinja2.Template(buffer)
    with open(f"./yaml/{job_name}.yaml", mode="w") as file:
        file.write(template.render(service))

    # scheduler
    with open("./jinja2_templates/job_scheduler.j2", "r") as file:
        buffer = file.read()
    scheduler = {
        "job_name": job_name,
        "model_name": model_name,
        "rank": 0,
        "batch_size": batch_size,
        "epoch_num": epoch_num,
        "port": port,
        "node": "00",
    }
    template = jinja2.Template(buffer)
    with open(f"./yaml/{job_name}.yaml", mode="a") as file:
        file.write("\n")
        file.write(template.render(scheduler))

    # worker
    for i in range(worker_num):
        rank = i + 1
        k8s_worker_id = i
        node = i % server_num + 1
        if node < 10:
            node = f"0{node}"
        else:
            node = f"{node}"
        with open("./jinja2_templates/job_worker.j2", "r") as file:
            buffer = file.read()
        worker = {
            "job_name": job_name,
            "model_name": model_name,
            "rank": rank,
            "batch_size": batch_size,
            "epoch_num": epoch_num,
            "port": port,
            "k8s_worker_id": k8s_worker_id,
            "node": node,
        }
        template = jinja2.Template(buffer)
        with open(f"./yaml/{job_name}.yaml", mode="a") as file:
            file.write("\n")
            file.write(template.render(worker))

    # ps
    for i in range(active_idle_ps_num):
        rank = i + worker_num + 1
        k8s_ps_id = i
        node = (i + worker_num) % server_num + 1
        if node < 10:
            node = f"0{node}"
        else:
            node = f"{node}"
        with open("./jinja2_templates/job_ps.j2", "r") as file:
            buffer = file.read()
        ps = {
            "job_name": job_name,
            "model_name": model_name,
            "rank": rank,
            "batch_size": batch_size,
            "epoch_num": epoch_num,
            "port": port,
            "k8s_ps_id": k8s_ps_id,
            "node": node,
        }
        template = jinja2.Template(buffer)
        with open(f"./yaml/{job_name}.yaml", mode="a") as file:
            file.write("\n")
            file.write(template.render(ps))

    # acc_tester
    with open("./jinja2_templates/job_acc_tester.j2", "r") as file:
        buffer = file.read()
    acc_tester = {
        "job_name": job_name,
        "model_name": model_name,
        "rank": worker_num + active_idle_ps_num + 1,
        "batch_size": batch_size,
        "epoch_num": epoch_num,
        "port": port,
        "node": "00",
    }
    template = jinja2.Template(buffer)
    with open(f"./yaml/{job_name}.yaml", mode="a") as file:
        file.write("\n")
        file.write(template.render(acc_tester))


__model_list = [
    "resnet20",
    "resnet56",
    "vgg13",
    "vgg16",
    "densenet121",
    "alexnet",
    "googlenet",
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
    2,
    2,
    2,
    2,
    1,
    2,
    1,
    2,
]
__epoch_list_index = 0


def get_epoch():
    global __epoch_list
    global __epoch_list_index
    index = __epoch_list_index
    __epoch_list_index = (__epoch_list_index + 1) % len(__epoch_list)
    return __epoch_list[index]


# config.load_kube_config()
# __batch_v1 = client.BatchV1Api()


def generate_jobs(
    running_job_num: int, total_job_num, server_num, worker_num, active_idle_ps_num, batch_size=128, epoch_num=1, port=29600
):
    global __JOB_TIMES
    # global __batch_v1
    logger = Logger(logger_name="Generate_Jobs_Logger", file_dir="./logs/generate_jobs.log").logger
    logger.info("Start!")
    job_to_add_id = 0
    running_jobs = set()
    for _ in range(running_job_num):
        id = job_to_add_id
        job_to_add_id += 1
        running_jobs.add(id)
        generate_job_yaml(
            f"job{id}",
            get_model_name(),
            server_num,
            worker_num,
            active_idle_ps_num,
            batch_size=batch_size,
            epoch_num=get_epoch(),
            port=port,
        )
        if job_to_add_id >= total_job_num:
            break
    start_time = time.time()
    for id in running_jobs:
        os.system(f"kubectl apply -f ./yaml/job{id}.yaml")
        logger.info(f"Job{id} added. Exp running time: {(time.time() - start_time):.3f} s.")
        __JOB_TIMES[f"job{id}"] = time.time()
        time.sleep(20)
    while True:
        time.sleep(3)
        completed_jobs = []
        for job_id in running_jobs:
            status = zos.run_cmd(f"kubectl get pod | grep job{job_id}- | awk '{{print $3}}'").split("\n")
            finished = True
            for s in status:
                if s != "Completed":
                    finished = False
                    # if s == "Error":
                    #     finished = True
                    break
            for s in status:
                if s == "Error":
                    finished = True
                    break
            if finished:
                completed_jobs.append(job_id)

        # for job_id in running_jobs:
        #     count = 0
        #     for i in range(worker_num):
        #         worker_id = i + 1
        #         name = f"job{job_id}-worker{worker_id}"
        #         api_response = __batch_v1.read_namespaced_job_status(namespace="default", name=name)
        #         if api_response.status.succeeded is not None:
        #             count += 1
        #     if count == worker_num:
        #         completed_jobs.append(job_id)

        # clean completed jobs
        for id in completed_jobs:
            os.system(f"kubectl delete -f ./yaml/job{id}.yaml")
            running_jobs.discard(id)
            job_name = f"job{id}"
            logger.info(f"Job{id} finished. Exp running time: {(time.time() - start_time):.3f} s.")
            logger.info(f"Job{id} running time is: {(time.time() - __JOB_TIMES[job_name]):.3f} s.")

        # add new jobs
        add_num = min(running_job_num - len(running_jobs), total_job_num - job_to_add_id)
        if add_num > 0:
            for _ in range(add_num):
                id = job_to_add_id
                job_to_add_id += 1
                running_jobs.add(id)
                generate_job_yaml(
                    f"job{id}",
                    get_model_name(),
                    server_num,
                    worker_num,
                    active_idle_ps_num,
                    batch_size=batch_size,
                    epoch_num=get_epoch(),
                    port=port,
                )
                os.system(f"kubectl apply -f ./yaml/job{id}.yaml")
                logger.info(f"Job{id} added. Exp running time: {(time.time() - start_time):.3f} s.")
                __JOB_TIMES[f"job{id}"] = time.time()

        # check complete status
        if len(running_jobs) == 0:
            print("All jobs complete.")
            logger.info("All jobs complete.")
            logger.info(f"Total time: {(time.time() - start_time):.3f} s.")
            return


if __name__ == "__main__":
    generate_jobs(5, 8, 4, 4, 8)
    # generate_job_yaml("job0", "resnet56", 4, 4, 8)
#     parser = argparse.ArgumentParser(description="Generate yaml file")
#     parser.add_argument("--job_name", type=str, default="vgg16", help="The job's name.")
#     parser.add_argument("--model_name", type=str, default="vgg16", help="The model's name.")
#     parser.add_argument("--num_workers", type=int, default=4, help="Total number of workers.")
#     parser.add_argument("--ss", type=str, default="asgd", help="Learning rate.")
#     parser.add_argument("--batch_size", type=int, default=128, help="Batch size of each worker during training.")
#     parser.add_argument("--lr", type=float, default=0.005, help="Learning rate.")

#     args = parser.parse_args()

#     # job_name = ['resnet20', 'resnet56', 'vgg13', 'vgg16', 'densenet121', 'alexnet', 'googlenet', 'mobilenet']
#     job_name = args.job_name
#     model_name = job_name
#     num_workers = args.num_workers
#     ss = args.ss
#     batch_size = args.batch_size
#     lr = args.lr
#     generate_job_yaml(job_name=job_name, model_name=model_name, num_workers=num_workers, ss=ss, batch_size=batch_size, lr=lr)
#     os.system(f"sudo kubectl apply -f yaml/{job_name}_{ss}.yaml")
