import psutil
import socket
import gpustat
import time
import argparse
import logging


def get_cpu_memory_gpu():
    cpu_usage = psutil.cpu_percent(interval=1)

    virtual_memory = psutil.virtual_memory()
    memory_usage = virtual_memory.used / 1024 / 1024 / 1024

    try:
        query = gpustat.new_query()
    except:
        gpu_memory = 0
        gpu_util = 0
    else:
        gpu = query.gpus[0]
        gpu_memory = 100. * gpu.memory_used / gpu.memory_total
        gpu_util = gpu.utilization

    return cpu_usage, memory_usage, gpu_memory, gpu_util


class Logger(object):
    def __init__(self, job_name, file_dir, log_level=logging.INFO, mode='w'):
        self.logger = logging.getLogger(job_name)
        self.logger.setLevel(log_level)
        self.fh = logging.FileHandler(filename=file_dir, mode=mode)
        self.formatter = logging.Formatter('%(asctime)s - %(name)s - %(message)s')
        self.fh.setFormatter(self.formatter)
        self.logger.addHandler(self.fh)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Worker information")
    parser.add_argument("--interval", type=float, default=1.0, help="Interval")
    parser.add_argument("--file_dir", type=str, default="./measurement/logs/info.log", help="The log location.")
    parser.add_argument("--job_name", type=str, help="The job's name.")

    args = parser.parse_args()

    logger = logging.getLogger('info')
    logger.setLevel(logging.INFO)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    fh = logging.FileHandler(args.file_dir, mode='w')
    fh.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(message)s')
    ch.setFormatter(formatter)
    fh.setFormatter(formatter)
    logger.addHandler(ch)
    logger.addHandler(fh)

    memory_usage = psutil.virtual_memory().used / 1024 / 1024 / 1024

    bw = psutil.net_io_counters(pernic=True)

    nic = socket.if_nameindex()[1][1]
    logger.info(nic)
    temp_recv = bw[nic].bytes_recv
    temp_sent = bw[nic].bytes_sent
    counter = 0

    t0 = time.time()
    while True:
        t1 = time.time()
        if t1 - t0 > args.interval:
            t0 = t1
            cpu_usage, memory_usage, gpu_memory, gpu_util = get_cpu_memory_gpu()
            bw = psutil.net_io_counters(pernic=True)

            logger.info(
                "cpu_usage: {:6.2f}% | memory_usage: {:5.2f} GB | gpu_memory: {:3.0f}% | gpu_util: {:3d}% "
                "| Bandwidth | recv: {:7.2f} MB | sent: {:7.2f} MB"
                    .format(cpu_usage, memory_usage, gpu_memory, gpu_util,
                            (bw[nic].bytes_recv - temp_recv) / 1024. / 1024.,
                            (bw[nic].bytes_sent - temp_sent) / 1024. / 1024.))

            temp_recv = bw[nic].bytes_recv
            temp_sent = bw[nic].bytes_sent
