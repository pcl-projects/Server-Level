import time

import numpy as np

import pickle as cPickle
import matplotlib.pyplot as plt
import os
os.environ['KERAS_BACKEND'] = 'theano'

from threading import Thread as Process
from multiprocessing import Process
from multiprocessing import Manager
from multiprocessing import Lock

# import theano, theano.tensor as T
import environment
import job_distribution
import pg_network
# import pandas as pd

import socket
import os
from _thread import *

ServerSocket = socket.socket()

host = 'server1'
port = 30000


list_server = ['server1', 'server2', 'server3', 'server4', 'server5', 'server6', 'server7', 'server8', 'server9', 'server10', 'server11', 'server12', 'server13', 'server14', 'server15', 'server16', 'server17', 'server18', 'server19', 'server20', 'server21', 'server22', 'server23', 'server24', 'server25', 'server26', 'server27', 'server28', 'server29', 'server30']
# try:
#     ServerSocket.bind((host, port))
# except socket.error as e:
#     print(str(e))

# print('Waitiing for a Connection..')
# ServerSocket.listen(5)


def threaded_client(connection):
    thread_starter = True
    # connection.send(str.encode('Welcome to the Server'))
    # while True:
    #     data = connection.recv(2048)
    #     reply = 'Server Says: ' + data.decode('utf-8')
    #     if not data:
    #         break
    #     connection.sendall(str.encode(reply))
    # connection.close()


# number_of_client = 0
# client_record = []

# while True:
#     Client, address = ServerSocket.accept()
#     print('Connected to: ' + address[0] + ':' + str(address[1]))
#     start_new_thread(threaded_client, (Client, ))
#     ThreadCount += 1
#     print('Thread Number: ' + str(ThreadCount))
#     client_record.append(Client)

#     number_of_client = number_of_client + 1

#     if number_of_client == 2:
#         break
    # Client.send(b"HELLO, How are you ? \
    #    Welcome to Akash hacking World")
# ServerSocket.close()


# take a job
# job = pd.read_csv('task_draft.csv')
from multiprocessing import Process
import subprocess
import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
import pickle
from keras.models import load_model
import random
#import GPUtil as GPU
import csv


def f(command):
    subprocess.call(command, shell=True)


def get_resource(cpu_res, mem_res, gpu_res, band_res, current):
    cluster_dict = {}
    center_dict = {}
    clusters_id_dict = {}
    cpu_res = np.array(cpu_res)
    mem_res = np.array(mem_res)
    gpu_res = np.array(gpu_res)
    band_res = np.array(band_res)
    # print(cpu_res)
    # print(container)
    # print(dfcpu.iloc[0, :])
    model_avail_cpu = load_model('cpu_avail_micro_trace.h5', compile=False)
    model_avail_mem = load_model('mem_avail_micro_trace.h5', compile=False)
    model_avail_gpu = load_model('gpu_avail_micro_trace.h5', compile=False)
    model_avail_band = load_model('band_avail_micro_trace.h5', compile=False)
    # model_avail_band = load_model('band_avail.h5', compile=False)
    last = 4

    for _ in range(5):
        lx = []
        for item in range(30):
            for_p = cpu_res[0:5, item]
            for_p = for_p.reshape(1, 5, 1)
            x = np.concatenate(model_avail_cpu.predict(for_p))[0]
            lx.append(x)

        cpu_res[0:4, :] = cpu_res[1:5, :]
        cpu_res[last, :] = np.array(lx)

    # print(cpu_res)

    for _ in range(5):
        lx = []
        for item in range(30):
            for_p = mem_res[0:5, item]
            for_p = for_p.reshape(1, 5, 1)
            x = np.concatenate(model_avail_mem.predict(for_p))[0]
            lx.append(x)

        mem_res[0:4, :] = mem_res[1:5, :]
        mem_res[last, :] = np.array(lx)


    for _ in range(5):
        lx = []
        for item in range(30):
            for_p = gpu_res[0:5, item]
            for_p = for_p.reshape(1, 5, 1)
            x = np.concatenate(model_avail_gpu.predict(for_p))[0]
            lx.append(x)

        gpu_res[0:4, :] = gpu_res[1:5, :]
        gpu_res[last, :] = np.array(lx)

    for _ in range(5):
        lx = []
        for item in range(30):
            for_p = band_res[0:5, item]
            for_p = for_p.reshape(1, 5, 1)
            x = np.concatenate(model_avail_band.predict(for_p))[0]
            lx.append(x)

        band_res[0:4, :] = band_res[1:5, :]
        band_res[last, :] = np.array(lx)

    # print(cpu_res)
    # print(mem_res)

    # for i in range(5):
    #     X = cpu_res[i, :]
    #     # print(len(X))
    #     Y = mem_res[i, :]
    #     # print(len(Y))

    #     dataset = np.zeros((30, 15))

    #     dataset[:, 0] = X
    #     dataset[:, 1] = Y

    #     # print(dataset)

    #     Kmean = KMeans(n_clusters=5, max_iter=1000)
    #     Kmean.fit(dataset)

    #     label = Kmean.labels_
    #     clusters = {}
    #     clusters_id = {}
    #     n = 0
    #     for item in label:
    #         if item in clusters:
    #             clusters[item].append(dataset[n, :])
    #             clusters_id[item].append(n)
    #         else:
    #             clusters[item] = [dataset[n, :]]
    #             clusters_id[item] = [n]
    #         n += 1

    #     cluster_dict[i] = clusters
    #     clusters_id_dict[i] = clusters_id
    #     # print("\n")
    #     # print(cluster_dict[i])
    #     # for item in clusters:
    #     #     print("Cluster ", item)
    #     #     print(len(clusters[item]))
    #     #     for i in clusters[item]:
    #     #         print(i)
    #     centers_c = np.array(Kmean.cluster_centers_)
    #     center_dict[i] = centers_c

    dataset = np.zeros((30, 15))

    for i in range(30):
        X = cpu_res[:, i]
        Y = mem_res[:, i]
        Z = gpu_res[:, i]
        W = band_res[:, i]

        dataset[i, 0:5] = X
        dataset[i, 5:10] = Y
        dataset[i, 10:15] = Z
        dataset[i, 15:20] = W

    Kmean = KMeans(n_clusters=5, max_iter=1000)
    Kmean.fit(dataset)

    label = Kmean.labels_

    clusters = {}
    clusters_id = {}
    n = 0
    for item in label:
        if item in clusters:
            clusters[item].append(dataset[n, :])
            clusters_id[item].append(n)
        else:
            clusters[item] = [dataset[n, :]]
            clusters_id[item] = [n]
        n += 1

    centers_c = np.array(Kmean.cluster_centers_)


        # print(center_dict[i])

    # print(cluster_dict)
    # print(center_dict)
    # print(clusters_id_dict)
    # return center_dict, cluster_dict, clusters_id_dict, cpu_res, mem_res

    return centers_c, clusters, clusters_id, cpu_res, mem_res


def task_assign(client_record):
    # Name	Category	size	Epoch	trainsize	testsize	mem	probmem	io	gpu	gpumem	cpu	probcpu
    
    job_arrival_df = pd.read_csv('trace_arrival.csv')
    job_arrival_df = pd.DataFrame(job_arrival_df)
    job_arrival_df = job_arrival_df.iloc[:, 0]

    task_for_run = pd.read_csv('micro_job.csv')
    first_time = True
    
    dfcpu = pd.read_csv('cpu_usage.csv')

    dfmem = pd.read_csv('mem_avail.csv')

    dfgpu = pd.read_csv('gpu_avail.csv')

    # host_number = [12345, 12350]
    cpu_con = np.array(dfcpu.iloc[0:5, 0:30])
    mem_con = np.array(dfmem.iloc[0:5, 0:30])
    gpu_con = np.array(dfgpu.iloc[0:5, 0:30])

    server_dict = {}
    client_help_dict = {}
    
    # # gpu_con = np.array(dfgpu.iloc[0:6, 0:30])
    # # mem_con = np.array(dfmem.iloc[0:6, 0:30])


    port = 10000
    count = 0
    # centers, reclusters, cpu_con, mem_con = get_resource(cpu_con, mem_con, 2)
    # # number_of_task = 1
    # for k in range(5):
    #     cluster_mem_adjust = (centers[k, 5:10]) * 128000
    #     if len(reclusters[k]) >= (number_of_task + 1) and np.all(cluster_mem_adjust >= 1200):
    #         cluster_cpu_adjust = (1 - centers[k, 0:5]) * 2000.0
    #         print("checked")
    #         print(len(reclusters[k]))
    while len(run_jobs) < 10:
        # gpu_count = 0
        # list_gpu = []
        # avai = GPU.getAvailability(GPU.getGPUs())
        # for gpu_index in range(len(avai)):
        #     if avai[gpu_index] == 1:
        #         list_gpu.append(gpu_index)

        centers, reclusters, cluster_id, cpu_con, mem_con = get_resource(cpu_con, mem_con, gpu_con, 2)
        d_e_diff = np.zeros(4)
        # job_batch = np.zeros(4)

        # for n in range(4):
        #     # d_time = np.random.randint(50, 150)
        #     d_e_diff[n] = np.random.randint(200, 400)
        #     # row_n = np.random.randint(2, 200)
        #     job_batch[n] = job_array[count]  # np.random.randint(4, 200)
        #     count = count + 1

        # sorted_d_e = np.argsort(d_e_diff)

        # for i in range(4):

        #     if i == 0:
        #         data_row = task_test2
        #         deadline = deadline_test2
        #     if i == 1:
        #         data_row = task_test2
        #         deadline = deadline_test2

        #     job_n = job_batch[sorted_d_e[i]]
        #     print(job_n)
        data_row = np.array(task_for_run.iloc[int(itera), :])

        # data_row = np.array(task_for_run.iloc[i, :])
        # print(data_row)
        file_name = str(data_row[0]) 
        file_n = data_row[0]
        name_need = data_row[1]
        category_need = data_row[2]
        size_of_data = data_row[3]
        epoch = data_row[4]
        train_size = data_row[5]
        test_size = data_row[6]
        cpu_task = data_row[8]
        task_time = data_row[7]  ### reconsider
        mem_task = data_row[9]
        task_io = data_row[10]
        task_gpu = data_row[11]

   
        task_mem_adjust = mem_task #/ float(number_of_task) + 750

        final_proba = -1111
        final_exec = None
        temp_prob = -1
        temp_exec = None
        utility = -999999
        deadline = exe_time + random.uniform(100,300)


        for k in range(0,5):
            cluster_mem_adjust = (centers[k, 5:10]) * 128000
            if len(reclusters[k]) >= (task_gpu) and np.all(cluster_mem_adjust >= task_mem_adjust):
                cluster_cpu_adjust = (100 - centers[k, 0:5]) * 14
                print(cluster_cpu_adjust)
                #print(len(reclusters[k]))
                with open('execution_model.pkl', 'rb') as f1:
                    exe_time_model = pickle.load(f1)
                with open('prob_model.pkl', 'rb') as f2:
                    prob_model = pickle.load(f2)

                mem_prob = round(random.uniform(.6, .99), 2)
                cpu_prob = round(random.uniform(.7, .99), 2)
                new_X = np.array([name_need, category_need, size_of_data, epoch,train_size, test_size, mem_task, mem_prob,task_io, 0, 0, cluster_cpu_adjust[0], cpu_prob])
                new_X = np.array(new_X).reshape(1, -1)
                exe_time = exe_time_model.predict(new_X)
                print(exe_time[0])



                new_X_p = np.array([mem_task, mem_prob, cluster_cpu_adjust[0], cpu_prob])
                new_X_p = np.array(new_X_p).reshape(1, -1)
                proba = prob_model.predict(new_X_p)
                #proba = round(random.uniform(.6, .99), 2)

                
                if proba[0] >= final_proba and (proba[0]-exe_time[0])> utility and exe_time[0] < deadline:
                    final_proba = proba[0]
                    final_exec = exe_time[0]
                    # index_j = j
                    index_k = k
                    utility = (proba[0]-exe_time[0])
                    print("\n changed ")

        print("\n selected "+str(index_k))

        container_id = cluster_id[index_k]
        ps_server = list_server[container_id[0]]
        w1_server = list_server[container_id[1]]
        w2_server = list_server[container_id[2]]
        run_jobs.append(1)
   
        # port = port + 1 
        command_ps = 'python all_job/'+'cifar10_'+file_name+'_w0.py --job_name=ps --task_id=0 --ps_hosts='+ps_server+':'+str(port+1)+' --worker_hosts='+w1_server+':'+str(port+2)+","+w2_server+':'+str(port+3)+' --max_steps='+str(epoch)+" --job_number="+str(itera)+" --deadline="+str(int(deadline))+' --method=my'
        client_record[0].sendall(str.encode(command_ps))

        time.sleep(5)


        for w in range(task_gpu):

            command_worker1 = 'python all_job/'+'cifar10_'+file_name+'_w0.py --job_name=worker --task_id=0 --ps_hosts='+ps_server+':'+str(port+1)+' --worker_hosts='+w1_server+':'+str(port+2)+","+w2_server+':'+str(port+3)+' --max_steps='+str(epoch)+" --job_number="+str(itera)+" --deadline="+str(int(deadline))+' --method=my'
            client_record[(container_id[w])].sendall(str.encode(command_worker1))

            time.sleep(5)
        
            # command_worker2 = 'python all_job/'+'cifar10_'+file_name+'_w0.py --job_name=worker --task_id=1 --ps_hosts='+ps_server+':'+str(port+1)+' --worker_hosts='+w1_server+':'+str(port+2)+","+w2_server+':'+str(port+3)+' --max_steps='+str(epoch)+" --job_number="+str(itera)+" --deadline="+str(int(deadline))+' --method=my'
            # client_record[(container_id[2]%7)].sendall(str.encode(command_worker2))


            print(job_arrival_df[itera])
            time.sleep(job_arrival_df[itera])


        # command = "singularity exec instance://" + dn + " python " + file_name + " " + all_arg
        #     # print(command)
        # p = Process(target=f, args=(command,))
        # p.start()


                # if final_proba == -1:
                #     if proba[0] > temp_prob:
                #         temp_prob = proba[0]
                #         temp_exec = exe_time[0]
                #         # index_j = j
                #         index_k = k



        # command_ps = "python alexnet_job/cifar10_alex_ps.py --job_name=ps --task_id=0 --ps_hosts='localhost:20000' --worker_hosts='localhost:20001'"
        # client_record[0].sendall(str.encode(command_ps))

        # command_worker = "python alexnet_job/cifar10_alex_w0.py --job_name=worker --task_id=0 --ps_hosts='localhost:20000' --worker_hosts='localhost:20001'"
        # client_record[1].sendall(str.encode(command_worker))

                # print(cpu_con, mem_con)
            # print(centers)
            # for u in range(5):
            #     print(centers[0][u, 0])
            # deadline = task_time + random.uniform(50, 100)

            # print(len(reclusters[0][1]))

            # for k in range(5):
            #     print(reclusters[k])
            #     print("\n")
            # print(len(reclusters[0]))
            

            # for j in range(5):
            #     for k in range(5):
            #         cluster_mem_adjust = (1 - centers[j][k, 1]) * 4000
            #         if len(reclusters[j][k]) >= (number_of_task + 1) and cluster_mem_adjust >= task_mem_adjust:
            #             cluster_cpu_adjust = (1 - centers[j][k, 0]) * 400.0
            #             with open('execution_model.pkl', 'rb') as f1:
            #                 exe_time_model = pickle.load(f1)
            #             with open('prob_model.pkl', 'rb') as f2:
            #                 prob_model = pickle.load(f2)

            #             mem_prob = round(random.uniform(.6, .99), 2)
            #             cpu_prob = round(random.uniform(.7, .99), 2)
            #             new_X = np.array([name_need, category_need, size_of_data, epoch,
            #                               train_size, test_size, mem_task, mem_prob,
            #                               task_io, 0, 0, cluster_cpu_adjust, cpu_prob])
            #             new_X = np.array(new_X).reshape(1, -1)
            #             exe_time = exe_time_model.predict(new_X)

            #             new_X_p = np.array([mem_task, mem_prob, cluster_cpu_adjust, cpu_prob])
            #             new_X_p = np.array(new_X_p).reshape(1, -1)
            #             proba = prob_model.predict(new_X_p)

            #             # print('task time exec time pro dead', task_time, exe_time[0], proba[0], deadline)

            #             if proba[0] > final_proba and exe_time[0] < deadline:
            #                 final_proba = proba[0]
            #                 final_exec = exe_time[0]
            #                 index_j = j
            #                 index_k = k
            #             if final_proba == -1:
            #                 if proba[0] > temp_prob:
            #                     temp_prob = proba[0]
            #                     temp_exec = exe_time[0]
            #                     index_j = j
            #                     index_k = k

            # print('\n')
            # print(final_proba, final_exec)
            # print(temp_prob, temp_exec)
            # print(index_j, index_k)
            # print(reclusters[index_j][index_k])
            # container_id = []
            # task_n = number_of_task
            # print(cpu_con)
            # for l in range(len(cluster_id[index_j][index_k])):
            #     print(cluster_id[index_j][index_k])
            #     index = cluster_id[index_j][index_k][l]
            #     print(index)
            #     # print(cpu_con[index_j])
            #     # con_num = np.where(cpu_con[index_j] == index[0])[0][0]
            #     # print(con_num)
            #     container_id.append(index[0])
        # number_of_task = 1
        # for k in range(5):
        #     cluster_mem_adjust = (centers[k, 5:10]) * 128000
        #     if len(reclusters[k]) >= (number_of_task + 1) and np.all(cluster_mem_adjust >= 1200):
        #         cluster_cpu_adjust = (1 - centers[k, 0:5]) * 2000.0
        #         print("checked")
        #         print(len(reclusters[k]))
        #         with open('execution_model.pkl', 'rb') as f1:
        #             exe_time_model = pickle.load(f1)
        #         with open('prob_model.pkl', 'rb') as f2:
        #             prob_model = pickle.load(f2)

        #         mem_prob = round(random.uniform(.6, .99), 2)
        #         cpu_prob = round(random.uniform(.7, .99), 2)
        #         new_X = np.array([name_need, category_need, size_of_data, epoch,
        #                           train_size, test_size, mem_task, mem_prob,
        #                           task_io, 0, 0, cluster_cpu_adjust, cpu_prob])
        #         new_X = np.array(new_X).reshape(1, -1)
        #         exe_time = exe_time_model.predict(new_X)



        #         new_X_p = np.array([mem_task, mem_prob, cluster_cpu_adjust, cpu_prob])
        #         new_X_p = np.array(new_X_p).reshape(1, -1)
        #         proba = prob_model.predict(new_X_p)

        #         if proba[0] > final_proba and exe_time[0] < deadline and (proba[0]-exe_time[0])> utility:
        #             final_proba = proba[0]
        #             final_exec = exe_time[0]
        #             # index_j = j
        #             index_k = k
        #         if final_proba == -1:
        #             if proba[0] > temp_prob:
        #                 temp_prob = proba[0]
        #                 temp_exec = exe_time[0]
        #                 # index_j = j
        #                 index_k = k



        #     container_id = cluster_id[index_k]
        #     worker_list = []
        #     ps_list = []    
            # print(container_id)
            # for up in range(len(container_id)):

            #     if task_n != 0 and (1 - mem_con[index_j, container_id[up]]) * 4000 >= task_mem_adjust:
            #         left_percent = ((1 - mem_con[index_j, container_id[up]]) * 4000 - task_mem_adjust) / 4000.0
            #         # print(mem_con[index_j, container_id[up]])
            #         mem_con[index_j, container_id[up]] = 1 - left_percent
            #         # print(mem_con[index_j, container_id[up]])
            #         if cpu_con[index_j, container_id[up]] > 1:
            #             cpu_con[index_j, container_id[up]] = cpu_con[index_j, container_id[up]] / 10.0
            #         if (1 - cpu_con[index_j, container_id[up]]) * 400 > cpu_task:
            #             left = ((1 - cpu_con[index_j, container_id[up]]) * 400 - cpu_task) / 400.0
            #             cpu_con[index_j, container_id[up]] = 1 - left
            #         else:
            #             cpu_con[index_j, container_id[up]] = 1

            #         task_n = task_n - 1
            #         worker_list.append(container_id[up])
            #     else:
            #         ps_list.append(container_id[up])

                # if task_n == 0:
                #     break

            # print(worker_list)
            # print(ps_list)
            # for ps in range(len(ps_list)):
            # if (1 - mem_con[index_j, ps_list[ps]]) * 4000 >= 750:
            #     left_percent = ((1 - mem_con[index_j, ps_list[ps]]) * 4000 - 750) / 4000.0
            #     mem_con[index_j, ps_list[ps]] = 1 - left_percent
        #     dn = "g" + str(ps_list[ps])
        #     arg1 = number_of_task + 1
        #     arg2 = 0
        #     arg3 = epoch
        #     arg4 = int(number_of_sample / number_of_task)
        #     arg5 = -1
        #     arg6 = number_of_task
        #     arg7 = port
        #     arg8 = list_gpu[gpu_count]
        #     all_arg = str(arg1) + " " + str(arg2) + " " + str(arg3) + " " + str(arg4) + " " + str(
        #         arg5) + " " + str(arg6) + " " + str(arg7) + " " + str(arg8)
        #     # print(dn)
        #     # docker exec - i d2 python3 test27.py 3 1 5 150 1 2

        #     command = "singularity exec instance://" + dn + " python " + file_name + " " + all_arg
        #     # print(command)
        #     p = Process(target=f, args=(command,))
        #     p.start()
        #         # break

        #     for worker in range(len(worker_list)):
        #         dn = "g" + str(worker_list[worker])
        #         arg1 = number_of_task + 1
        #         arg2 = worker + 1
        #         arg3 = epoch
        #         arg4 = int(number_of_sample / number_of_task)
        #         arg5 = worker
        #         arg6 = number_of_task
        #         arg7 = port
        #         arg8 = list_gpu[gpu_count]
        #         all_arg = str(arg1) + " " + str(arg2) + " " + str(arg3) + " " + str(arg4) + " " + str(arg5) + " " + str(
        #             arg6) + " " + str(arg7) + " " + str(arg8)
        #         # print(dn)
        #         # docker exec - i d2 python3 test27.py 3 1 5 150 1 2
        #         if job_n > 100:
        #             gpu_count = gpu_count + 1
        #             command = "singularity exec --nv instance://" + dn + " python " + file_name + " " + all_arg
        #         elif job_n < 100:
        #             command = "singularity exec instance://" + dn + " python " + file_name + " " + all_arg
        #         # print(command)
        #         p = Process(target=f, args=(command,))
        #         p.start()
        #     # print("job ass")
        #     time_slot = itera * 25 + (index_j + 1) * 5
        #     # print(time_slot)
        #     # writing result to file

        #     arrival_time = random.uniform(1, 25)

        #     time_data = []
        #     name = file_n + str(port)
        #     time_data.append(name)
        #     time_data.append(time_slot)
        #     time_data.append(arrival_time)
        #     time_data.append(deadline)
        #     time_data.append(int(job_n))
        #     # time_data.append(skip)
        #     # time_data.append('end')
        #     data_csv = []
        #     data_csv.append(time_data)
        #     import csv
        #     with open('task_slot.csv', 'a', newline='') as file:
        #         writer = csv.writer(file)
        #         writer.writerows(data_csv)
        #     port = port + 15

        # # print('before sleep')
        # time.sleep(800)
        # print(container[index_j].index(reclusters[index_j][index_k]))
        # print(container/)
        # print(index_j)

        # print(len(reclusters[j][k]))

        # print(round(random.uniform(.6, 1), 2))
        # print(cluster_cpu_adjust)

        # for j in range(5):
        #     if len(reclusters[j])

    # print(name_need, category_need, size_of_data, epoch, train_size, test_size, cpu_task, mem_task, task_io,
    #       task_gpu, number_of_task, number_of_sample)
    # print(Kmean.cluster_centers_)
    # print(Kmean.labels_)

    ################# arrival rate ##########################

    # import random

    # arrival_time = []
    #
    # for i in range(5):
    #     arrival_time.append(random.uniform(1, 5))
    #     # print(arrival_time)
    #
    # arrival_time = np.sort(np.array(arrival_time))
    # print(arrival_time)

comp_jobs =[]
run_jobs =[]
def exec():
    file_read_status = 'Archive/complete_status.csv'

    df = pd.read_csv(file_read_status, header=None)
        # print(df_itn)
    if len(df.index)>0:
        index_job_name = pd.DataFrame(df.values[:,0])
        # print(itn_time_all)

        index_worker = pd.DataFrame(df.values[:,1])

        index_status = pd.DataFrame(df.values[:,2])
        # print(itn_time_all)

        index_job_name = index_job_name.to_numpy()

        # index_worker =index_worker.to_numpy()

        index_status = index_status.to_numpy()

        for job_name in index_job_name:
            comp_jobs.append(1)
        if len(run_jobs) > 0:
            run_jobs.pop(run_jobs[len(run_jobs)-1])
               

def sched(client_record):
    while True:
        task_assign(client_record)
        exec()

def main():
    ThreadCount = 0

    try:
        ServerSocket.bind((host, port))
    except socket.error as e:
        print(str(e))

    print('Waitiing for a Connection..')
    ServerSocket.listen(30)

    number_of_client = 0
    client_record = []

    while True:
        Client, address = ServerSocket.accept()
        print('Connected to: ' + address[0] + ':' + str(address[1]))
        start_new_thread(threaded_client, (Client, ))
        ThreadCount += 1
        print('Thread Number: ' + str(ThreadCount))
        client_record.append(Client)

        number_of_client = number_of_client + 1

        if number_of_client == 30:
            break

    # task_assign(client_record)
    sched(client_record)
    # task_assign()


if __name__ == '__main__':
    main()
    
