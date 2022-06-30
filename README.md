# Code for Straggler-Avoiding Job Scheduling and Distributed PS Load Reassignment

This repo contains the source code for straggler-avoiding job scheduling (SAS) and distributed PS load reassignment with PS-level and server-level self-managing strategies.

Under ./sched_code/ contains the code for straggler-avoiding job scheduling (SAS), which is run in machines without Kubernetes.

Under ./ps_level_server_level/ contains the code for distributed PS load reassignment with PS-level and server-level self-managing strategies.

Code in ./ps_level_server_level/for_kubernetes/ is run in machines with Kubernetes while code in ./ps_level_server_level/for_native/ is run in machines without kubernetes.
