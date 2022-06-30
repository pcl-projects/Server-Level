# Straggler-Avoiding Job Scheduling and PS-Level and Server-Level PS Load Self-Managing

This repo contains the source code for straggler-avoiding job scheduling (SAS) and PS-level and server-level PS load self-managing.

Under ./sched_code/ contains the code for straggler-avoiding job scheduling (SAS), which is run in machines without Kubernetes.

Under ./ps_level_server_level/ contains the code for PS-level and server-level PS load self-managing.

Code in ./ps_level_server_level/for_kubernetes/ is run in machines with Kubernetes while code in ./ps_level_server_level/for_native/ is run in machines without kubernetes.
