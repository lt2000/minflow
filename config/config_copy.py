COUCHDB_URL = 'http://openwhisk:openwhisk@192.168.1.116:5984/'
REDIS_HOST = '127.0.0.1' # it serves to connect with the local redis, so it should be 127.0.0.1
REDIS_PORT = 8002 # it follows the same configuration as created redis by docker (e.g., -p 6379:6379)
REDIS_DB = 0
GATEWAY_ADDR = '192.168.1.116:7000' # need to update as your private_ip
MASTER_HOST = '192.168.1.116:8001' # need to update as your private_ip
WORKFLOW_YAML_ADDR = {'fileprocessing': '/home/k8s/little/faasflow/benchmark/fileprocessing/flat_workflow.yaml',
                  'illgal_recognizer': '/home/k8s/little/faasflow/benchmark/illgal_recognizer/flat_workflow.yaml',
                  'video': '/home/k8s/little/faasflow/benchmark/video/flat_workflow.yaml',
                  'wordcount': '/home/k8s/little/faasflow/benchmark/wordcount/flat_workflow.yaml',
                  'cycles': '/home/k8s/little/faasflow/benchmark/generator/cycles/flat_workflow.yaml',
                  'epigenomics': '/home/k8s/little/faasflow/benchmark/generator/epigenomics/flat_workflow.yaml',
                  'genome': '/home/k8s/little/faasflow/benchmark/generator/genome/flat_workflow.yaml',
                  'soykb': '/home/k8s/little/faasflow/benchmark/generator/soykb/flat_workflow.yaml'}
NETWORK_BANDWIDTH = 25 * 1024 * 1024 / 4 # 25MB/s / 4
NET_MEM_BANDWIDTH_RATIO = 15 # mem_time = net_time / 15
CONTAINER_MEM = 256 * 1024 * 1024 # 256MB
NODE_MEM = 128 * 1024 * 1024 * 1024 # 256G
RESERVED_MEM_PERCENTAGE = 0.2
GROUP_LIMIT = 100
RPMs = {'genome-25': [2, 4, 6, 8], 'genome-50': [2, 4, 6, 8, 10], 'genome-75': [2, 4, 6, 8, 10], 'genome-100': [2, 4, 6, 8, 10],
'video-25': [4, 8, 16, 24], 'video-50': [8, 16, 24, 32, 40], 'video-75': [8, 16, 24, 32, 40], 'video-100': [8, 16, 24, 32, 40]}
FUNCTION_INFO_ADDRS = {'genome': '../../benchmark/generator/genome', 'epigenomics': '../../benchmark/generator/epigenomics',
                                                'soykb': '../../benchmark/generator/soykb', 'cycles': '../../benchmark/generator/cycles',
                                                'fileprocessing': '../../benchmark/fileprocessing', 'wordcount': '../../benchmark/wordcount',
                                                'illgal_recognizer': '../../benchmark/illgal_recognizer', 'video': '../../benchmark/video'}
DATA_MODE = 'optimized' # raw, optimized
CONTROL_MODE = 'WorkerSP' # WorkerSP, MasterSP
CLEAR_DB_AND_MEM = True
