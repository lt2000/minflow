import yaml
import component
import sys
import network
sys.path.append('../../config')
import config
yaml_file_addr = config.WORKFLOW_YAML_ADDR


def print_workflow(nodes):
    for name in nodes:
        print('function name: ', nodes[name].name)
        print('function prev: ', nodes[name].prev)
        print('function next: ', nodes[name].next)
        print('function nextDis: ', nodes[name].nextDis)
        print('function source: ', nodes[name].source)
        print('function runtime: ', nodes[name].runtime)
        print('\n====================================')
        print('====================================\n')

    pass

def parse(workflow_name):
    data = yaml.load(
        open(yaml_file_addr[workflow_name]), Loader=yaml.FullLoader)
    global_input = dict()
    start_functions = []
    nodes = dict()
    parent_cnt = dict()
    shuffle_functions = set()
    foreach_functions = set()
    merge_funtions = set()   # ???
    total = 0
    if 'global_input' in data:
        for key in data['global_input']:
            parameter = data['global_input'][key]['value']['parameter']
            global_input[parameter] = data['global_input'][key]['size']
    functions = data['functions']
    parent_cnt[functions[0]['name']] = 0     # start function
    for function in functions:
        name = function['name']
        source = function['source']
        runtime = function['runtime']
        input_files = dict()
        output_files = dict()
        next = list()
        nextDis = list()
        send_byte = 0
        if 'input' in function:
            for key in function['input']:
                input_files[key] = {'function': function['input'][key]['value']['function'],
                                    'parameter': function['input'][key]['value']['parameter'],
                                    'size': function['input'][key]['size'], 'arg': key,
                                    'type': function['input'][key]['type']}
        if 'output' in function:
            for key in function['output']:
                output_files[key] = {'size': function['output'][key]
                                     ['size'], 'type': function['output'][key]['type']}
                send_byte += function['output'][key]['size']
        send_time = send_byte / config.NETWORK_BANDWIDTH
        conditions = list()
        if 'next' in function:
            foreach_flag = False
            reducer_foreach_num = 0
            if function['next']['type'] == 'switch':
                conditions = function['next']['conditions']
            elif function['next']['type'] == 'foreach':
                foreach_flag = True
            elif function['next']['type'] == 'shuffle':
                for f in function['next']['nodes']:
                    shuffle_functions.add(f)
                net = network.min_generator(function['next']['number'])
                foreach_flag = True
                if name in shuffle_functions:   # current function is a mapper
                    reducer_foreach_num = net.m // net.foreach_size[-1]
                    for i in range(net.shuffle_n):
                        for j in range(net.m // net.foreach_size[i]):
                            next_function = []
                            next = []
                            nextDis = []
                            if i == 0:
                                name = function['name'] + '-' + str(j)
                            else:
                                name = function['name'] + '-' + \
                                    function['next']['nodes'][0] + \
                                    '-' + str(i) + '-' + str(j)
        
                            if i % 2 == 0:
                                if i == net.shuffle_n - 1:
                                    next_function.append(function['next']['nodes'][0] + '-' + str(j))
                                else: 
                                    next_function.append(function['name'] + '-' +
                                                     function['next']['nodes'][0] +
                                                     '-' + str(i+1) + '-' + str(j))
                            else:
                                if i == net.shuffle_n - 1:
                                    for k in range(net.m // net.foreach_size[i+1]):
                                        next_function.append(function['next']['nodes'][0] + '-' + str(k))
                                else:
                                    for k in range(net.m // net.foreach_size[i+1]):
                                        next_function.append(function['name'] + '-' +
                                                             function['next']['nodes'][0] +
                                                             '-' + str(i+1) + '-' + str(k))
                            for n in next_function:
                                if name in foreach_functions:
                                    merge_funtions.add(n)
                                if foreach_flag:
                                    foreach_functions.add(n)
                                next.append(n)
                                nextDis.append(send_time)
                                if n not in parent_cnt:
                                    parent_cnt[n] = 1
                                else:
                                    parent_cnt[n] = parent_cnt[n] + 1

                            current_function = component.function(name, [], next, nextDis, source, runtime,
                                                                  input_files, output_files, conditions)
                            if 'scale' in function:
                                current_function.set_scale(function['scale'])
                            if 'mem_usage' in function:
                                current_function.set_mem_usage(
                                    function['mem_usage'])
                            if 'split_ratio' in function:
                                current_function.set_split_ratio(
                                    function['split_ratio'])  # ???
                            total = total + 1
                            nodes[name] = current_function
                    continue
                else:                           # next function is a mapper
                    foreach_num = net.m // net.foreach_size[0]
                    next_function = function['next']['nodes'][0]
                    function['next']['nodes'].clear()
                    for idx in range(foreach_num):
                        function['next']['nodes'].append(
                            next_function + '-' + str(idx))
            # current function is a reducer and next function is not null
            if name in shuffle_functions and function['next']['type'] != 'shuffle':
                for i in range(reducer_foreach_num):
                    name = function['name'] + '-' + str(i)
                    if 'next' in function:
                        for n in function['next']['nodes']:
                            if name in foreach_functions:
                                merge_funtions.add(n)
                            if foreach_flag:
                                foreach_functions.add(n)
                            next.append(n)
                            nextDis.append(send_time)
                            if n not in parent_cnt:
                                parent_cnt[n] = 1
                            else:
                                parent_cnt[n] = parent_cnt[n] + 1
                    current_function = component.function(name, [], next, nextDis, source, runtime,
                                                          input_files, output_files, conditions)
                    if 'scale' in function:
                        current_function.set_scale(function['scale'])
                    if 'mem_usage' in function:
                        current_function.set_mem_usage(function['mem_usage'])
                    if 'split_ratio' in function:
                        current_function.set_split_ratio(
                            function['split_ratio'])  # ???
                    total = total + 1
                    nodes[name] = current_function
   
            for n in function['next']['nodes']:
                if name in foreach_functions:
                    merge_funtions.add(n)
                if foreach_flag:
                    foreach_functions.add(n)
                next.append(n)
                nextDis.append(send_time)
                if n not in parent_cnt:
                    parent_cnt[n] = 1
                else:
                    parent_cnt[n] = parent_cnt[n] + 1
        
        elif name in shuffle_functions: # current function is a reducer and next function is null
            for i in range(reducer_foreach_num):
                    name = function['name'] + '-' + str(i)
                    current_function = component.function(name, [], next, nextDis, source, runtime,
                                                          input_files, output_files, conditions)
                    if 'scale' in function:
                        current_function.set_scale(function['scale'])
                    if 'mem_usage' in function:
                        current_function.set_mem_usage(function['mem_usage'])
                    if 'split_ratio' in function:
                        current_function.set_split_ratio(
                            function['split_ratio'])  # ???
                    total = total + 1
                    nodes[name] = current_function
            continue      
        current_function = component.function(name, [], next, nextDis, source, runtime,
                                              input_files, output_files, conditions)
        if 'scale' in function:
            current_function.set_scale(function['scale'])
        if 'mem_usage' in function:
            current_function.set_mem_usage(function['mem_usage'])
        if 'split_ratio' in function:
            current_function.set_split_ratio(function['split_ratio'])  # ???
        total = total + 1
        nodes[name] = current_function
    for name in nodes:
        if name not in parent_cnt or parent_cnt[name] == 0:
            parent_cnt[name] = 0
            start_functions.append(name)
        for next_node in nodes[name].next:
            nodes[next_node].prev.append(name)
    print_workflow(nodes)
    return component.workflow(workflow_name, start_functions, nodes, global_input, total, parent_cnt, foreach_functions, merge_funtions)


if __name__ == "__main__":
    parse('wordcount-shuffle')
