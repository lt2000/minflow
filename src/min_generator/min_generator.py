import matplotlib.pyplot as plt
import networkx as nx
import random


group_ratio = []
group_num = []


def is_prime(n):  # Determine if n is prime
    if n == 1 or n == 2:
        return True
    for i in range(2, n//2 + 1):
        if n % i == 0:
            return False
    return True


def min_sum_factorization(m):
    if m == 1 or m == 2:
        return m
    for i in range(2, m//2 + 1):  # If m(>2) has a factor, it must be less than or equal to m over 2
        if m % i == 0:  # i is a factor of m
            # If the minimum sum is equal to itself then i is prime
            if min_sum_factorization(i) == i:
                group_ratio.append(i)
                factor = m//i
                if is_prime(factor):
                    group_ratio.append(factor)
                return i + min_sum_factorization(factor)  # Dynamic programming
    return m  # m is prime and returns itself


def min_optimized(m):  # m: the number of mappers; r: the number of reducers, Suppose m = r
    # Using dynamic programming to find factors and minimal factorization
    min_sum_factorization(m)

    # Combine two 2s into a 4 o reduce the number of stages, because 2x2=2+2
    local_group_ratio = []
    for i in range(len(group_ratio)-1, -1, -1):
        if group_ratio[i] != 2:
            local_group_ratio.append(group_ratio[i])
        else:
            for j in range((i+1)//2):
                local_group_ratio.append(4)
            if (i+1) % 2 != 0:
                local_group_ratio.append(2)
            break

    group_num.append(m)
    for idx, ratio in enumerate(sorted(local_group_ratio)):
        last = group_num[idx]
        group_num.append(last//ratio)
    return group_num

# def min_idx(idx, m, group_num):
#     group_size = m // group_num
#     group_idx = idx // group_size
#     group_inner_idx = idx % group_size

#     return group_idx, group_inner_idx


def schedule_hash(layer, idx, down):
    m = group_num[0]
    if layer == 0:
        return idx
    if not ((layer % 2) ^ down):
        return idx // (m // group_num[layer+down]) + (idx % (m // group_num[layer+down])) * (group_num[layer+down])
    return idx


def min_plot(group_num):  # plot multistage interconnection network
    left, right, bottom, top = .1, .9, .1, .9
    group_size = [group_num[0]//i for i in group_num]
    layer_num = len(group_num)
    layer_sizes = [group_num[0] for i in range(layer_num)]

    G = nx.Graph()
    v_spacing = (top - bottom)/float(max(layer_sizes))
    h_spacing = (right - left)/float(len(layer_sizes) - 1)
    node_count = 0
    for i, v in enumerate(layer_sizes):
        layer_top = v_spacing*(v-1)/2. + (top + bottom)/2.
        for j in range(v):
            G.add_node(node_count, pos=(
                left + i*h_spacing, layer_top - j*v_spacing))
            node_count += 1

    for x, (left_nodes, right_nodes) in enumerate(zip(layer_sizes[:-1], layer_sizes[1:])):
        for i in range(left_nodes):
            for j in range(right_nodes):
                # baseline network
                if (i // group_size[x+1]) == (j // group_size[x+1]) and i % group_size[x] == (j // (group_size[x+1] // group_size[x])) % group_size[x]:
                    G.add_edge(schedule_hash(x, i, 0)+sum(layer_sizes[:x]), schedule_hash(x, j, 1)+sum(layer_sizes[:x+1]))

    pos = nx.get_node_attributes(G, 'pos')
    # 把每个节点中的位置pos信息导出来
    nx.draw(G, pos,
            node_color='b',
            with_labels=False,
            node_size=50,
            # edge_color=[random.random() for i in range(len(G.edges))],
            width=1,
            # cmap=plt.cm.Dark2,  # matplotlib的调色板，可以搜搜，很多颜色呢
            edge_cmap=plt.cm.Blues
            )
    # plt.show()
    plt.savefig('./test.jpg')


def min_map():

    pass


if __name__ == "__main__":
    m = 30
    min_optimized(m)
    shuffle_num = len(group_num) - 1
    min_plot(group_num)
    print(group_num)
    print(shuffle_num)
