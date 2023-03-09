import matplotlib.pyplot as plt
import networkx as nx
import random


class min_generator:

    def __init__(self, m):
        self.m = m
        self.group_ratio = []
        self.group_num = []
        self.foreach_size = []
        self.min_optimized()
        self.shuffle_n = len(self.group_ratio)

    def is_prime(self, n):  # Determine if n is prime
        if n == 1 or n == 2:
            return True
        for i in range(2, n//2 + 1):
            if n % i == 0:
                return False
        return True

    def min_sum_factorization(self, m):
        if m == 1 or m == 2:
            return m
        # If m(>2) has a factor, it must be less than or equal to m over 2
        for i in range(2, m//2 + 1):
            if m % i == 0:  # i is a factor of m
                # If the minimum sum is equal to itself then i is prime
                if self.min_sum_factorization(i) == i:
                    self.group_ratio.append(i)
                    factor = m//i
                    if self.is_prime(factor):
                        self.group_ratio.append(factor)
                    # Dynamic programming
                    return i + self.min_sum_factorization(factor)
        return m  # m is prime and returns itself

    # m: the number of mappers; r: the number of reducers, Suppose m = r
    def min_optimized(self):
        # Using dynamic programming to find factors and minimal factorization
        self.min_sum_factorization(self.m)

        # Combine two 2s into a 4 o reduce the number of stages, because 2x2=2+2
        local_group_ratio = []
        for i in range(len(self.group_ratio)-1, -1, -1):
            if self.group_ratio[i] != 2:
                local_group_ratio.append(self.group_ratio[i])
            else:
                for j in range((i+1)//2):
                    local_group_ratio.append(4)
                if (i+1) % 2 != 0:
                    local_group_ratio.append(2)
                break
        self.group_ratio = sorted(local_group_ratio)[:]

        # calculate group_num list
        self.group_num.append(self.m)
        for idx, ratio in enumerate(self.group_ratio):
            last = self.group_num[idx]
            self.group_num.append(last//ratio)

        # calculate foreach_size list
        shuffle_n = len(self.group_ratio)
        for k,v in enumerate(self.group_ratio):
            if k % 2 == 0:
                self.foreach_size.extend([v, v])
            elif k + 1 == shuffle_n:
                self.foreach_size.append(self.m)
        return 0


    def schedule_hash(self, layer, idx, down):
        m = self.m
        if layer == 0:
            return idx
        if not ((layer % 2) ^ down):
            return idx // (m // self.group_num[layer+down]) + (idx % (m // self.group_num[layer+down])) * (self.group_num[layer+down])
        return idx


    def min_plot(self):  # plot multistage interconnection network
        left, right, bottom, top = .1, .9, .1, .9
        group_size = [self.group_num[0]//i for i in self.group_num]
        layer_num = len(self.group_num)
        layer_sizes = [self.group_num[0] for i in range(layer_num)]

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
                        G.add_edge(self.schedule_hash(
                            x, i, 0)+sum(layer_sizes[:x]), self.schedule_hash(x, j, 1)+sum(layer_sizes[:x+1]))

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


if __name__ == "__main__":
    net = min_generator(8)
    # net.min_optimized(net.m)
    print(net.group_num)
    print(net.group_ratio)
    print(net.foreach_size)
