import yaml
from ph_errs.ph_err import exception_linkage_parse_fail


class PhDagRes(object):
    def __init__(self, type, name, value, state='ready'):
        self.type = type
        self.name = name
        self.value = value
        self.state = state  # ready, run, success, failed

    def __str__(self):
        return str(self.__dict__)


class PhDagStrRes(PhDagRes):
    def __init__(self, name, value):
        super().__init__('str', name, value)


class PhDagDataRes(PhDagRes):
    def __init__(self, name, value):
        super().__init__('data', name, value)


class PhDagAssetRes(PhDagRes):
    def __init__(self, name, value):
        super().__init__('asset', name, value)


class PhDagFuncRes(PhDagRes):
    def __init__(self, name, value):
        super().__init__('func', name, value)


def gen_res(name, value):
    if type(value) == str and value.startswith('s3a://'):
        if 'asset' in value.split('/')[-1]:
            return PhDagAssetRes(name, value)
        else:
            return PhDagDataRes(name, value)
    else:
        return PhDagStrRes(name, value)


class PhDagNode(object):
    def __init__(self, func, inputs, outputs):
        self.func = func
        self.inputs = inputs
        self.outputs = outputs

    def __str__(self):
        return str(self.__dict__)


class PhDagLinkage(object):
    def __init__(self, linkage):
        self.linkage = linkage
        self.linkage = self.get_linkages()

    def get_parallel_pair(self, linkage):
        def count_index(linkage):
            left = []
            left_count = 0
            right = []
            right_count = 0
            for index, char in enumerate(linkage):
                if char == '[':
                    left_count += 1
                    left.append(index)
                elif char == ']':
                    right_count += 1
                    right.append(index)

            return (left_count, left), (right_count, right)

        def first(lst, cond = lambda x: True):
            for i in lst:
                if cond(i):
                    return i

        lefts, rights = count_index(linkage)
        if lefts[0] != rights[0]:
            raise exception_linkage_parse_fail
        lefts, rights = lefts[1], rights[1]

        parallel_pair = []
        for left in lefts[::-1]:
            right = first(rights, lambda x: left < x)
            lefts.remove(left)
            rights.remove(right)
            parallel_pair.append((left, right))

        return parallel_pair

    def parse_serial(self, linkage):
        return [linkage.strip() for linkage in linkage.split('>>')]

    def parse_parallel(self, linkage):
        return [linkage.strip() for linkage in linkage.split(',')]

    def get_linkages(self):
        linkage = self.linkage
        parallel_pair = self.get_parallel_pair(linkage)
        parallel_map = {}
        for pair in parallel_pair:
            parallel = self.parse_parallel(linkage[pair[0]+1: pair[1]])
            key = str(pair[0])+'_'+str(pair[1])
            parallel_map[key] = parallel
            linkage = linkage[0:pair[0]] + key + linkage[pair[1]+1:]

        linkages = self.parse_serial(linkage)
        for index, linkage in enumerate(linkages):
            if linkage in parallel_map.keys():
                linkages[index] = parallel_map[linkage]

        return linkages


class PhDagGraph(object):
    def __init__(self, linkages, nodes):
        self.linkage = linkages
        self.nodes = nodes
        self._v = {}
        self._e = set()

    def __str__(self):
        return str(self.__dict__)

    def to_yaml(self, path):
        with open(path, 'a') as file:
            yaml.dump(self, file)
        print(self.__dict__)
    def convert_ve(self):
        for node in self.nodes:
            self._v[node.func.value] = node.func
            for i in node.inputs:
                if i.value not in self._v.keys():
                    self._v[i.value] = i
                self._e.add((i.value, node.func.value))
            for o in node.outputs:
                if o.value not in self._v.keys():
                    self._v[o.value] = o
                self._e.add((node.func.value, o.value))
        return self

    def to_graph(self):
        self.convert_ve()

        return self

    def print_graph(self):
        def bule_print(str):
            print('\033[1;33;44m{}\033[0m'.format(str))

        for i in self._e:
            print(i)
