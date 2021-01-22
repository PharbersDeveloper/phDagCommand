import os
import re
import json
import subprocess

from .ph_ide_base import PhIDEBase, dv, exception_file_not_exist, exception_function_not_implement, PhYAMLConfig


class PhIDEJupyter(PhIDEBase):
    """
    针对 Jupyter 环境的执行策略
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger.debug('maxauto PhIDEJupyter init')
        self.logger.debug(self.__dict__)

    def create(self, **kwargs):
        """
        jupyter的创建过程
        """
        self.logger.info('maxauto ide=jupyter 的 create 实现')
        self.logger.debug(self.__dict__)

        self.check_path(self.job_path)

        super().create()

    def run(self, **kwargs):
        """
        jupyter的运行过程
        """
        self.logger.info('maxauto ide=jupyter 的 run 实现')
        self.logger.debug(self.__dict__)
        self.logger.error('maxauto --ide=jupyter 时，不支持 run 子命令')

    def dag_copy_job(self, **kwargs):
        """
        maxauto dag 时 copy jupyter 环境下生成的 job
        """
        self.logger.info('maxauto ide=jupyter 的 dag_copy_job 实现')
        self.logger.debug(self.__dict__)

        job_name = kwargs['job_name'].replace('.', '_')
        dag_full_path = self.dag_path + job_name
        job_full_path = self.project_path + self.job_prefix + kwargs['job_name'].replace('.', '/') + '.ipynb'

        # 1. 检查是否存在
        if not os.path.exists(job_full_path):
            raise exception_file_not_exist

        # 2. 创建目标文件夹
        subprocess.call(["mkdir", "-p", dag_full_path])

        # 3. 读取源文件的配置和输入输出参数
        ipynb_dict, cm, im, om = {}, {}, {}, {}
        with open(job_full_path, 'r') as rf:
            ipynb_dict = json.load(rf)
            source = ipynb_dict['cells'][0]['source']
            cm = self.get_ipynb_map_by_key(source, 'config')
            im = self.get_ipynb_map_by_key(source, 'input args')
            om = self.get_ipynb_map_by_key(source, 'output args')

        # 4. 将读取的配置和参数写到 phconf.yaml 中
        input_str = ["- key: {}\n        value: {}".format(k, v) for k, v in im.items()]
        input_str = '\n      '.join(input_str)
        output_str = ["- key: {}\n        value: {}".format(k, v) for k, v in om.items()]
        output_str = '\n      '.join(output_str)
        self.create_phconf_file(dag_full_path, input_str=input_str, output_str=output_str, **kwargs)

        runtime_inst = self.table_driver_runtime_inst(kwargs['runtime'])(**self.__dict__)

        # python 需要 __init__.py 文件
        if kwargs['runtime'] == 'python3':
            # 5. 创建 /__init__.py file
            runtime_inst.c9_create_init(dag_full_path + "/__init__.py")

        # 6. 生成 /phmain.* file
        runtime_inst.c9_create_phmain(dag_full_path)

        # 7. 根据 .ipynb 转换为 phjob.* 文件
        runtime_inst.jupyter_to_c9(dag_full_path, cm=cm, im=im, om=om, ipynb_dict=ipynb_dict)

        # 8. phconf 转为 args.properties
        self.yaml2args(dag_full_path)
