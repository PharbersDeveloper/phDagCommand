import os
import re
import subprocess

from phcli.ph_errs.ph_err import *
from .ph_rt_base import PhRTBase
from phcli.define_value import CLI_CLIENT_VERSION
from phcli.ph_max_auto import define_value as dv
from phcli.ph_max_auto.ph_config.phconfig.phconfig import PhYAMLConfig


class PhRTPython3(PhRTBase):
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def c9_create_init(self, path=None):
        if not path:
            path = self.job_path + "/__init__.py"
        subprocess.call(["touch", path])

    def c9_create_phmain(self, path=None):
        if not path:
            path = self.job_path

        config = PhYAMLConfig(path)
        config.load_yaml()
        f_lines = self.phs3.open_object_by_lines(dv.TEMPLATE_BUCKET, dv.CLI_VERSION + dv.TEMPLATE_PHMAIN_FILE_PY)
        with open(path + "/phmain.py", "w") as file:
            s = []
            for arg in config.spec.containers.args:
                s.append(arg.key)

            for line in f_lines:
                line = line + "\n"
                if line == "$alfred_debug_execute\n":
                    file.write("@click.command()\n")
                    for must in dv.PRESET_MUST_ARGS.split(","):
                        file.write("@click.option('--{}')\n".format(must.strip()))
                    for arg in config.spec.containers.args:
                        file.write("@click.option('--" + arg.key + "')\n")
                    for output in config.spec.containers.outputs:
                        file.write("@click.option('--" + output.key + "')\n")
                    file.write("""def debug_execute(**kwargs):
    try:
        args = {"name": "$alfred_name"}
        outputs = [$alfred_outputs]

        args.update(kwargs)
        result = exec_before(**args)

        args.update(result if isinstance(result, dict) else {})
        result = execute(**args)

        args.update(result if isinstance(result, dict) else {})
        result = exec_after(outputs=outputs, **args)

        return result
    except Exception as e:
        logger = phs3logger(kwargs["job_id"])
        logger.error(traceback.format_exc())
        print(traceback.format_exc())
        raise e
"""
                               .replace('$alfred_outputs', ', '.join(['"'+output.key+'"' for output in config.spec.containers.outputs])) \
                               .replace('$alfred_name', config.metadata.name)
                               )
                else:
                    file.write(line)

    def c9_create(self, **kwargs):
        # 1. /__init__.py file
        self.c9_create_init()

        # 2. /phjob.py file
        self.phs3.download(dv.TEMPLATE_BUCKET, dv.CLI_VERSION + dv.TEMPLATE_PHJOB_FILE_PY, self.job_path + "/phjob.py")
        with open(self.job_path + "/phjob.py", "a") as file:
            file.write("""def execute(**kwargs):
    \"\"\"
        please input your code below\n""")

            if self.command == 'submit':
                file.write('        get spark session: spark = kwargs["spark"]()\n')

            file.write("""    \"\"\"
    logger = phs3logger(kwargs["job_id"], LOG_DEBUG_LEVEL)
    logger.info("当前 owner 为 " + str(kwargs["owner"]))
    logger.info("当前 run_id 为 " + str(kwargs["run_id"]))
    logger.info("当前 job_id 为 " + str(kwargs["job_id"]))
""")

            if self.command == 'submit':
                file.write('    spark = kwargs["spark"]()')

            file.write("""
    logger.info(kwargs["a"])
    logger.info(kwargs["b"])
    logger.info(kwargs["c"])
    logger.info(kwargs["d"])
    return {}
""")

        # 3. /phmain.py file
        self.c9_create_phmain()

    def jupyter_create(self, **kwargs):
        path = self.job_path + ".ipynb"
        dir_path = "/".join(path.split('/')[:-1])
        subprocess.call(['mkdir', '-p', dir_path])

        f_lines = self.phs3.open_object_by_lines(dv.TEMPLATE_BUCKET, dv.CLI_VERSION + dv.TEMPLATE_JUPYTER_PYTHON_FILE)
        with open(path, "w") as file:
            for line in f_lines:
                line = line.replace('$name', self.name) \
                            .replace('$runtime', self.runtime) \
                            .replace('$command', self.command) \
                            .replace('$timeout', str(self.timeout)) \
                            .replace('$user', os.getenv('USER', 'unknown')) \
                            .replace('$group', self.group) \
                            .replace('$ide', self.ide) \
                            .replace('$access_key', os.getenv('AWS_ACCESS_KEY_ID', "NULL")) \
                            .replace('$secret_key', os.getenv('AWS_SECRET_ACCESS_KEY', "NULL"))
                file.write(line)

    def create(self, **kwargs):
        if self.ide == 'c9':
            self.c9_create(**kwargs)
        elif self.ide == 'jupyter':
            self.jupyter_create(**kwargs)
        else:
            raise exception_function_not_implement

    def c9_to_jupyter(self, source_path, target_path):
        # 创建json文件
        path = target_path + "/phJupyterPython.ipynb"
        subprocess.call(["touch", path])

        with open(target_path + "/phJupyterPython.ipynb", "w") as file:
            indentation = 4
            frist_cell_index = 0
            second_cell_index = 1
            redundantCell_begin_index = 2
            redundantCell_end_index = 5

            # 从S3中获取jupyter模板
            template_str = self.phs3.open_object(dv.TEMPLATE_BUCKET, dv.CLI_VERSION + dv.TEMPLATE_JUPYTER_PYTHON_FILE)
            # 字符串转换成字典
            data = json.loads(template_str)
            # phconf.yaml中获取数据
            config = PhYAMLConfig(source_path)
            config.load_yaml()
            job_name = config.metadata.name
            job_runtime = config.spec.containers.runtime
            job_command = config.spec.containers.command
            job_timeout = str(config.spec.containers.timeout)

            # 写入input_args数据
            input_index = data['cells'][frist_cell_index]['source'].index("a = 123\n")
            del data['cells'][frist_cell_index]['source'][input_index:input_index + 2]
            for arg in config.spec.containers.args:
                arg_key = arg.key
                arg_value = arg.value
                if str.isdigit(arg_value):
                    data['cells'][frist_cell_index]['source'].insert(input_index, arg_key + " = " + arg_value + "\n")
                else:
                    data['cells'][frist_cell_index]['source'].insert(input_index, arg_key + " = '" + arg_value + "'\n")

            # 写入output_args数据
            output_index = data['cells'][frist_cell_index]['source'].index("c = 'abc'\n")
            del data['cells'][frist_cell_index]['source'][output_index:output_index + 2]
            for output in config.spec.containers.outputs:
                output_key = output.key
                output_value = output.value
                if str.isdigit(output_value):
                    data['cells'][frist_cell_index]['source'].insert(output_index,
                                                                     output_key + " = " + output_value + "\n")
                else:
                    data['cells'][frist_cell_index]['source'].insert(output_index,
                                                                     output_key + " = '" + output_value + "'\n")

            empty_source1 = []
            # 把source中内容进行遍历，替换指定内容
            for str1 in data['cells'][frist_cell_index]['source']:
                str2 = str1.replace('$name', job_name) \
                    .replace('$runtime', job_runtime) \
                    .replace('$command', job_command) \
                    .replace('$timeout', job_timeout)
                empty_source1.append(str2)
            # 把修改后的数据写进一个空list，再把空list 写入到source中
            data['cells'][frist_cell_index]['source'] = empty_source1

            empty_source2 = []
            for str1 in data['cells'][second_cell_index]['source']:
                str2 = str1.replace('$name', job_name) \
                    .replace('$runtime', job_runtime) \
                    .replace('$command', job_command) \
                    .replace('$timeout', job_timeout) \
                    .replace('$user', "user") \
                    .replace('$group', "group") \
                    .replace('$ide', "idee") \
                    .replace('$access_key', "AWS_ACCESS_KEY_ID") \
                    .replace('$secret_key', "AWS_SECRET_ACCESS_KEY")
                empty_source2.append(str2)
            data['cells'][second_cell_index]['source'] = empty_source2

            # 删除data中多余的cell
            del data['cells'][redundantCell_begin_index:redundantCell_end_index]

            # phjob的内容copyt到.ipynb下的source中
            with open(source_path + "/phjob.py", "r") as phjob_flie:
                line = phjob_flie.readline()
                while line:
                    while line.startswith('def'):
                        demo = {
                            "cell_type": "code",
                            "execution_count": None,
                            "metadata": {},
                            "outputs": [],
                            "source": []
                        }
                        demo['source'].append(line)
                        line = phjob_flie.readline()
                        while not line.startswith('def') and not line == "":
                            demo['source'].append(line)
                            line = phjob_flie.readline()
                        data['cells'].append(demo)
                    line = phjob_flie.readline()

                for cell in data['cells'][:]:
                    for source in cell['source']:
                        if source.startswith('def execute'):
                            execute_index = data['cells'].index(cell)

                empty_source = []
                for execute_source_str in data['cells'][execute_index]['source']:
                    if len(execute_source_str) - len(execute_source_str.lstrip()) >= indentation:
                        if not execute_source_str.lstrip().startswith('spark = kwargs['):
                            empty_source.append(execute_source_str[indentation:])
                    elif execute_source_str.lstrip().startswith('#'):
                        empty_source.append(execute_source_str)
                data['cells'][execute_index]['source'] = empty_source

            # 把data从字典转换成json格式,indent=1进行换行，ensure_ascii防止汉字转成Unicode码
            json_str = json.dumps(data, indent=1, ensure_ascii=False)
            file.write(json_str)

    def jupyter_to_c9(self, dag_full_path, **kwargs):
        im = kwargs['im']
        om = kwargs['om']
        ipynb_dict = kwargs['ipynb_dict']

        self.phs3.download(dv.TEMPLATE_BUCKET, dv.CLI_VERSION + dv.TEMPLATE_PHJOB_FILE_PY, dag_full_path + "/phjob.py")
        with open(dag_full_path + "/phjob.py", "a") as file:
            file.write("""def execute(**kwargs):
    \"\"\"
        please input your code below
        get spark session: spark = kwargs["spark"]()
    \"\"\"
    spark = kwargs['spark']()
    logger = phs3logger(kwargs["job_id"], LOG_DEBUG_LEVEL)

""")
            # 取参数
            for input in im:
                file.write("    {key} = kwargs['{key}']\n".format(key=input))
            for output in om:
                file.write("    {key} = kwargs['{key}']\n".format(key=output))
            file.write("\n")

            # copy 逻辑代码
            for cell in ipynb_dict['cells'][2:]:
                for row in cell['source']:
                    row = re.sub(r'(^\s*)print(\(.*)', r"\1logger.debug\2", row)
                    file.write('    '+row)
                file.write('\r\n')
                file.write('\r\n')

    def submit_run(self, **kwargs):
        submit_conf = {
            "jars": "s3a://ph-platform/2020-11-11/jobs/python/phcli/common/aws-java-sdk-bundle-1.11.828.jar,"
                    "s3a://ph-platform/2020-11-11/jobs/python/phcli/common/hadoop-aws-3.2.1.jar",
        }
        submit_file = {
            "py-files": "s3a://" + dv.TEMPLATE_BUCKET + "/" + dv.CLI_VERSION + dv.DAGS_S3_PHJOBS_PATH + "common/phcli-{}-py3.8.egg,".format(CLI_CLIENT_VERSION) +
                        self.submit_prefix + "phjob.py",
        }
        submit_main = self.submit_prefix + "phmain.py"

        super().submit_run(submit_conf=submit_conf,
                           submit_file=submit_file,
                           submit_main=submit_main)

    def script_run(self, **kwargs):
        self.phs3.download(dv.TEMPLATE_BUCKET, dv.CLI_VERSION + self.s3_job_path + "/phmain.py", 'phmain.py')
        self.phs3.download(dv.TEMPLATE_BUCKET, dv.CLI_VERSION + self.s3_job_path + "/phjob.py", 'phjob.py')
        entrypoint = ['python3', './phmain.py']
        super().script_run(entrypoint=entrypoint)


