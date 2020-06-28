import os
import sys
import boto3
import yaml
import click

from phlmd.model import *


class PhDeploy(object):
    """
    自动化部署一系列 Lambda 技术栈
    """
    _DEFAULT_BUCKET = "ph-api-lambda"
    _DEFAULT_OBJECT = "template/deploy/ph-lambda-deploy-template.yaml"
    _DEFAULT_CONF_FILE = "../.ph-lambda-deploy2.yaml"
    _DEFAULT_MAX_INST = 100

    def init(self, data):
        """
        初始化环境，关联本地项目和 lambda function
        :param data:
            :arg name: 项目名称，同时也是 layer，function，api gateway URL 一级路径名称
            :arg runtime: 项目使用的运行时
            :arg desc: 项目描述
            :arg lib_path: layer 依赖目录
            :arg code_path: function 代码目录
            :arg handler: lambda function 入口
        """
        buf = boto3.client('s3').get_object(
            Bucket=self._DEFAULT_BUCKET,
            Key=self._DEFAULT_OBJECT)["Body"].read().decode('utf-8') \
            .replace("#name#", data["name"]) \
            .replace("#runtime#", data["runtime"]) \
            .replace("#desc#", data["desc"]) \
            .replace("#lib_path#", data["lib_path"]) \
            .replace("#code_path#", data["code_path"]) \
            .replace("#handler#", data["handler"])

        if os.path.exists(self._DEFAULT_CONF_FILE):
            with open(self._DEFAULT_CONF_FILE) as f:
                deploy_conf = yaml.safe_load(f)
            if data["name"] in deploy_conf.keys():
                return f"Init Error，name {data['name']} is exists"

            with open(self._DEFAULT_CONF_FILE, "at") as at:
                at.write(buf)
            return "Append Init Success"
        else:
            with open(self._DEFAULT_CONF_FILE, "wt") as wt:
                wt.write(buf)
            return "Download Init Success"

    def __check_version(self, deploy_conf) -> dict:
        function_info = ph_lambda.PhLambda().get(deploy_conf["metadata"])
        if function_info == {}:
            last_version = "v0"
        else:
            last_version = function_info["Aliases"][0]["Name"]
        new_version = f"v{int(last_version[1:])+1}"
        deploy_conf["metadata"]["version"] = new_version
        return deploy_conf

    def __write_conf(self, all_conf):
        with open(self._DEFAULT_CONF_FILE, 'w') as w:
            yaml.dump(all_conf, w, default_flow_style=False, encoding='utf-8', allow_unicode=True)

    def apply(self, deploy_conf):

        if "role" in deploy_conf.keys():
            role = ph_role.Ph_Role()
            try:
                role.apply(dict(**{"name": deploy_conf["metadata"]["name"] + "-lambda-role"}, **deploy_conf["role"]))
                print("Role 更新完成")
            except:
                if role.get(deploy_conf["metadata"]) == {}:
                    print("Role 不存在，请联系管理员创建")
                    sys.exit(2)
            print()

        if "layer" in deploy_conf.keys():
            layer = ph_layer.PhLayer()

            if "lib_path" in deploy_conf["layer"]:
                print("开始打包本地依赖: " + deploy_conf["layer"]["lib_path"] + "\t->\t" + deploy_conf["layer"]["package_name"])
                layer.package(dict(**deploy_conf["metadata"], **deploy_conf["layer"]))
                print("本地依赖打包完成")

            response = layer.apply(dict(**deploy_conf["metadata"], **deploy_conf["layer"]))
            print("layer 更新完成: " + response["LayerVersionArn"])
            print()

        if "lambda" in deploy_conf.keys():
            lambda_function = ph_lambda.PhLambda()

            if "code_path" in deploy_conf["lambda"]:
                print("开始打包本地代码: " + deploy_conf["lambda"]["code_path"] + "\t->\t" + deploy_conf["lambda"]["package_name"])
                lambda_function.package(dict(**deploy_conf["metadata"], **deploy_conf["lambda"]))
                print("本地代码打包完成")

            response = lambda_function.apply(dict(**deploy_conf["metadata"], **deploy_conf["lambda"]))
            print("lambda 更新完成: " + response["AliasArn"])
            print()

        if "gateway" in deploy_conf.keys():
            gateway = ph_gateway.PhGateway()
            response = gateway.apply(dict(**deploy_conf["metadata"], **deploy_conf["gateway"]))
            print("gateway 更新完成: " + response)
            print()

    def __clean_cache(self, deploy_conf):
        print("开始清理执行缓存")

        if "layer" in deploy_conf.keys():
            if os.path.exists(deploy_conf["layer"]["package_name"]):
                os.remove(deploy_conf["layer"]["package_name"])

            layer = ph_layer.PhLayer()
            layer_versions = layer.get({"name": deploy_conf["metadata"]["name"]})["LayerVersions"]
            if len(layer_versions) > self._DEFAULT_MAX_INST:
                for lv in layer_versions[self._DEFAULT_MAX_INST:]:
                    layer.delete({
                        "name": deploy_conf["metadata"]["name"],
                        "version": lv["Version"],
                    })

        if "lambda" in deploy_conf.keys():
            if os.path.exists(deploy_conf["lambda"]["package_name"]):
                os.remove(deploy_conf["lambda"]["package_name"])

            lambda_function = ph_lambda.PhLambda()
            lambda_aliases = lambda_function.get({"name": deploy_conf["metadata"]["name"]})["Aliases"]
            if len(lambda_aliases) > self._DEFAULT_MAX_INST:
                for la in lambda_aliases[self._DEFAULT_MAX_INST:]:
                    lambda_function.delete({
                        "name": deploy_conf["metadata"]["name"],
                        "version": la["Name"],
                    })

        print("执行缓存清理完成")
        print()

    def push(self, data):
        """
        发布依赖到 lambda layer, 项目代码到 lambda function, 并在 API Gateway 中关联到当前 lambda function 别名
        请在项目的根目录执行
        :param data:
            :arg n | name : 指定提交的项目，如果只代理一个项目则无需传入
            :arg    : 默认不传参数，按照预计使用频率，所以只发布 function + gateway
            :arg all: 发布全部资源 （role、layer、function、gateway）
            :arg role: 只发布 role
            :arg lib: 只发布 layer
            :arg code: 只发布 function
            :arg api: 只发布 gateway
        """
        # get project name from args
        if "n" in data.keys():
            project_name = data.pop("n")
        elif "name" in data.keys():
            project_name = data.pop("name")
        else:
            project_name = ""

        # ensure project name
        with open(self._DEFAULT_CONF_FILE) as f:
            all_conf = yaml.safe_load(f)
        if project_name == "":
            project_name = list(all_conf.keys())[0]
        print(f"开始部署 {project_name}")

        # check version and write back
        deploy_conf = self.__check_version(all_conf[project_name])
        all_conf[project_name] = deploy_conf
        self.__write_conf(all_conf)

        # filter operator
        if not len(data):
            data = {"code": "", "api": ""}
        if "all" not in data.keys():
            all_operator = {"role": "role", "lib": "layer", "code": "lambda", "api": "gateway"}
            for not_oper in all_operator.keys() - set(data.keys()):
                del deploy_conf[all_operator[not_oper]]

        self.apply(deploy_conf)
        self.__clean_cache(deploy_conf)

        return "Deploy Success"
