# -*- coding: utf-8 -*-

import sys
import getopt
import click
from phlmd.model import ph_role, ph_layer, ph_lambda, ph_gateway
from pherrs.ph_err import PhError


@click.group('model', short_help='专项部署特定资源[功能关闭]')
def model():
    """
    用于快速打包和部署 AWS Lambda 和 API Gateway

\b
操作名有如下：
    package : 对 lambda 的 layer 或者 code 打成 zip 包
    create  : 创建 lambda 的 role, layer 或者 code 或者 API Gateway 的一级资源
    lists   : 获取所有资源实例
    get     : 获取指定资源实例
    update  : 更新 lambda 的 layer 或者 code 或者 API Gateway 的一级资源
    apply   : 发布或更新 lambda 的 layer 或者 code 或者 API Gateway 的一级资源
    stop    : 使 lambda 或者 API Gateway 停止接受请求
    start   : 重新使 lambda 或者 API Gateway 接受请求
    delete  : 删除 lambda 的 layer 或者 code, 或者 API Gateway 的一级资源

\b
资源名有如下：
    role    : lambda 代理角色
    layer   : lambda 的依赖层
    lambda  : lambda 的源代码
    gateway : lambda 的触发器 API Gateway
    """
    pass


def fineness_func(operator, model, argv):
    """
    粒度功能使用
    :return:
    """

    def get_model_inst(model):
        model_switcher = {
            "role": ph_role.Ph_Role(),
            "layer": ph_layer.Ph_Layer(),
            "lambda": ph_lambda.Ph_Lambda(),
            "gateway": ph_gateway.Ph_Gateway(),
        }
        return model_switcher.get(model, "Invalid model")

    def get_oper_inst(model_inst, oper):
        if oper == "package":
            return model_inst.package
        elif oper == "create":
            return model_inst.create
        elif oper == "lists":
            return model_inst.lists
        elif oper == "get":
            return model_inst.get
        elif oper == "update":
            return model_inst.update
        elif oper == "apply":
            return model_inst.apply
        elif oper == "stop":
            return model_inst.stop
        elif oper == "start":
            return model_inst.start
        elif oper == "delete":
            return model_inst.delete
        else:
            raise PhError("Invalid operator")

    try:
        opts, args = getopt.getopt(argv, "h",
                                   ["help", "name=", "version=",
                                    "runtime=", "package_name=", "lib_path=", "is_pipenv=", "code_path=", # runtime package args
                                    "arpd_path=", "policys_arn="# role oper args
                                                  "layer_path=",  # layer oper args
                                    "lambda_path=", "lambda_handler=", "lambda_layers=", # lambda oper args
                                    "lambda_timeout=", "lambda_memory_size=", "lambda_concurrent=", # lambda oper args
                                    "lambda_desc=", "lambda_env=", "lambda_tag=", "vpc_config=", # lambda oper args
                                    "rest_api_id=", "api_template=", "lambda_name=", "role_name=", # apigateway oper args
                                    ])
    except getopt.GetoptError:
        print('请注意调用格式： aws_lambda_deploy operator model [opt arg]')
        sys.exit(2)
    except:
        sys.exit(2)

    inst = get_oper_inst(get_model_inst(model), operator)

    inst_args = {}
    for opt, arg in opts:
        if opt == '-h' or opt == "--help":
            print(inst.__doc__)
            sys.exit(2)
        else:
            inst_args[opt.split("-")[-1]] = arg

    print(inst(inst_args))
