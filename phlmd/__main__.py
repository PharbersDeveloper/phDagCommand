import click


CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


@adf.group("lmd", context_settings=CONTEXT_SETTINGS)
def lmd():
    """
    自动化部署一系列 Lambda 技术栈
    """
    pass


def init():
    pass


def push():
    pass

# import os
# import sys
# import getopt
# from model import *
# from ph_deploy import Ph_Deploy
#
# # If we are running from a wheel, add the wheel to sys.path
# # This allows the usage python pip-*.whl/pip install pip-*.whl
# if __package__ == '':
#     # __file__ is pip-*.whl/pip/__main__.py
#     # first dirname call strips of '/__main__.py', second strips off '/pip'
#     # Resulting path is the name of the wheel itself
#     # Add that to sys.path so we can import pip
#     path = os.path.dirname(os.path.dirname(__file__))
#     sys.path.insert(0, path)
#
#
# def print_help():
#     help_msg = """
# 本脚本用于快速打包和部署 AWS Lambda 和 API Gateway
# 调用方式为 $ 脚本名 操作名 资源名 参数
#
# 操作名有如下：
#     package : 对 lambda 的 layer 或者 code 打成 zip 包
#     create  : 创建 lambda 的 role, layer 或者 code 或者 API Gateway 的一级资源
#     lists   : 获取所有资源实例
#     get     : 获取指定资源实例
#     update  : 更新 lambda 的 layer 或者 code 或者 API Gateway 的一级资源
#     apply   : 发布或更新 lambda 的 layer 或者 code 或者 API Gateway 的一级资源
#     stop    : 使 lambda 或者 API Gateway 停止接受请求
#     start   : 重新使 lambda 或者 API Gateway 接受请求
#     delete  : 删除 lambda 的 layer 或者 code, 或者 API Gateway 的一级资源
#
# 资源名有如下：
#     role    : lambda 代理角色
#     layer   : lambda 的依赖层
#     lambda  : lambda 的源代码
#     gateway : lambda 的触发器 API Gateway
#     deploy  : 自动化部署一系列 Lambda 技术栈
#
# 参数：
#     各种资源名的参数请使用 $ 脚本名 操作名 资源名 -h 或 --help 单独查看
#     """
#
#     print(help_msg)
#
#
# def fineness_func(operator, model, argv):
#     """
#     粒度功能使用
#     :return:
#     """
#
#     def get_model_inst(model):
#         model_switcher = {
#             "role": ph_role.Ph_Role(),
#             "layer": ph_layer.Ph_Layer(),
#             "lambda": ph_lambda.Ph_Lambda(),
#             "gateway": ph_gateway.Ph_Gateway(),
#         }
#         return model_switcher.get(model, "Invalid model")
#
#     def get_oper_inst(model_inst, oper):
#         if oper == "package":
#             return model_inst.package
#         elif oper == "create":
#             return model_inst.create
#         elif oper == "lists":
#             return model_inst.lists
#         elif oper == "get":
#             return model_inst.get
#         elif oper == "update":
#             return model_inst.update
#         elif oper == "apply":
#             return model_inst.apply
#         elif oper == "stop":
#             return model_inst.stop
#         elif oper == "start":
#             return model_inst.start
#         elif oper == "delete":
#             return model_inst.delete
#         else:
#             raise Exception("Invalid operator")
#
#     try:
#         opts, args = getopt.getopt(argv, "h",
#                                    ["help", "name=", "version=",
#                                     "runtime=", "package_name=", "lib_path=", "is_pipenv=", "code_path=", # runtime package args
#                                     "arpd_path=", "policys_arn="# role oper args
#                                                   "layer_path=",  # layer oper args
#                                     "lambda_path=", "lambda_handler=", "lambda_layers=", # lambda oper args
#                                     "lambda_timeout=", "lambda_memory_size=", "lambda_concurrent=", # lambda oper args
#                                     "lambda_desc=", "lambda_env=", "lambda_tag=", # lambda oper args
#                                     "rest_api_id=", "api_template=", "lambda_name=", "role_name=", # apigateway oper args
#                                     ])
#     except getopt.GetoptError:
#         print('请注意调用格式： aws_lambda_deploy operator model [opt arg]')
#         sys.exit(2)
#     except:
#         print_help()
#         sys.exit(2)
#
#     inst = get_oper_inst(get_model_inst(model), operator)
#
#     inst_args = {}
#     for opt, arg in opts:
#         if opt == '-h' or opt == "--help":
#             print(inst.__doc__)
#             sys.exit(2)
#         else:
#             inst_args[opt.split("-")[-1]] = arg
#
#     print(inst(inst_args))
#
#
# def deploy_func(operator, argv):
#     inst = Ph_Deploy()
#     if "init" == operator:
#         inst = inst.init
#     elif "push" == operator:
#         inst = inst.push
#
#     try:
#         opts, args = getopt.getopt(argv, "hn:",
#                                    ["help", "name=", "runtime=", "desc=",
#                                     "lib_path=", "code_path=", "handler=",
#                                     "all", "role", "lib", "code", "api",
#                                     ])
#     except getopt.GetoptError:
#         print('请注意调用格式： aws_lambda_deploy [init|push [-n example]] --help')
#         sys.exit(2)
#
#     inst_args = {}
#     for opt, arg in opts:
#         if opt == '-h' or opt == "--help":
#             print(inst.__doc__)
#             sys.exit(2)
#         else:
#             inst_args[opt.split("-")[-1]] = arg
#
#     print(inst(inst_args))
#
#
# if __name__ == '__main__':
#     try:
#         operator = sys.argv[1]
#     except:
#         print('请注意调用格式： aws_lambda_deploy [init|push [-n example]] --help')
#         sys.exit(2)
#
#     # 快捷发布功能
#     if operator in ["init", "push"]:
#         deploy_func(operator, sys.argv[2:])
#
#     # 原子功能使用
#     else:
#         fineness_func(operator, sys.argv[2], sys.argv[3:])
