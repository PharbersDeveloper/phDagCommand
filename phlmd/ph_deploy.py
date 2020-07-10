import os
import sys
import boto3
import yaml
import click
from phlmd.model import ph_role
from phlmd.model import ph_layer
from phlmd.model import ph_lambda
from phlmd.model import ph_gateway


@click.group("deploy")
def deploy():
    """
    自动化部署一系列 Lambda 技术栈
    """
    pass


__DEFAULT_BUCKET = "ph-api-lambda"
__DEFAULT_OBJECT = "template/deploy/ph-lambda-deploy-template.yaml"
__DEFAULT_CONF_FILE = ".ph-lambda-deploy.yaml"
__DEFAULT_MAX_INST = 100


@deploy.command("init")
@click.option('-n', '--name', prompt='项目名称', help='项目名称')
@click.option('-R', '--runtime', prompt='项目使用的运行时', help='项目使用的运行时',
              type=click.Choice(['python3.6', 'python3.8', 'nodejs10.x', 'go1.x']))
@click.option('-D', '--desc', prompt='项目描述', help='项目描述')
@click.option('-L', '--lib_path', prompt='layer 依赖目录'
                                         '(Python 如".venv/lib/python3.8/site-packages", Nodejs 如"node_modules")',
              help='layer 依赖目录')
@click.option('-C', '--code_path', prompt='function 代码目录', help='function 代码目录')
@click.option('-H', '--handler', prompt='lambda function 入口', help='lambda function 入口')
def init(name, runtime, desc, lib_path, code_path, handler):
    """
    初始化环境，关联本地项目和 lambda function
    """
    buf = boto3.client('s3').get_object(
        Bucket=__DEFAULT_BUCKET,
        Key=__DEFAULT_OBJECT)["Body"].read().decode('utf-8') \
        .replace("#name#", name) \
        .replace("#runtime#", runtime) \
        .replace("#desc#", desc) \
        .replace("#lib_path#", lib_path) \
        .replace("#code_path#", code_path) \
        .replace("#handler#", handler)

    if os.path.exists(__DEFAULT_CONF_FILE):
        with open(__DEFAULT_CONF_FILE) as f:
            deploy_conf = yaml.safe_load(f)
        if name in deploy_conf.keys():
            click.secho(f"Init Error，name '{name}' is exists", fg='red', blink=True, bold=True)
            return

        with open(__DEFAULT_CONF_FILE, "at") as at:
            at.write(buf)

        click.secho(f"Append Init Success", fg='green', blink=True, bold=True)
        return
    else:
        with open(__DEFAULT_CONF_FILE, "wt") as wt:
            wt.write(buf)
        click.secho(f"Download Init Success", fg='green', blink=True, bold=True)
        return


def __check_max_version(deploy_conf) -> (dict, str):
    def max_version(cur, max) -> str:
        return cur if int(cur[1:]) > int(max[1:]) else max

    function_info = ph_lambda.PhLambda().get(deploy_conf["metadata"])
    last_version = "v0"
    if function_info != {}:
        for alias in function_info["Aliases"]:
            last_version = max_version(alias["Name"], last_version)

    return deploy_conf, last_version


def __write_conf(all_conf):
    with open(__DEFAULT_CONF_FILE, 'w') as w:
        yaml.dump(all_conf, w, default_flow_style=False, encoding='utf-8', allow_unicode=True)


def __apply(deploy_conf):
    if "role" in deploy_conf.keys():
        role = ph_role.PhRole()
        try:
            role.apply(dict(**{"name": deploy_conf["metadata"]["name"] + "-lambda-role"}, **deploy_conf["role"]))
            click.secho(f"Role 更新完成", fg='green', blink=True, bold=True)
        except:
            if role.get(deploy_conf["metadata"]) == {}:
                click.secho(f"Role 不存在，请联系管理员创建", fg='red', blink=True, bold=True)
                sys.exit(2)
        click.secho()

    if "layer" in deploy_conf.keys():
        layer = ph_layer.PhLayer()

        if "lib_path" in deploy_conf["layer"]:
            click.secho("开始打包本地依赖: " + deploy_conf["layer"]["lib_path"] + "\t->\t" + deploy_conf["layer"][
                "package_name"], blink=True, bold=True)
            layer.package(dict(**deploy_conf["metadata"], **deploy_conf["layer"]))
            click.secho("本地依赖打包完成", fg='green', blink=True, bold=True)

        response = layer.apply(dict(**deploy_conf["metadata"], **deploy_conf["layer"]))
        click.secho("layer 更新完成: " + response["LayerVersionArn"], fg='green', blink=True, bold=True)
        click.secho()

    if "lambda" in deploy_conf.keys():
        lambda_function = ph_lambda.PhLambda()

        if "code_path" in deploy_conf["lambda"]:
            click.secho("开始打包本地代码: " + deploy_conf["lambda"]["code_path"] + "\t->\t" + deploy_conf["lambda"][
                "package_name"], blink=True, bold=True)
            lambda_function.package(dict(**deploy_conf["metadata"], **deploy_conf["lambda"]))
            click.secho("本地代码打包完成", fg='green', blink=True, bold=True)

        response = lambda_function.apply(dict(**deploy_conf["metadata"], **deploy_conf["lambda"]))
        click.secho("lambda 更新完成: " + response["AliasArn"], fg='green', blink=True, bold=True)
        click.secho()

    if "gateway" in deploy_conf.keys():
        gateway = ph_gateway.PhGateway()
        response = gateway.apply(dict(**deploy_conf["metadata"], **deploy_conf["gateway"]))
        click.secho("gateway 更新完成: " + response, fg='green', blink=True, bold=True)
        click.secho()


def __clean_cache(deploy_conf):
    click.secho("开始清理执行缓存", blink=True, bold=True)

    if "layer" in deploy_conf.keys():
        if os.path.exists(deploy_conf["layer"]["package_name"]):
            os.remove(deploy_conf["layer"]["package_name"])

        layer = ph_layer.PhLayer()
        layer_versions = layer.get({"name": deploy_conf["metadata"]["name"]})["LayerVersions"]
        if len(layer_versions) > __DEFAULT_MAX_INST:
            for lv in layer_versions[__DEFAULT_MAX_INST:]:
                layer.delete({
                    "name": deploy_conf["metadata"]["name"],
                    "version": lv["Version"],
                })

    if "lambda" in deploy_conf.keys():
        if os.path.exists(deploy_conf["lambda"]["package_name"]):
            os.remove(deploy_conf["lambda"]["package_name"])

        lambda_function = ph_lambda.PhLambda()
        lambda_aliases = lambda_function.get({"name": deploy_conf["metadata"]["name"]})["Aliases"]
        if len(lambda_aliases) > __DEFAULT_MAX_INST:
            for la in lambda_aliases[__DEFAULT_MAX_INST:]:
                lambda_function.delete({
                    "name": deploy_conf["metadata"]["name"],
                    "version": la["Name"],
                })

    click.secho("执行缓存清理完成", fg='green', blink=True, bold=True)
    click.secho()


@deploy.command("push", short_help='发布function并自动关联到API Gateway')
@click.option('-n', '--name', prompt='指定提交的项目，如果只代理一个项目则无需传入', default='', help='指定提交的项目')
@click.option('-o', '--oper', prompt='执行操作',
              type=click.Choice(['defalut', 'all', 'role', 'lib', 'code', 'api']),
              default='defalut', help='要执行的操作')
def push(name, oper):
    """
    【请在项目的根目录执行】

    \b
    发布依赖到 lambda layer, 发布项目代码到 lambda function, 并在 API Gateway 中关联到当前 lambda function 别名

    \b
    :arg oper:
        :arg default: 默认不传参数，按照预计使用频率，所以只发布 function + gateway
        :arg all: 发布全部资源 （role、layer、function、gateway）
        :arg role: 只发布 role
        :arg lib: 只发布 layer
        :arg code: 只发布 function
        :arg api: 只发布 gateway
    """

    with open(__DEFAULT_CONF_FILE) as f:
        all_conf = yaml.safe_load(f)

    # ensure project name
    if name == "":
        project_name = list(all_conf.keys())[0]
    elif name in all_conf.keys():
        project_name = name
    else:
        click.secho(f"name '{name}' is not exists", fg='red', blink=True, bold=True)
        return

    click.secho(f"开始部署 '{project_name}'", fg='green', blink=True, bold=True)

    # check version and write back, tut the version is not updated when `api` is only released
    deploy_conf, max_version = __check_max_version(all_conf[project_name])
    if "api" == oper:
        deploy_conf["metadata"]["version"] = max_version
        all_conf[project_name] = deploy_conf
        __write_conf(all_conf)
    else:
        deploy_conf["metadata"]["version"] = f"v{int(max_version[1:]) + 1}"
        all_conf[project_name] = deploy_conf
        __write_conf(all_conf)

    # filter operator
    all_operator = {"role": "role", "lib": "layer", "code": "lambda", "api": "gateway"}
    if "defalut" == oper:
        data = {"code": "", "api": ""}
    elif "all" == oper:
        data = {"role": "", "lib": "", "code": "", "api": ""}
    else:
        data = {oper: ""}

    for not_oper in all_operator.keys() - set(data.keys()):
        del deploy_conf[all_operator[not_oper]]

    __apply(deploy_conf)
    __clean_cache(deploy_conf)

    click.secho(f"部署成功 '{project_name}'", fg='green', blink=True, bold=True)
