import boto3
from phlmd.model.aws_operator import AWSOperator
from phlmd.model.aws_util import AWSUtil
from phlmd.model.ph_role import PhRole
from phlmd.model.ph_layer import PhLayer
from pherrs.ph_err import PhError


class PhLambda(AWSOperator):
    """
    lambda 的源代码
    """

    aws_util = AWSUtil()
    lambda_client = boto3.client('lambda')

    def package(self, data):
        """
        对 lambda 源代码打包
        :param data:
            :arg runtime 运行时字符串，“python” 或者 “nodejs” 或者 “go”
            :arg code_path: lambda 代码位置
            :arg package_name 打包的名称
        """
        runtime_inst = self.aws_util.get_rt_inst(data['runtime'])
        return runtime_inst.pkg_code(data)

    def create(self, data):
        """
        创建 lambda, 并发布一个新版本，然后使用 version 定义一个别名
        :param data:
            :arg name: 创建的 lambda 函数的名字
            :arg version: lambda 函数别名版本
            :arg runtime: lambda 函数适用的运行时，如果多个请使用 “,” 分割, 但是实际使用以第一个为准
            :arg lambda_path: lambda zip 的位置
                            可以是本地（file/python-lambda-example-code.zip，会先被传到 S3）或
                            s3 上的文件（s3://ph-api-lambda/test_ph_lambda_create_local/lambda/python-lambda-example-code-v1.zip）
            :arg role_name: lambda 的代理角色名称
            :arg lambda_handler: lambda 的入口函数
            :arg lambda_layers: lambda 函数依赖的层名称，如果多个请使用 “,” 分割
            :arg lambda_desc: lambda 函数的描述
            :arg lambda_timeout: [int] lambda 的超时时间，默认30s（官方是3s）
            :arg lambda_memory_size: [int] lambda 的使用内存，默认128
            :arg lambda_env: [dict] lambda 的环境变量
            :arg lambda_tag: [dict] lambda 的标签
            :arg vpc_config: [dict] lambda VPC 配置
        """
        bucket_name, object_name = self.aws_util.sync_local_s3_file(
            data["lambda_path"],
            bucket_name=data.get("bucket", self._DEFAULT_BUCKET),
            dir_name=self._DEFAULT_LAMBDA_DIR.replace("#name#", data["name"]),
            version=data.get("version", ""),
        )

        role_arn = PhRole().get({"name": data["role_name"]})["Role"]["Arn"]

        layers_arn = []
        for layer_name in data["lambda_layers"].split(","):
            layers_arn.append(PhLayer().get({"name": layer_name})["LayerVersions"][0]["LayerVersionArn"])

        vpc_config = data['vpc_config'] if 'vpc_config' in data.keys() else {}

        lambda_response = self.lambda_client.create_function(
            FunctionName=data["name"],
            Runtime=data["runtime"].split(",")[0],
            Role=role_arn,
            Handler=data["lambda_handler"],
            Code={
                'S3Bucket': bucket_name,
                'S3Key': object_name,
            },
            Description=data.get("lambda_desc", "aws_lambda_deploy create lambda function"),
            Timeout=data.get("lambda_timeout", 30),
            MemorySize=data.get("lambda_memory_size", 128),
            # Publish=True|False,
            VpcConfig=vpc_config,
            # DeadLetterConfig={
            #     'TargetArn': 'string'
            # },
            Environment={
                'Variables': data.get("lambda_env", {}),
            },
            # KMSKeyArn='string',
            # TracingConfig={
            #     'Mode': 'Active'|'PassThrough'
            # },
            Tags=data.get("lambda_tag", {}),
            Layers=layers_arn,
        )

        response = self.lambda_client.publish_version(
            FunctionName=data["name"],
            # CodeSha256='string',
            # Description='string',
            # RevisionId='string'
        )

        response = self.lambda_client.create_alias(
            FunctionName=data["name"],
            Name=data["version"],
            FunctionVersion=response["Version"],
            Description=data.get("lambda_desc", "aws_lambda_deploy create lambda function"),
            # RoutingConfig={
            #     'AdditionalVersionWeights': {
            #         'string': 123.0
            #     }
            # }
        )

        return response

    def lists(self, data):
        """
        获取所有 lambda 实例
        """
        response = self.lambda_client.list_functions(
            # MasterRegion='string',
            FunctionVersion='ALL',
            # Marker='string',
            MaxItems=50,
        )

        return response

    def get(self, data):
        """
        获取指定 lambda 实例
        :param data:
            :arg name: 创建的 lambda 函数的名字
        """
        try:
            response = self.lambda_client.get_function(
                FunctionName=data["name"],
                # Qualifier='string',
            )

            versions = self.lambda_client.list_versions_by_function(
                FunctionName=data["name"],
                # Marker='string',
                # MaxItems=123
            )["Versions"]
            versions.reverse()
            response["Versions"] = versions

            aliases = self.lambda_client.list_aliases(
                FunctionName=data["name"],
            )["Aliases"]
            aliases.reverse()
            response["Aliases"] = aliases
        except:
            response = {}

        return response

    def update(self, data):
        """
        更新 lambda, 并发布一个新版本，然后使用 version 定义一个别名
        :param data:
            :arg name: 创建的 lambda 函数的名字 【必需】
            :arg version: lambda 函数别名版本
            :arg runtime: lambda 函数适用的运行时，如果多个请使用 “,” 分割, 但是实际使用以第一个为准
            :arg lambda_path: lambda zip 的位置
                            可以是本地（file/python-lambda-example-code.zip，会先被传到 S3）或
                            s3 上的文件（s3://ph-api-lambda/test_ph_lambda_create_local/lambda/python-lambda-example-code-v1.zip）
            :arg role_name: lambda 的代理角色名称
            :arg lambda_handler: lambda 的入口函数
            :arg lambda_layers: lambda 函数依赖的层名称，如果多个请使用 “,” 分割
            :arg lambda_desc: lambda 函数的描述
            :arg lambda_timeout: [int] lambda 的超时时间，默认30s（官方是3s）
            :arg lambda_memory_size: [int] lambda 的使用内存，默认128
            :arg lambda_env: [dict] lambda 的环境变量
            :arg vpc_config: [dict] lambda VPC 配置
        """

        # 更新代码
        if "lambda_path" in data.keys():
            bucket_name, object_name = self.aws_util.sync_local_s3_file(
                data["lambda_path"],
                bucket_name=data.get("bucket", self._DEFAULT_BUCKET),
                dir_name=self._DEFAULT_LAMBDA_DIR.replace("#name#", data["name"]),
                version=data.get("version", ""),
            )

            response = self.lambda_client.update_function_code(
                FunctionName=data["name"],
                # ZipFile=b'bytes',
                S3Bucket=bucket_name,
                S3Key=object_name,
                # S3ObjectVersion='string',
                # Publish=True|False,
                # DryRun=True|False,
                # RevisionId='string'
            )
            del data["lambda_path"]
            return self.update(data)

        # 更新配置
        else:
            response = self.get(data)

            if "role_name" in data.keys():
                role_arn = PhRole().get({"name": data["role_name"]})["Role"]["Arn"]
            else:
                role_arn = response["Configuration"]["Role"]

            layers_arn = []
            if "lambda_layers" in data.keys():
                for layer_name in data["lambda_layers"].split(","):
                    layers_arn.append(PhLayer().get({"name": layer_name})["LayerVersions"][0]["LayerVersionArn"])
            else:
                for layer_name in response["Configuration"]["Layers"]:
                    layers_arn.append(layer_name["Arn"])

            vpc_config = data['vpc_config'] if 'vpc_config' in data.keys() else {}

            lambda_response = self.lambda_client.update_function_configuration(
                FunctionName=data["name"],
                Role=role_arn,
                Handler=data.get("lambda_handler", response["Configuration"]["Handler"]),
                Description=data.get("lambda_desc", response["Configuration"]["Description"]),
                Timeout=data.get("lambda_timeout", response["Configuration"]["Timeout"]),
                MemorySize=data.get("lambda_memory_size", response["Configuration"]["MemorySize"]),
                VpcConfig=vpc_config,
                Environment={
                    'Variables': data.get("lambda_env", response["Configuration"].get("Environment", {}).get("Variables", {})),
                },
                Runtime=data.get("runtime", [response["Configuration"]["Runtime"]]).split(",")[0],
                # DeadLetterConfig={
                #     'TargetArn': 'string'
                # },
                # KMSKeyArn='string',
                # TracingConfig={
                #     'Mode': 'Active'|'PassThrough'
                # },
                # RevisionId='string',
                Layers=layers_arn
            )

        response = self.lambda_client.publish_version(
            FunctionName=data["name"],
            # CodeSha256='string',
            # Description='string',
            # RevisionId='string'
        )

        response = self.lambda_client.create_alias(
            FunctionName=data["name"],
            Name=data["version"],
            FunctionVersion=response["Version"],
            Description=data.get("lambda_desc", "aws_lambda_deploy create lambda function"),
            # RoutingConfig={
            #     'AdditionalVersionWeights': {
            #         'string': 123.0
            #     }
            # }
        )

        return response

    def apply(self, data):
        """
        发布或更新 lambda, 并发布一个新版本，然后使用 version 定义一个别名
        :param data:
            :arg name: 创建的 lambda 函数的名字
            :arg version: lambda 函数别名版本
            :arg runtime: lambda 函数适用的运行时，如果多个请使用 “,” 分割, 但是实际使用以第一个为准
            :arg lambda_path: lambda zip 的位置
                            可以是本地（file/python-lambda-example-code.zip，会先被传到 S3）或
                            s3 上的文件（s3://ph-api-lambda/test_ph_lambda_create_local/lambda/python-lambda-example-code-v1.zip）
            :arg role_name: lambda 的代理角色名称
            :arg lambda_handler: lambda 的入口函数
            :arg lambda_layers: lambda 函数依赖的层名称，如果多个请使用 “,” 分割
            :arg lambda_desc: lambda 函数的描述
            :arg lambda_timeout: [int] lambda 的超时时间，默认30s（官方是3s）
            :arg lambda_memory_size: [int] lambda 的使用内存，默认128
            :arg lambda_env: [dict] lambda 的环境变量
            :arg lambda_tag: [dict] lambda 的标
        """
        if self.get(data) == {}:
            return self.create(data)
        else:
            return self.update(data)

    def stop(self, data):
        """
        停止 lambda 继续处理请求，并发数设置为 0
        :param data:
            :arg name: 创建的 lambda 函数的名字 【必需】
        """
        data["lambda_concurrent"] = 0
        return self.start(data)

    def start(self, data):
        """
        重新激活 lambda，使其可以接受请求，并发数设置为指定值或默认值
        :param data:
            :arg name: 创建的 lambda 函数的名字 【必需】
            :arg lambda_concurrent: 分配给别名的预留并发值, 不指定则使用非预留账户并发
        """

        if "lambda_concurrent" in data.keys():
            response = self.lambda_client.put_function_concurrency(
                FunctionName=data["name"],
                ReservedConcurrentExecutions=data["lambda_concurrent"],
            )
        else:
            response = self.lambda_client.delete_function_concurrency(
                FunctionName=data["name"],
            )

        return response

    def delete(self, data):
        """
        删除 lambda, 版本 或 别名
        :param data:
            :arg name: 创建的 lambda 函数的名字 【必需】
            :arg version: lambda 函数别名版本, 不传或 #ALL# 则删除整个 lambda 函数
        """

        if "version" not in data.keys() or data["version"] == "#ALL#":
            response = self.lambda_client.delete_function(
                FunctionName=data["name"],
            )
        else:
            func_version = self.lambda_client.get_alias(
                FunctionName=data["name"],
                Name=data["version"],
            )["FunctionVersion"]

            response = self.lambda_client.delete_alias(
                FunctionName=data["name"],
                Name=data["version"],
            )

            response = self.lambda_client.delete_function(
                FunctionName=data["name"],
                Qualifier=func_version,
            )

        return response


