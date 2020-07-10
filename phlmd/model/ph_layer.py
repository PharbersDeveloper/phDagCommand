import boto3
from phlmd.model.aws_operator import AWSOperator
from phlmd.model.aws_util import AWSUtil
from pherrs.ph_err import PhError


class PhLayer(AWSOperator):
    """
    lambda 的依赖层
    """

    aws_util = AWSUtil()
    lambda_client = boto3.client('lambda')

    def package(self, data):
        """
        对 lambda layer 按照 runtime 打包
        :param data:
            :arg runtime 运行时字符串，“python” 或者 “nodejs” 或者 “go”
            :arg lib_path: python 运行时，仅当 is_pipenv = False 时有效，指定 python 的 lib 位置
            :arg package_name 打包的名称
            :arg is_pipenv: 是否使用的 pipenv 构建的项目，默认为 True
        """
        runtime_inst = self.aws_util.get_rt_inst(data['runtime'])
        return runtime_inst.pkg_layer(data)

    def create(self, data):
        """
        创建 lambda 的 layer
        :param data:
            :arg name: layer 名字
            :arg version: layer 版本
            :arg layer_path: layer zip 的位置
                            可以是本地（python-lambda-example-layer.zip，会先被传到 S3）或
                            s3 上的文件（https://ph-lambda-layer.s3.cn-northwest-1.amazonaws.com.cn/python-lambda-example-layer.zip）
            :arg runtime: layer 适用的运行时，如果多个请使用 “,” 分割
            :arg layer_desc: layer 的描述
        """

        bucket_name, object_name = self.aws_util.sync_local_s3_file(
            path=data["layer_path"],
            bucket_name=data.get("bucket", self._DEFAULT_BUCKET),
            dir_name=self._DEFAULT_LAYER_DIR.replace("#name#", data["name"]),
            version=data.get("version", ""),
        )

        response = self.lambda_client.publish_layer_version(
            LayerName=data["name"],
            Description=data.get("layer_desc", "aws_lambda_deploy create layer"),
            Content={
                'S3Bucket': bucket_name,
                'S3Key': object_name,
            },
            CompatibleRuntimes=data["runtime"].split(","),
            LicenseInfo='MIT'
        )

        return response

    def lists(self, data):
        """
        获取所有 layer 实例
        :param data:
            :arg runtime: layer 适用的运行时，只可指定一个【不强制】
            :arg name: layer 名字
        """
        if "name" in data.keys():
            response = self.lambda_client.list_layer_versions(
                LayerName=data["name"],
            )
        elif "runtime" in data.keys():
            response = self.lambda_client.list_layers(
                CompatibleRuntime=data["runtime"],
            )
        else:
            response = self.lambda_client.list_layers()

        return response

    def get(self, data):
        """
        获取指定的 layer 实例
        :param data:
            :arg name: layer 名字可加版本
        """
        response = self.lambda_client.list_layer_versions(
            LayerName=data["name"].split(":")[0],
        )
        if not len(response["LayerVersions"]):
            return {}

        version = data["name"].split(":")[1:2]
        if len(version):
            for layer in response["LayerVersions"]:
                if int(version[0]) == layer["Version"]:
                    response["LayerVersions"] = [layer]
                    break

        return response

    def update(self, data):
        """
        更新 lambda 的 layer，等于 create layer
        """
        return self.create(data)

    def apply(self, data):
        """
        发布或更新 lambda 的 layer, 等于 create layer
        """
        if self.get(data) == {}:
            return self.create(data)
        else:
            return self.update(data)

    def stop(self, data):
        """
        lambda 的 layer 不可停止
        """
        print(self.stop.__doc__)

    def start(self, data):
        """
        lambda 的 layer 不可启动
        """
        print(self.start.__doc__)

    def delete(self, data):
        """
        删除 lambda 的 layer
        :param data:
            :arg name: 要删除的 layer 名字
            :arg version: 要删除的 layer 版本
        """
        response = self.lambda_client.delete_layer_version(
            LayerName=data["name"],
            VersionNumber=data["version"],
        )

        return response
