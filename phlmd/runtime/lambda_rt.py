import os
from abc import ABCMeta, abstractmethod
import click
from pherrs.ph_err import PhError


class LambdaRuntime(metaclass=ABCMeta):
    """
    封装 AWS Lambda 各个运行时的的常规操作和抽象方法
    """

    _package_root = ".package"
    _package_cmds = []

    @abstractmethod
    def pkg_layer(self, data):
        pass

    @abstractmethod
    def pkg_code(self, data):
        pass

    def package(self, package_name):
        """
        执行打包逻辑
        :param package_name 打包的名称
        """

        if not isinstance(self._package_cmds, list):
            raise PhError('Expected an list')

        self._package_cmds.extend([
            "mv package.zip %s" % package_name,
        ])

        try:
            for cmd in self._package_cmds:
                click.secho(f"正在执行: {cmd} ", fg='green', blink=True, bold=True)
                os.system(cmd)
        except Exception as ex:
            print(ex)
