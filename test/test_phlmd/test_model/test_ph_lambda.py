import os
import pytest
from phlmd.model.ph_lambda import PhLambda


def test_ph_lambda_package_python():
    args = {
        "runtime": "python",
        "code_path": "./phlmd/",
        "package_name": "test_ph_lambda_package_python.zip",
    }

    PhLambda().package(args)
    assert os.path.exists(args["package_name"])
    os.remove("test_ph_lambda_package_python.zip")


@pytest.mark.skip("nodejs unrealized")
def test_ph_lambda_package_nodejs():
    pass


@pytest.mark.skip(reason='Deprecation')
def test_ph_lambda_create_local():
    args = {
        "name": "test_ph_lambda_create_local",
        "version": "v1",
        "runtime": "python3.8,python3.6",
        "lambda_path": "file/python-lambda-example-code.zip",
        "role_name": "test_ph_role_create_s3",
        "lambda_handler": "hello_world.app.lambda_handler",
        "lambda_layers": "test_ph_layer_create_s3",
        "lambda_desc": "test_ph_lambda_create_local 单元测试",
        "lambda_timeout": 50,
        "lambda_memory_size": 128,
        "lambda_env": {'TEST': 'test'},
        "lambda_tag": {"language": "python"},
    }
    assert PhLambda().create(args) != {}


def test_ph_lambda_create_s3():
    args = {
        "name": "test_ph_lambda_create_s3",
        "version": "v1",
        "runtime": "python3.8,python3.6",
        "lambda_path": "s3://ph-api-lambda/example/lambda/example-code-v10.zip",
        "role_name": "test_ph_role_create_s3",
        "lambda_handler": "hello_world.app.lambda_handler",
        "lambda_layers": "test_ph_layer_create_s3",
        "lambda_desc": "test_ph_lambda_create_s3 单元测试",
        "lambda_timeout": 50,
        "lambda_memory_size": 128,
        "lambda_env": {'TEST': 'test'},
        "lambda_tag": {"language": "python"},
    }
    assert PhLambda().create(args) != {}


def test_ph_lambda_lists():
    args = {}
    assert PhLambda().lists(args) != {}


def test_ph_lambda_get():
    args = {
        "name": "test_ph_lambda_create_s3",
    }
    assert PhLambda().get(args) != {}


def test_ph_lambda_update():
    args = {
        "name": "test_ph_lambda_create_s3",
        "version": "v2",
        "runtime": "python3.8",
        "lambda_handler": "hello_world.app.lambda_handler",
        "lambda_layers": "test_ph_layer_create_s3",
        "lambda_desc": "test_ph_lambda_update 单元测试",
        "lambda_timeout": 30,
        "lambda_memory_size": 138,
        "lambda_env": {'TEST': 'test'},
    }
    assert PhLambda().update(args) != {}


def test_ph_lambda_stop():
    args = {
        "name": "test_ph_lambda_create_s3",
    }
    assert PhLambda().stop(args) != {}


def test_ph_lambda_start():
    args = {
        "name": "test_ph_lambda_create_s3",
    }
    assert PhLambda().start(args) != {}


@pytest.mark.skip(reason='Deprecation')
def test_ph_lambda_delete_local():
    args = {
        "name": "test_ph_lambda_create_local",
    }
    assert PhLambda().delete(args) != {}


@pytest.mark.skip(reason='Used for test_ph_gateway Test')
def test_ph_lambda_delete_s3():
    args = {
        "name": "test_ph_lambda_create_s3",
    }
    assert PhLambda().delete(args) != {}
