import os
import pytest
from phlmd.model.ph_layer import PhLayer


def test_ph_layer_package_python():
    args = {
        "runtime": "python",
        "package_name": "test_ph_layer_package_python.zip",
        "lib_path": ".venv/lib/python3.8/site-packages/",
    }

    PhLayer().package(args)
    assert os.path.exists(args["package_name"])
    os.remove("test_ph_layer_package_python.zip")


@pytest.mark.skip(reason='nodejs unrealized')
def test_ph_layer_package_nodejs():
    args = {
        "runtime": "nodejs",
        "package_name": "test_ph_layer_package_nodejs.zip",
    }

    PhLayer().package(args)
    assert os.path.exists(args["package_name"])
    os.remove("test_ph_layer_package_nodejs.zip")


@pytest.mark.skip(reason='Deprecation')
def test_ph_layer_create_local():
    args = {
        "name": "test_ph_layer_create_local",
        "version": "v1",
        "layer_path": "file/python-lambda-example-layer.zip",
        "runtime": "python3.8,python3.6",
    }
    assert PhLayer().create(args) != {}


def test_ph_layer_create_s3():
    args = {
        "name": "test_ph_layer_create_s3",
        "version": "v1",
        "layer_path": "s3://ph-api-lambda/example/layer/example-layer-v5.zip",
        "runtime": 'python3.8,python3.6',
    }
    assert PhLayer().create(args) != {}


def test_ph_layer_lists():
    args = {
        "runtime": "python3.8"
    }
    assert PhLayer().lists(args) != {}


def test_ph_layer_get():
    args = {
        "name": "test_ph_layer_create_s3",
    }
    assert PhLayer().get(args) != {}


@pytest.mark.skip(reason='Deprecation')
def test_ph_layer_delete_local():
    args = {
        "name": "test_ph_layer_create_local",
    }
    args["version"] = PhLayer().get(args)["LayerVersions"][0]["Version"]
    assert PhLayer().delete(args) != {}


@pytest.mark.skip(reason='Used for test_ph_lambda Test')
def test_ph_layer_delete_s3():
    args = {
        "name": "test_ph_layer_create_s3",
    }
    args["version"] = PhLayer().get(args)["LayerVersions"][0]["Version"]
    assert PhLayer().delete(args) != {}
