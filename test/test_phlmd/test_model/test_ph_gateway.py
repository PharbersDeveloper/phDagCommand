import os
import pytest
from phlmd.model.ph_gateway import PhGateway

@pytest.mark.dependency(depends=['test_ph_lambda.test_ph_lambda_create_s3'])
def test_ph_gateway_create():
    args = {
        "name": "test_ph_gateway_create",
        "rest_api_id": "2t69b7x032",
        "api_template": "s3://ph-api-lambda/template/gateway/jsonapi-openapi-template.yaml",
        "role_name": "example-lambda-role",
        "lambda_name": "example",
        "version": "v2",
    }
    assert PhGateway().create(args) != {}


def test_ph_gateway_lists():
    assert PhGateway().lists({}) != {}


def test_ph_gateway_get():
    args = {
        "name": "test_ph_gateway_create",
        "rest_api_id": "cf3t42hwhl",
    }
    assert PhGateway().get(args) != {}


def test_ph_gateway_update():
    args = {
        "name": "test_ph_gateway_create",
        "rest_api_id": "cf3t42hwhl",
        "api_template": "s3://ph-api-lambda/template/gateway/jsonapi-openapi-template.yaml",
        "role_name": "test_ph_role_create_s3",
        "lambda_name": "test_ph_lambda_create_s3",
        "version": "v1",
    }
    assert PhGateway().update(args) != {}


def test_ph_gateway_apply():
    args = {
        "name": "test_ph_gateway_create",
        "rest_api_id": "2t69b7x032",
        "api_template": "s3://ph-api-lambda/template/gateway/jsonapi-openapi-template.yaml",
        "role_name": "example-lambda-role",
        "lambda_name": "example",
        "version": "v2",
    }
    assert PhGateway().apply(args) != {}


def test_ph_gateway_delete():
    args = {
        "name": "test_ph_gateway_create",
        "rest_api_id": "cf3t42hwhl",
    }
    assert PhGateway().delete(args) != {}
