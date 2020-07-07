import pytest
from phlmd.model.ph_role import PhRole


def test_ph_role_package():
    pass


@pytest.mark.skip("Deprecation")
def test_ph_role_create_local():
    role_name = "test_ph_role_create_local"
    arpd_path = "file/trust-policy.json"
    args = {
        "name": role_name,
        "arpd_path": arpd_path,
    }
    assert PhRole().create(args) != {}


def test_ph_role_create_s3():
    role_name = "test_ph_role_create_s3"
    arpd_path = "s3://ph-api-lambda/template/role/trust-policy-template.json"
    args = {
        "name": role_name,
        "arpd_path": arpd_path,
        "policys_arn": [
            "arn:aws-cn:iam::aws:policy/service-role/AWSLambdaRole",
            "arn:aws-cn:iam::aws:policy/AWSLambdaExecute",
            # "arn:aws-cn:iam::{{aws_id}}:policy/AWSKmsDecrypt",
            "arn:aws-cn:iam::aws:policy/AmazonAPIGatewayInvokeFullAccess"
        ],
    }
    assert PhRole().create(args) != {}


def test_ph_role_lists():
    pass


def test_ph_role_get():
    role_name = "test_ph_role_create_s3"
    args = {
        "name": role_name,
    }
    assert PhRole().get(args) != {}


def test_ph_role_update():
    role_name = "test_ph_role_create_s3"
    arpd_path = "s3://ph-api-lambda/template/role/trust-policy-template.json"
    args = {
        "name": role_name,
        "arpd_path": arpd_path,
    }
    assert PhRole().update(args) != {}


def test_ph_role_apply():
    role_name = "test_ph_role_create_s3"
    arpd_path = "s3://ph-api-lambda/template/role/trust-policy-template.json"
    args = {
        "name": role_name,
        "arpd_path": arpd_path,
    }
    assert PhRole().apply(args) != {}


def test_ph_role_stop():
    pass


def test_ph_role_start():
    pass


@pytest.mark.skip("Deprecation")
def test_ph_role_delete_local():
    args = {
        "name": "test_ph_role_create_local",
    }
    assert PhRole().delete(args) != {}


@pytest.mark.skip(reason='Used for test_ph_lambda Test')
def test_ph_role_delete_s3():
    args = {
        "name": "test_ph_role_create_s3",
    }
    assert PhRole().delete(args) != {}
