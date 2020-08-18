from ph_aws.ph_sts import PhSts
from ph_aws.ph_s3 import PhS3

phsts = PhSts()
phsts = phsts.assume_role('arn:aws-cn:iam::444603803904:role/Ph-Cli-Lmd', 'Ph-Cli-Lmd')

phs3 = PhS3(phsts=phsts)

TEST_BUCKET = 'ph-platform'
TEST_OBJECT = '2020-08-10/template/python/phcli/lmd/ph-lambda-deploy-template.yaml'


def test_ph_s3_list_buckets():
    assert phs3.list_buckets()


def test_ph_s3_open_object_by_lines():
    assert phs3.open_object_by_lines(TEST_BUCKET, TEST_OBJECT)


def test_ph_s3_download():
    phs3.download(TEST_BUCKET, TEST_OBJECT, 'test.yaml')



