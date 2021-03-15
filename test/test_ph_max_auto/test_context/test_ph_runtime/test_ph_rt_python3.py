import base64

from phcli.ph_max_auto.phcontext.ph_runtime.ph_rt_python3 import PhRTPython3
from phcli.ph_max_auto import define_value as dv
from phcli.ph_aws.ph_s3 import PhS3
from phcli.ph_aws.ph_sts import PhSts


def main():
    print("test_main方法被调用")
    phsts1 = PhSts().assume_role(
        base64.b64decode(dv.ASSUME_ROLE_ARN).decode(),
        dv.ASSUME_ROLE_EXTERNAL_ID,
    )
    phs3_test = PhS3(phsts=phsts1)

    source_path1 = "/workspace/BPBatchDAG/phjobs/test1/test1"
    target_path1 = "/workspace/phdagcommand/test/test_ph_max_auto/test_context/test_runtime"
    t = PhRTPython3(phs3=phs3_test, target_path=target_path1)
    t.c9_to_jupyter(phs3_test, source_path1, target_path1)


if __name__ == '__main__':
    main()
    print("success")