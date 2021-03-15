
from phcli.ph_max_auto.phcontext.ph_runtime.ph_rt_python3 import PhRTPython3
from phcli.ph_aws.ph_s3 import PhS3



def main():
        
        
        source_path = '/workspace/BPBatchDAG/phjobs/test1/phconf.yaml'
        
        target_path = '/workspace/phdagcommand/test/test_ph_max_auto/test_context/test_ph_runtime'
        
        PhRTPython3.c9_to_jupyter(self, source_path=source_path, target_path=target_path)
        
        print('success')
    
if __name__ == '__main__':
     main()