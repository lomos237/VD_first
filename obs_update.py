from obs import ObsClient
import os
import traceback

# 推荐通过环境变量获取AKSK，这里也可以使用其他外部引入方式传入，如果使用硬编码可能会存在泄露风险
# 您可以登录访问管理控制台获取访问密钥AK/SK，获取方式请参见https://support.huaweicloud.com/usermanual-ca/ca_01_0003.html。
# 运行本代码示例之前，请确保已设置环境变量AccessKeyID和SecretAccessKey
ak = os.getenv("HN4LIRUYMNGYDKHXXSPU")
sk = os.getenv("00T1kCbfKqTWzGdPtphcBPuxmTDGObEkEBWitUZl")
# 【可选】如果使用临时AKSK和SecurityToken访问OBS，则同样推荐通过环境变量获取
# security_token = os.getenv("SecurityToken")#  server填写Bucket对应的Endpoint, 这里以华北-北京四为例，其他地区请按实际情况填写
server = "https://obs.cn-north-4.myhuaweicloud.com"
# 创建obsClient实例
# 如果使用临时AKSK和SecurityToken访问OBS，需要在创建实例时通过security_token参数指定securityToken值
obsClient = ObsClient(access_key_id=ak, secret_access_key=sk, server=server)

def out_put_res(resp, objectKey=None):
    if isinstance(resp, list):
        for res in resp:
            out_put_res(res)
    elif isinstance(resp, tuple) and isinstance(resp[1], list):
        out_put_res(resp[1])
    elif isinstance(resp, tuple):
        if resp[1].status < 300:
            print(f'Put File Succeeded, objectkey: {resp[0]}')
        else:
            print(f'Put File Failed, objectkey: {resp[0]}')
            print('requestId:', resp[1].requestId)
            print('errorCode:', resp[1].errorCode)
            print('errorMessage:', resp[1].errorMessage)
    else:
        if resp.status < 300:
            print(f'Put File Succeeded, objectkey: {objectKey}')
        else:
            print(f'Put File Failed, objectkey: {objectKey}')
            print('requestId:', resp.requestId)
            print('errorCode:', resp.errorCode)
            print('errorMessage:', resp.errorMessage)
try:
    bucketName = "pre-datalake-seu"
    # 上传后的文件夹名称，本地文件夹中的所有文件会上传到该文件夹中，注意不要以/结尾
    objectKey = "test"
    # 待上传文件夹的完整路径，如aa/
    folder_path = 'test_data/'
    # 文件夹上传
    resp = obsClient.putFile(bucketName, objectKey, folder_path)
    # resp为文件夹中每个文件上传结果的清单
    out_put_res(resp, objectKey)
except:
    print('Put File Failed')
    print(traceback.format_exc())