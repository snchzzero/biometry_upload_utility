import asyncio
from execute_upload_biometry import BiometryUploadBiometry

if __name__ == '__main__':
    try:
        print('hello')
        utility = BiometryUploadBiometry()
        asyncio.run(utility.execute_upload_biometry())
        print('main - done')
    except Exception as ex:
        print('Close main error: ', ex)
