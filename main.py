import asyncio
from execute_upload_biometry import BiometryUploadBiometry

if __name__ == '__main__':
    try:
        print('hello')
        utility = BiometryUploadBiometry()
        asyncio.run(utility.execute_upload_biometry())
    except:
        pass
