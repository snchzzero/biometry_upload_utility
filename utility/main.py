import asyncio

from execute_upload_biometry import BiometryUploadBiometry

if __name__ == '__main__':
    utility = BiometryUploadBiometry()
    try:
        asyncio.run(utility.execute_upload_biometry())
        asyncio.run(utility.stop_services())
    except Exception as ex:
        print('Biometry upload: Error: ', ex)
    finally:
        asyncio.run(utility.stop_services())
