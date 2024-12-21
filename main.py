import asyncio
from execute_upload_biometry import execute_upload_biometry


if __name__ == '__main__':
    try:
        print('hello')

        asyncio.run(execute_upload_biometry())
    except:
        pass
