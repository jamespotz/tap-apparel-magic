import os
import base64

def main(data, context):
    stream = os.popen('sh execute.sh')
    output = stream.read()
    print(output)

if __name__ == "__main__":
    main('data', 'context')
