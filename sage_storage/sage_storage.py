


import requests
import json
import sys
import logging
from requests_toolbelt.multipart.encoder import MultipartEncoder
import os
from pathlib import Path

def createHeader(token):
    headers = {}
    if token:
        headers["Authorization"] = "sage "+token
    return headers


def doRequest(method, url, **kwargs):

    logging.debug(method +" "+url)

    try:
        r = requests.request(method, url, **kwargs)
    except:
        raise
    #print("test: " +r.text)
    if r.status_code != 200:
        try:
            result = r.json()
        except Exception:
            #print("could not get json: \"" +r.text+"\"")
            raise Exception("status_code: {}".format(r.status_code))

        #print("test: " +r.text)

        # if there is an error field, there is no need to raise an exception
        if "error" in result:
            return result

        raise Exception("status_code: {} {}".format(r.status_code, result))

    try:
        #result = r.json()
        json_data = json.loads(r.text)
    except:
        raise

    return json_data

#curl  -X POST 'localhost:8080/api/v1/objects?type=training-data&name=mybucket'  -H "Authorization: sage ${SAGE_USER_TOKEN}"
def createBucket(host, token, name, datatype):
    
    if not host :
        raise "host not defined"

    headers = createHeader(token)
    

    params = {}
    
    if name:
        params["name"] = name

    if datatype:
        params["type"] = datatype

    url = f'{host}/api/v1/objects'

    return doRequest("POST", url, headers=headers, params=params)
    
def showBucket(host, token, bucketID):
    

    if not host :
        raise "host not defined"

    headers = createHeader(token)

    url = f'{host}/api/v1/objects/{bucketID}'


    return doRequest("GET", url, headers=headers)

def deleteBucket(host, token, bucketID):
    

    if not host :
        raise "host not defined"

    headers = createHeader(token)

    url = f'{host}/api/v1/objects/{bucketID}'


    return doRequest("DELETE", url, headers=headers)

# TODO add query
def listBuckets(host, token):
    

    if not host :
        raise "host not defined"

    headers = createHeader(token)
    
    url = f'{host}/api/v1/objects'

    return doRequest("GET", url, headers=headers)
    




def getPermissions(host, token, bucketID):
    

    if not host :
        raise "host not defined"

    headers = createHeader(token)
    
   
    params = {"permissions" : True}
    url = f'{host}/api/v1/objects/{bucketID}'

    return doRequest("GET", url, headers=headers, params=params)
    

# curl -X PUT "localhost:8080/api/v1/objects/${BUCKET_ID}?permissions" 
# -d '{"granteeType": "USER", "grantee": "otheruser", "permission": "READ"}' -H "Authorization: sage ${SAGE_USER_TOKEN}"
def addPermissions(host, token, bucketID, granteeType, grantee, permission):
    if not host :
        raise "host not defined"

    dataDict = {
        "granteeType": granteeType,
        "grantee": grantee,
        "permission": permission
    }

    data = json.dumps(dataDict)

    headers = createHeader(token)
    params = {"permissions" : True}
    url = f'{host}/api/v1/objects/{bucketID}' 

    return doRequest("PUT", url, headers=headers, params=params, data=data)

# curl -X DELETE "localhost:8080/api/v1/objects/${BUCKET_ID}?permissions&grantee=USER:otheruser:READ" -H "Authorization: sage ${SAGE_USER_TOKEN}"
#  last argument "permission" is optional
def deletePermissions(host, token, bucketID, granteeType, grantee, permission):
    if not host :
        raise "host not defined"

    #dataDict = {
    #    "granteeType": granteeType,
    #    "grantee": grantee,
    #    "permission": permission
    #}

    #data = json.dumps(dataDict)

    headers = createHeader(token)

    permissionTuple = f'{granteeType}:{grantee}'
    if permission:
        permissionTuple = f'{granteeType}:{grantee}:{permission}'

    params = {"permissions" : True,
                "grantee" : permissionTuple
            }

    url = f'{host}/api/v1/objects/{bucketID}' 

    return doRequest("DELETE", url, headers=headers, params=params)

def makePublic(host, token, bucketID):
    if not host :
        raise "host not defined"

    dataDict = {
        "granteeType": "GROUP",
        "grantee": "AllUsers",
        "permission": "READ"
    }

    data = json.dumps(dataDict)

    headers = createHeader(token)
    params = {"permissions" : True}
    url = f'{host}/api/v1/objects/{bucketID}' 

    return doRequest("PUT", url, headers=headers, params=params, data=data)


def uploadDirectory(host, token, bucketID, sourceDirectory, key):

    if not host :
        raise "host not defined"

    if not key:
        key = ""


    headers = createHeader(token)
    
   
    
    localFileBase = os.path.basename(localFile)

        
    # streaming multipart form-data object
    mp_encoder = MultipartEncoder(
        fields={
            
            # plain file object, no filename or mime type produces a
            # Content-Disposition header with just the part name
            # (filename, data, content_type, headers)
            'file': (localFileBase, open(localFile, 'rb')),
        }
    )


    headers['Content-Type'] = mp_encoder.content_type

    if len(key) > 0:
        if key[0] == '/':
            key = key[1:]

    
    url = f'{host}/api/v1/objects/{bucketID}/{key}'
    print(url)
    
    return doRequest("PUT", url, headers=headers, data=mp_encoder)



def _uploadFile(host, token, bucketID, source, key):
    localFileBase = os.path.basename(source)


    headers = createHeader(token)


    # streaming multipart form-data object
    mp_encoder = MultipartEncoder(
        fields={
            
            # plain file object, no filename or mime type produces a
            # Content-Disposition header with just the part name
            # (filename, data, content_type, headers)
            'file': (localFileBase, open(source, 'rb')),
        }
    )


    headers['Content-Type'] = mp_encoder.content_type


    url = f'{host}/api/v1/objects/{bucketID}/{key}'
    print(url)
    
    result = doRequest("PUT", url, headers=headers, data=mp_encoder)

    if result == None:
        raise Exception("doRequest returned None")

    if isinstance(result,dict):
        if "error" in result:
            return result

    return

# upload files and direcories specifies in sources
# directories are copied recursively
def upload(host, token, bucketID, sources, key):

    if not host :
        raise "host not defined"

    if not key:
        key = ""

    if len(key) > 0 and key[0] == '/':
        key = key[1:]

    if len(sources) > 1 and len(key) > 0:
        if key[-1] != "/":
            raise Exception("Target key should end with a \"/\" to indicate a directory")

    #headers = createHeader(token)
    
    count = 0

    for source in sources:

        if os.path.isfile(source):

            result = _uploadFile(host, token, bucketID, source, key)
            if isinstance(result,dict):
                if "error" in result:
                    return result
            
            #count += 1


        if os.path.isdir(source):

            sourceDirName = source
            if sourceDirName[-1]=="/":
                sourceDirName = sourceDirName[:-1]

            sourceDirName = os.path.basename(sourceDirName)

            

            #print(f'is dir {sourceDirName} ({source})')
            for root, dirs, files in os.walk(source):
                #print(root)
                #print(dirs)
                #print(files)
                root_suffix = root[len(source):]
                if len(root_suffix) > 0 and root_suffix[0] == "/":
                    root_suffix = root_suffix[1:]
                path = root.split(os.sep)
                #print((len(path) - 1) * '---', os.path.basename(root))
                for file in files:

                    localFile = os.path.join(root, file)
                    print(f'localFile: {localFile}')

                    print(f'key: {key} sourceDirName: {sourceDirName}  root_suffix: {root_suffix} file: {file}')
                    remoteKey = os.path.join( key, sourceDirName, root_suffix, file)
                    print(f'remoteKey: {remoteKey}')

                    result = _uploadFile(host, token, bucketID, localFile, remoteKey)
                    if isinstance(result,dict):
                        if "error" in result:
                            return result

                    count += 1      

                    #print(root)
                    #print(len(path) * '---', file)


    obj = {}
    obj["files_uploaded"] = count
    return obj

# TODO 
# - add option to preserve path of key 
def downloadFile(host, token, bucketID, key, target):

    if not host :
        raise Exception("host not defined")

    if not key:
        raise Exception("key not defined")

    if key == "":
        raise Exception("key is empty")


    # create targetFile string 
    if target:

        if target[-1] == "/":
            targetFile = os.path.join(target, os.path.basename(key))
        else:
            targetFile = target

    else:
        targetFile = os.path.basename(key)

    # prevent overwrite
    targetFileObject = Path(targetFile)
    if targetFileObject.exists():
        raise Exception("target file already exists")

    tempFile = f'{targetFile}.part'


    headers = createHeader(token)
    

    
    if len(key) > 0:
        if key[0] == '/':
            key = key[1:]

    
    url = f'{host}/api/v1/objects/{bucketID}/{key}'


    with requests.get(url, headers=headers, stream=True) as r:
        r.raise_for_status()
        with open(tempFile, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024): 
                # If you have chunk encoded response uncomment if
                # and set chunk_size parameter to None.
                #if chunk: 
                f.write(chunk)

    os.rename(tempFile, targetFile)

    return
    #return doRequest("GET", url, headers=headers, data=mp_encoder)



def listFiles(host, token, bucketID, prefix, recursive):

    
    if not host :
        raise "host not defined"

    headers = createHeader(token)
    
    if not prefix:
        prefix = ""

    url = f'{host}/api/v1/objects/{bucketID}/{prefix}'
    
    params = {}
    if recursive:
        params["recursive"] = True

    return doRequest("GET", url, headers=headers, params=params)


def deleteFile(host, token, bucketID, key):

    
    if not host :
        raise "host not defined"

    headers = createHeader(token)
    
    
    url = f'{host}/api/v1/objects/{bucketID}/{key}'
    
    params = {}
    #if recursive:
    #    params["recursive"] = True

    return doRequest("DELETE", url, headers=headers, params=params)