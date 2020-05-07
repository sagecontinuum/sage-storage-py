


import requests
import json
import sys
import logging
from requests_toolbelt.multipart.encoder import MultipartEncoder
import os

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
def createBucket(host, token, name, datatype, debug=False):
    
    if not host :
        raise "host not defined"

    headers = createHeader(token)
    

    params = {}
    
    if name:
        params["name"] = name

    if datatype:
        params["type"] = datatype

    url = str(host) + "/api/v1/objects"

    return doRequest("POST", url, headers=headers, params=params)
    
def showBucket(host, token, bucketID, debug=False):
    

    if not host :
        raise "host not defined"

    headers = createHeader(token)

    url = str(host) + "/api/v1/objects/" + bucketID


    return doRequest("GET", url, headers=headers)

def deleteBucket(host, token, bucketID, debug=False):
    

    if not host :
        raise "host not defined"

    headers = createHeader(token)

    url = str(host) + "/api/v1/objects/" + bucketID


    return doRequest("DELETE", url, headers=headers)

# TODO add query
def listBuckets(host, token, debug=False):
    

    if not host :
        raise "host not defined"

    headers = createHeader(token)
    
    url = str(host) + "/api/v1/objects"

    return doRequest("GET", url, headers=headers)
    




def getPermissions(host, token, bucketID, debug=False):
    

    if not host :
        raise "host not defined"

    headers = createHeader(token)
    
   
    params = {"permissions" : True}
    url = str(host) + "/api/v1/objects/" + bucketID 

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
    url = str(host) + "/api/v1/objects/" + bucketID 

    return doRequest("PUT", url, headers=headers, params=params, data=data)

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
    url = str(host) + "/api/v1/objects/" + bucketID 

    return doRequest("PUT", url, headers=headers, params=params, data=data)

def uploadFile(host, token, bucketID, localFile, key):

    if not host :
        raise "host not defined"

    if not key:
        key = ""
    headers = createHeader(token)
    
   
    
    localFileBase = os.path.basename(localFile)

        

    mp_encoder = MultipartEncoder(
        fields={
            
            # plain file object, no filename or mime type produces a
            # Content-Disposition header with just the part name
            # (filename, data, content_type, headers)
            'file': (localFileBase, open(localFile, 'rb')),
        }
    )

    headers['Content-Type'] = mp_encoder.content_type

    url = str(host) + "/api/v1/objects/" + bucketID + "/" + key

    return doRequest("PUT", url, headers=headers, data=mp_encoder)



def listFiles(host, token, bucketID, prefix, recursive):

    
    if not host :
        raise "host not defined"

    headers = createHeader(token)
    
    if not prefix:
        prefix = ""

    url = str(host) + "/api/v1/objects/"+bucketID +"/"+prefix
    
    params = {}
    if recursive:
        params["recursive"] = True

    return doRequest("GET", url, headers=headers, params=params)