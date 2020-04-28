


import requests
import json
import sys
import logging



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

    if r.status_code != 200:
        try:
            result = r.json()
        except Exception:
            raise Exception("status_code: {}".format(r.status_code))

        
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

    if debug:
        logging.debug(url)

    return doRequest("POST", url, headers=headers, params=params)
    
def showBucket(host, token, bucketID, debug=False):
    

    if not host :
        raise "host not defined"

    headers = createHeader(token)
    
   

    url = str(host) + "/api/v1/objects/" + bucketID

    
    
    
    return doRequest("GET", url, headers=headers)


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

    if debug:
        logging.debug(url)

    return doRequest("GET", url, headers=headers, params=params)
    