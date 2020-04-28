


import requests
import json

import logging


#curl  -X POST 'localhost:8080/api/v1/objects?type=training-data&name=mybucket'  -H "Authorization: sage ${SAGE_USER_TOKEN}"
def createBucket(host, token, name, datatype, debug=False):
    
    if not host :
        raise "host not defined"


    headers = {}
    if token:
        headers["Authorization"] = "sage "+token

    params = {}
    
    if name:
        params["name"] = name

    if datatype:
        params["type"] = datatype

    url = str(host) + "/api/v1/objects"

    if debug:
        logging.debug(url)

    try:
        r = requests.post(url, headers=headers , params=params)
    except Exception as e:
        raise

    if r.status_code != 200:
        try:
            result = r.json()
        except Exception as e:
            raise Exception("status_code: {}".format(r.status_code))

        
        raise Exception("status_code: {} {}".format(r.status_code, result))

    try:
        #result = r.json()
        json_data = json.loads(r.text)
    except Exception as e:
        raise

    return json_data

    

    


    
