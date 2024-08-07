import time
import shopify
import json
import requests
import mimetypes
import base64

class ShopifyUploader:
    def __init__(self,token,site,api="2024-01"):
        
        self.key = token
        self.site = site
        self.uploaded = {}
        session = shopify.Session(f"{self.site}.myshopify.com/admin",api,token)
        shopify.ShopifyResource.activate_session(session)
        
    def check_upload(self,filename,original_filename=None):
    
        if filename in self.uploaded:
            return self.uploaded[filename]
        if original_filename is not None and original_filename in self.uploaded:
            return self.uploaded[original_filename]
        
        result = json.loads(shopify.GraphQL().execute(
            """
            query GetFile($query:String) {
                files(first: 1, query: $query) {
                    nodes {
                        id
                        preview {
                            image {
                                url
                                id
                            }
                        }
                    }
                }
            }
            """,
            {"query":f'filename:"{filename}"'}
        ))
        
        if not 'data' in result:
            print(json.dumps(result,indent=1))
        
        nodes = result['data']['files']['nodes']
        if len(nodes)>0:
            self.uploaded[filename] = {
                "url":result["data"]["files"]['nodes'][0]['preview']['image']['url'],
                "id":result["data"]["files"]['nodes'][0]['id']
            }
            if original_filename is not None:
                self.uploaded[original_filename] = {
                    "url":result["data"]["files"]['nodes'][0]['preview']['image']['url'],
                    "id":result["data"]["files"]['nodes'][0]['id']
                }   
            return self.uploaded[filename]
        else:
            return None
    def actual_filename(self,url):
        filename = url.split("?")[0].split("/")[-1]
        if filename in self.uploaded:
            return self.uploaded[filename]["url"]
        res = requests.head(url)
        filename = f'{".".join(filename.split(".")[0:-1])}{mimetypes.guess_extension(res.headers["Content-Type"])}'
        return filename
    def upload_image(self,url, alt="",check=True):
        filename = self.actual_filename(url)
        if filename in self.uploaded:
            return self.uploaded[filename]
        res = requests.head(url)
        filename = f'{".".join(filename.split(".")[0:-1])}{mimetypes.guess_extension(res.headers["Content-Type"])}'

        uploaded = self.check_upload(filename,url.split("?")[0].split("/")[-1])
        if uploaded is not None:
            
            return uploaded
        upl = json.loads(shopify.GraphQL().execute(
            """
            mutation fileCreate($files: [FileCreateInput!]!) {
                fileCreate(files: $files) {
                    files {
                        preview {
                            image {
                                url 
                            }
                        } 
                        fileStatus
                        fileErrors {
                            code
                            details
                            message
                        }
                        id
                    } 
                } 
            }""",
            {
                'files': {
                    'alt': alt,
                    'contentType': 'IMAGE',
                    'originalSource': url,
                    'filename':filename,
                }
            }
        ))
        
        if not check:
            if len(upl['data']['fileCreate']['files'])>0 and upl['data']['fileCreate']['files'][0]['fileStatus'] == "UPLOADED":
                return {"id":upl['data']['fileCreate']['files'][0]['id']}
            
        
        if upl['data']['fileCreate']['files'] is not None and len(upl['data']['fileCreate']['files'])>0 and upl['data']['fileCreate']['files'][0]['fileStatus'] == "UPLOADED":
            attempts = 0
            while attempts<30:
                
                uploaded = self.check_upload(filename,url.split("?")[0].split("/")[-1])
                if uploaded is None:
                    time.sleep(0.25)
                    attempts = attempts+1
                else:
                    return uploaded
        else:
            return url
        