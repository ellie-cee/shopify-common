import csv
import traceback
import shopify
import requests
import json
from jmespath import search as jpath

class ShopifyETL:
    def __init__(self,domain,token,version="2024-04"):
        self.token = token
        self.domain = domain
        self.version = version
        
    def setSession(self):
        session = shopify.Session(f"{self.domain}.myshopify.com/admin",self.version,self.token)
        shopify.ShopifyResource.activate_session(session)
    def read_into(self,csv_file,fieldname=None):
        input = csv.DictReader(open(csv_file),delimiter=',',quotechar='"')
        ret = {}
        if fieldname is None:
            fieldname = input.fieldnames[0]
        for row in input:
            ret[row[fieldname]] = row
            
        return ret
    def records(self,api_object):
        ret = []
        proceed = True
        while proceed:
            for row in api_object:
                ret.append(row)
                if api_object.has_next_page():
                    api_object = api_object.next_page()
                else:
                    proceed = False
        return ret
    def graphql_records(self,query,variables={},queryRoot="data"):
        variables["cursor"] = None
        ret = []
        proceed = True
        recordSet = json.loads(shopify.GraphQL().execute(query,variables))
        try:
            while proceed:
                for record in jpath(f"data.{queryRoot}.nodes || []",recordSet):
                    ret.append(record)
                pageInfo = jpath(f"data.{queryRoot}.pageInfo",recordSet)
                if pageInfo and pageInfo.get("hasNextPage",False):
                    variables["cursor"] = pageInfo.get("endCursor")
                else:
                    proceed = False
        except:
            print(json.dumps(recordSet,indent=2))
            traceback.print_exc()
            
            return []
        return ret
        

class Article(ShopifyETL):
    def upset_metafield(self,blog,article,namespace,key,type,value):
        return requests.put(
            f"https://{self.domain}.myshopify.com/admin/api/{self.version}/blogs/{blog}/articles/{article}.json",
            headers={
                "Content-type":"application/json",
                "X-Shopify-Access-Token":self.token
            },
            json={
                "article":{
                    "id":article,
                    "metafields":[{"key":key,"value":value,"type":type,"namespace":namespace}]
                }    
            }
        ).json()
    def getByHandle(self,handle):
        self.setSession()
        assets = shopify.Article.find(handle=handle)
        if len(assets)>0:
            return next(assets)
        return None
class Page(ShopifyETL):
    def upset_metafield(self,id,namespace,key,type,value):
        return requests.put(
            f"https://{self.domain}.myshopify.com/admin/api/{self.version}/pages/{id}.json",
            headers={
                "Content-type":"application/json",
                "X-Shopify-Access-Token":self.token
            },
            json={
                "page":{
                    "id":id,
                    "metafields":[{"key":key,"value":value,"type":type,"namespace":namespace}]
                }    
            }
        ).json()
    def getByHandle(self,handle):
        self.setSession()
        assets = shopify.Article.find(handle=handle)
        if len(assets)>0:
            return next(assets)
        return None
    
class Metafields(ShopifyETL):
    def upset(self,payload):
        self.setSession()
        ret =  json.loads(shopify.GraphQL().execute(
            """
            mutation MetafieldsSet($metafields: [MetafieldsSetInput!]!) {
                metafieldsSet(metafields: $metafields) {
                    metafields {
                        key
                        namespace
                        value
                        createdAt
                        updatedAt
                    }
                    userErrors {
                        field
                        message
                        code
                    }
                }
            }""",
            payload
        ))
        return ret
        