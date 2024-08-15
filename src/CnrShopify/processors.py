import csv
import re
import subprocess
import traceback
import requests
import xmltodict
import json
import os
import sys
from jmespath import search as jpath
from bs4 import BeautifulSoup, Comment, NavigableString
from slugify import slugify
import shopify
from shopify_uploader import ShopifyUploader
import paramiko
import mimetypes
import base64
import shopify
from urllib.parse import urlparse,parse_qs
from glob import glob
from PIL import Image,ImageOps
import time
import argparse
import pathlib
import functools


class Initializer:
    @staticmethod
    def config(self):
        open("config.json","w").write(
            json.dumps(
                {
                    "token":"",
                    "key":"",
                    "secret":"",
                    "storefront_token":"",
                    "site":"sitename",
                    "source_url":"https://example.com",
                    "dest_url":"https://store.example.com",
                    "useSFTP":False,
                    "sftpHost":"sftp.example.com",
                    "sftpUser":"user",
                    "sftpPassword":"password!3$",
                    "sftpDir":"/files/dir",
                    "hostUrl":"https://sftp.example.com/dir",
                    "apiVersion":"2024-07"
                },
                indent=1
            )
        )
        
class ArticleProcessor:
    def __init__(self,inputFile=None):
        self.inputFile = inputFile
        if self.inputFile is None:
            print("An input file is required!")
            sys.exit()
        else:
            self.input = json.load(open(self.inputFile))
        
        self.transport = None
        
        self.reprocess = False
        
        self.config_obj = json.load(open("config.json"))
        session = shopify.Session(f"{self.config('site')}.myshopify.com/admin",self.config("apiVersion"),self.config("token"))
        shopify.ShopifyResource.activate_session(session)
        self.uploader = ShopifyUploader(self.config("token"),self.config("site"))
                
        if (self.config("useSFTP")):
            self.connectSFTP()
        
        self.blogs = blogs = [{"title":blog.title,"id":blog.id,"handle":blog.handle} for blog in shopify.Blog.find()]
        self.blog_titles = list(map(lambda blog: blog["title"],self.blogs))
        self.categories = {}
        self.handles = {}
        self.testHandles = []
        self.redirects = {}
        self.main_categories = {}
        self.nav = {}
        self.nav_parents = {}
        
    def setTestHandles(self,handles):
        if handles is not None:
            self.testHandles = handles
    def stripTags(self):
        return ["style","script"]
        
    def config(self,key,default=None):
        return self.config_obj.get(key,default)
        
    def connectSFTP(self):
        transport = paramiko.Transport(self.config("sftpHost"),22)
        transport.connect(
            username=self.config("sftpUser"),
            password=self.config("sftpPassword"),
        )
        self.sftp =  paramiko.SFTPClient.from_transport(transport)
    def update_metafield(self,articleId,blogId,namespace,key,value,type="single_line_text_field"):
       
        return requests.put(
            f"https://{self.config('site')}.myshopify.com/admin/api/2024-04/blogs/{blogId}/articles/{articleId}.json",
            headers={
                "Content-type":"application/json",
                "X-Shopify-Access-Token":self.config("token")
            },
            json={
                "article":{
                    "id":articleId,
                    "metafields":[{"key":key,"value":value,"type":type,"namespace":namespace}]
                }    
            }
        ).json()
    def uploadSFTP(self,filename):
        if not self.sftp:
            return
        self.sftp.put(
                f"download/{filename}",
                f"{self.config('sftpDir')}/{filename}"
            )
        
    def fetch(self,url):
        attempts = 0
        retry = True
        while attempts<10 and retry:
            try:
                res =  requests.get(
                    url,
                    headers={
                        "Referer":self.config("blog_url"),
                        "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
                    }
                )
                if res.status_code == 404:
                    return None
                return res
            except:
                attempts = attempts+1
                
    def filenameFor(self,path):
        newPath = None
        if "wp-content/uploads" in path:
            newPath = "-".join(path.split("/")[-3:])
        else:
            newPath = path.split("/")[-1]
        parsed = pathlib.Path(newPath)
        if parsed.suffix and parsed.suffix!="":
            return f"{slugify(parsed.stem)}{parsed.suffix}".replace("jpeg","jpg")
        else:
            return slugify(parsed.stem).replace("jpeg","jpg")
        return newPath
    
    def isImage(self,path):
        return subprocess.check_output(["file","--mime-type",path]).decode("utf-8").split(":")[-1].strip().startswith("image")
        
    def download(self,url,backupFilename="image"):
        parsedUrl = urlparse(url)
        
        filename = self.filenameFor(url)
        
        fileContents = None
        
        if not pathlib.Path(filename).suffix.lower() in [".jpg",".jpeg",".png",".gif",".webp"]:
        #if not re.search(r'\.(jpg|jpeg|png|gif|webp)$',filename.lower()):
            existing = glob(f"download/{backupFilename}.*")
            if len(existing):
                filename = existing[-1].split("/")[-1]
            
            
                
            else:
                res = requests.head(url)
                if res.status_code==405:
                    res = requests.get(
                        url,
                        headers={
                            "Referer":self.config("blog_url"),
                            "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
                        }
                    )
                    if res.status_code==200:
                        fileContents = res.content
                    else:
                        fileContents = None             
                filename = f"{backupFilename}{mimetypes.guess_extension(res.headers['Content-Type'])}"
                
                print(f"downloading {url} as {filename}",file=sys.stderr)
        
        if not os.path.isfile(f"download/{filename}") or not self.isImage(f"download/{filename}"    ):
            print(f"downloading {url}",file=sys.stderr)
            if fileContents is None:
                res = self.fetch(url)
                fileContents = res.content if res else None
            if fileContents is not None:   
                open(f"download/{filename}","wb").write(fileContents)
                try:
                    self.fit20MP(filename)
                except:
                    print(f"invalid image {filename}",file=sys.stderr)
                    traceback.print_exc()
                    return None
            else:
                return None
        else:
            try:
                self.fit20MP(filename)
            except:
                print(f"invalid image {filename}",file=sys.stderr)
                traceback.print_exc()
                return None
                print(f"invalid image {filename}",file=sys.stderr)
                
        return filename
    
    def fit20MP(self,filename):
        image = Image.open(f"download/{filename}")
        width,height = image.size
        if (width*height)>(5600*3740):
            reduce_factor = 0
            if (width>height):
                reduce_factor = 1-(5600/width)
            else:
                reduce_factor = 1-(3740/height)
                    
            print(f"Image {filename} is way to big!",file=sys.stderr)
            print(f"{width}x{height} to {int(width*reduce_factor)}x{int(height*reduce_factor)} {reduce_factor}")
            ImageOps.cover(image,(int(width*reduce_factor),int(height*reduce_factor))).save(f"download/{filename}")
            
        self.uploadSFTP(filename)
        
            
            
        
    def getBlog(self,category):
        return next((blog for blog in self.blogs if blog.get("title")==category),None)
    
    def main_category(self,post):
        
        for cat in ["Real Health","Fitness","Food","Recipes"]:
            if cat in post.get("categories"):
                parent = self.getBlog(cat)
                if parent is not None:
                    return parent
        for cat in post.get("categories"):
            if cat in self.nav_parents:
                parent =  self.getBlog(self.nav_parents[cat])
                if parent is not None:
                    return parent
        return self.getBlog("Uncategorized")
    def youtubeEmbed(self,soup,url):
        parsed = urlparse(url)
        qs = parse_qs(parsed.query)

        final_url = None
        if "/watch" in parsed.path or "/embed" in parsed.path:
            if "v" in qs:
                final_url = f"https://{parsed.hostname}/embed/{qs.get('v') if type(qs.get('v')) is not list else qs.get('v')[0] }"
            else:
                final_url = url
        elif parsed.hostname == "youtu.be":
            final_url = f"https://www.youtube.com/embed/{url.split('/')[-1]}"
        else:
            final_url = url
            
            
        iframe = BeautifulSoup.new_tag(
            soup,
            "iframe",
            attrs={
                "title":"YouTube Video Player",
                "src":final_url,
                "width":"560",
                "height":"315",
                "frameBorder":"0",
                "allowFullscreen":"allowFullscreen",
                "class":"blog-youtube-embed"
            }
        )
            
        return iframe
    
    def htmlPreProcess(self,soup):
        return soup
    def htmlPostProcess(self,soup):
        return soup
    def stripAttrs(self):
        return []
    
    def getPostByHandle(self,handle,blog=None):
        for article in shopify.Article.find(handle=handle):
            if article.handle==handle:
                if blog is not None:
                    if blog==article.blog_id:
                        return article
                else:
                    return article

        return None
    def finalizeTags(self,post):
        return ",".join(post.get("all_tags",[]))
    
    
    def process_article(self,post):
        imageCount = 1
        post["redirects"] = {}
        post["links"] = []
        post["images"] = []
        blog = self.main_category(post)
        post["blog"] = blog
        
        print(f"Processing {post.get('handle')}")
        articleImage = None
        if post.get("articleImage") is not None:
            articleImage = self.download(post.get("articleImage"))
            
        soup = BeautifulSoup(post.get("html"),'html.parser')
        soup = self.htmlPreProcess(soup)
        
        for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
            comment.extract()
        for noscript in soup.find_all("noscript"):
            noscript.decompose()
        for tag in soup.find_all():
            if tag.name in self.stripTags():
                tag.decompose()
                continue
            elif isinstance(tag,NavigableString):
                content = str(tag)
                if content.startswith("[embed]"):
                    tag.replaceWith(self.youtubeEmbed(soup,content[7:content.index("[/embed]")]))
                elif content.startswith("https://youtu.be"):
                    tag.replaceWith(self.youtubeEmbed(soup,content))
            elif    tag.text.strip().startswith("https://www.youtube.com"):
                 tag.replaceWith(self.youtubeEmbed(soup,tag.text.strip()))
            if hasattr(tag,"attrs") and tag.attrs is not None:
                tag.attrs = {key:value for key,value in tag.attrs.items() if key not in self.stripAttrs()}
                
        for embed in soup.find_all("figure",class_="wp-block-embed-youtube"):
            inner = embed.find("div",class_="wp-block-embed__wrapper")
            if inner:
                youtube = inner.text.strip().replace("/watch","/embed")
                inner.replaceWith(self.youtubeEmbed(soup,youtube))
                
            
        for link in soup.find_all("a"):
            handle = ""
            link.attrs = {key:value for key,value in link.attrs.items() if key not in ["data-href"]}
            if "href" in link.attrs:
                if link["href"].startswith(self.config("dest_url")):
                    link["href"] = link["href"].replace(self.config("dest_url"),"")
                    # store link, leave it alone
                    continue
                elif link["href"].startswith(self.config("source_url")):
                    url = link["href"].replace(self.config("source_url"),"")
                    try:
                        handle = list(filter(lambda x:x!="",url.split("?")[0].split("#")[0].split("/")))[-1]
                    except:
                        link["href"] = "/"
                        
                    if handle in self.handles:
                        post["redirects"][url] = self.handles[handle]
                        self.redirects[url] = self.handles[handle]
                        link["href"] = post["redirects"][url]
                    post["links"].append(url)
        articleImageCount = 0            
        for img in soup.find_all("img"):
            articleImageCount = articleImageCount + 1
            uploadToShopify = None
            defaultFilename=f"{post.get('handle')}-{articleImageCount}"
            if "data-lazy-src" in img.attrs:
                filename = self.download(
                    img.attrs.get("data-lazy-src"),
                    backupFilename=defaultFilename
                )
                del img.attrs["data-lazy-src"]
                if filename:
                    uploadToShopify = f"{self.config('hostUrl')}/{filename}"
            elif "src" in img.attrs:
                if not img.attrs.get("src").startswith("data/") and "cdn.shopify.com" not in img.attrs.get("src"):
                    filename = self.download(
                        img.attrs.get("src"),
                        backupFilename=defaultFilename
                    )
                    if filename:
                        uploadToShopify = f"{self.config('hostUrl')}/{filename}"
            if filename is None:
                print(f"{defaultFilename} is invalid.",file=sys.stderr)
                img.decompose()
            elif uploadToShopify is not None:
                print(f"Uploading {filename} to Shopify")
                del img["src"]
                
                img.attrs["data-widths"] = "[180, 360, 540, 720, 900, 1080]"
                img["id"] = defaultFilename
                img.attrs["data-sizes"] = "auto"
                uploaded = self.uploader.upload_image(uploadToShopify)
                img.attrs["data-src"] = uploaded["url"] if uploaded is not None  and "url" in uploaded else "" 
                
                img.attrs["class"] = "lazyload lazyload-fade"
                post["images"].append(filename)
                img.attrs["data-original"] = f"{img.attrs['data-src'].split('?')[0]}?width=500"
                img.attrs["src"] = f"{img.attrs['data-src'].split('?')[0]}?width=500"
            else:
                post["images"].append(filename) 
            
        post["tags"].append("_imported")
        post["tags"].append(f"_imported-as-{post['status']}")
        
        post["all_tags"] = []
        for x in ["tags","categories"]:
            if x in post and type(post[x]) is list:
                post["all_tags"] = post["all_tags"]+post[x]
        post["all_tags"] = list(filter(lambda tag: tag not in self.blog_titles and len(tag)>1,set(post["all_tags"])))
        
        post["html"] = str(self.htmlPostProcess(soup))
        
        postObj = shopify.Article()
        if post.get("shopifyId") is not None:
            try:
                postObj = shopify.Article.get(post["shopifyId"])
            except:
                print(f"error processing {json.dumps(post,indent=1)}")
                sys.exit()
                postObj = shopify.Article()
                
        postObj.title =post["title"]
        postObj.body_html = post["html"]
        if "blog" in post and post["blog"]["id"] is not None:
            postObj.blog_id = post["blog"]["id"]
        
        postObj.handle = post["handle"]
        postObj.summary_html = post["excerpt"] if post["excerpt"] else ""
        postObj.updated_at  = post["published"]
        postObj.attributes["tags"]= self.finalizeTags(post)
        postObj.published = False
        postObj.author=post["author"] if post["author"] is not None else "Dr Livingood"
                
        imageContents = None
        if articleImage is not None and articleImage!="":
            imageContents = open(f"download/{articleImage}","rb").read()
        elif post["images"] is not None and len(post["images"])>0:
            imageContents = open(f"download/{post['images'][0]}","rb").read()
        
                
        if imageContents is not None:
            image = shopify.Image()
            encoded = base64.b64encode(imageContents).decode('ascii')
            image.attachment = encoded
            postObj.image = image
        if postObj.save():
            post["shopifyId"] = postObj.id
            print(f"Created Article {post['handle']}",file=sys.stderr)
            if post["excerpt"] is not None:
                ret = self.update_metafield(
                    postObj.id,
                    postObj.blog_id,
                    "global",
                    "description_tag",
                    post["excerpt"]
                )
        else:
            article = self.getPostByHandle(post["handle"],post["blog"]["id"])
            if article is not None:
                post["shopifyId"] = article.id
                postObj.id = article.id
                if postObj.save():
                    print(f"Updated Article {post['handle']}",file=sys.stderr)
                else:
                    print(f"Could not Update Article {post['handle']}",file=sys.stderr)
            else:
                print(f"Could not Create Article {post['handle']}",file=sys.stderr)
           
        return post
    def run(self):
        
            
           
        
        poasts = self.input.get("poasts")
        if len(self.testHandles)>0:
            poasts = list(filter(lambda post:post["handle"] in self.testHandles,poasts))
        
        for article in poasts:
            if article.get("shopifyId") is not None:
                if not self.reprocess:
                    print(f"skipping {article.get('handle')}: already processed",file=sys.stderr)
                    continue
            if article.get("handle") is None:
                continue
            try:
                self.handles[article.get("handle")] = f"/blogs/{self.main_category(article)['handle']}/{article.get('handle')}"
            except:
                sys.exit()
            if "/?p=" in article.get("url"):
                self.redirects[f"/{article.get('handle')}/"] = self.handles[article.get("handle")]
            else:
                self.redirects[article.get("url").replace(self.config('source_url'),"")] = self.handles[article.get("handle")]
            for cat in article.get("categories"):
                self.categories[cat] = True
            self.main_categories[article.get("category")] = True
            
            retries = 0
            proceed = True
            while proceed and retries<5:
                try:
                    article = self.process_article(article)
                    proceed = False
                except KeyboardInterrupt as e:
                    sys.exit()
                except:
                    traceback.print_exc()
                    retries = retries+1
                    
        
        return self
    def processNav(self,url,root):
        pass
    def getIds(self):
        for post in self.input.get("poasts"):
            print(f"finding id for {post.get('handle')}",file=sys.stderr)
            post["blog"] = self.main_category(post)
            print(f"setting blog to {post['blog']['id']}:{post['blog']['title']}")
            postObj = None
            if post.get("shopifyId") is not None:
                try:
                    postObj = shopify.Article.get(post["shopifyId"])
                    if postObj.id!=post.get("shopifyId"):
                        post["shopifyId"]=postObj.id
                        print(f"setting to {postObj.id}",file=sys.stderr)
                except:
                    postObj = self.getPostByHandle(post.get("handle"))
                    if postObj is not None:
                        post["shopifyId"]=postObj.id
                        print(f"setting to {postObj.id}",file=sys.stderr)
                        
        return self               
            
        
    def write(self,path=None):
        if path is None:
            path = self.inputFile
            
        open(path,"w").write(json.dumps(self.input,indent=1))
        return self
    def writeRedirects(self,path):
        writer = csv.DictWriter(open(path,"w"),delimiter=',',quotechar='"',fieldnames=["URL","TARGET"])
        writer.writeheader()
        for url,target in self.redirects.items():
            writer.writerow({
                "URL":url,
                "TARGET":target
            })

class WordpressImporter:
    def __init__(self,wordpressFile,useCache=True,outputFile=None):
        self.useCache = useCache
        self.input = xmltodict.parse(open(wordpressFile).read().replace("wp:",""))
        self.config_obj = json.load(open("config.json"))
        self.outputFile = outputFile
        self.parsed = {"poasts":[]}
        
        if self.outputFile is not None:
            if pathlib.Path(self.outputFile).exists():
                self.parsed = json.load(open(self.outputFile))
        self.handles = [x.get("handle") for x in self.parsed.get("poasts")]
      
            
                
    def config(self,key,default=None):
        return self.config_obj.get(key,default)
    
    def data(self):
        return self.input
    
    def exists(self,post):
        return post.get("handle")
        
    def run(self):
        for post in filter(lambda x:x["post_type"]=="post",jpath("rss.channel.item",self.input)):
            if post.get("post_name") in self.handles:
                found = False
                index = 0
                for poast in self.parsed["poasts"]:
                    if poast.get("handle")==post.get("post_name"):
                        if poast.get("shopifyId") is None:
                            found = True
                            self.parsed["poasts"][index] = self.postDetails(post)
                            break
                    index = index+1
                if not found:
                    print(f"Skipping {post.get('post_name')}")            
            else:
                self.parsed.get("poasts").append(self.postDetails(post))
        return self
    def cached(self,handle):
        if not self.useCache:
            return None
        if not os.path.isfile(f"/download/{handle}.html"):
            return None
        return open(f"download/{handle}.html").read()
    def cache(self,handle,contents):
        if not self.useCache:
            return
        open(f"download/{handle}.html","w").write("contents")

    def parsed(self):
        return self.parsed
    def write(self,outputFile=None):
        if outputFile is None:
            outputFile = self.outputFile
        
        open(outputFile,"w").write(json.dumps(self.parsed,indent=1))
        return self
    
    def arrayVal(self,array,key):
        array = [array] if type(array) is dict else array
        try:
            ret = list(filter(lambda obj: obj.get(key) is not None,array))
            if len(ret)>0:
                return ret[0]    
        except:
            print(array)
        return None

    def attachment(self,id):
        if id=="" or id is None:
            return None
        
        ret = list(filter(lambda item: item.get("post_type")=="attachment" and item.get("post_id")==id,jpath("rss.channel.item",self.data())))
        if len(ret)>0:
            return ret[0]["attachment_url"]
        return None

    def category(self,id):
        if id=="" or id is None:
            return None
        
        ret = list(filter(lambda item: item.get("term_id")==id,jpath("rss.channel.category",self.data())))
        if len(ret)>0:
            return ret[0]["cat_name"].replace("&amp;","&")
        return None

    def author(self,email):
        if id=="" or id is None:
            return None
        
        ret = list(filter(lambda item: item.get("author_email")==email,jpath("rss.channel.author",self.data())))
        if len(ret)>0:
            return ret[0]["author_display_name"]
        return None
        
    def postMeta(self,post,key):
        meta = [post.get("postmeta")] if type(post.get("postmeta")) is dict else post.get("postmeta")
        ret = list(
            filter(lambda kv:kv.get("meta_key")==key,meta)
        )
        if len(ret)>0:
            return ret[0].get("meta_value")
        return None
    def innerHTML(self,soup):
        return soup.find("div",class_="elementor-widget-theme-post-content").find("div",class_="elementor-widget-container")
        
    def postContent(self,url,handle):
        attempts = 0
        retry = True
        cachedContent = self.cached(handle)
        if cachedContent is not None:
            return cachedContent
        
        while attempts<10 and retry:
            try:
                content = requests.get(
                    url,
                    headers={
                        "Referer":self.config("blog_url"),
                        "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
                    }
                ).content.decode("utf-8")
                soup = BeautifulSoup(content,'html.parser')
                retry = False
                content = str(self.innerHTML(soup))
                self.cache(handle,content)
                return content
            except:
                traceback.print_exc()
                print(f"retrying {url}",file=sys.stderr)
                attempts = attempts+1
            

    def postDetails(self,post):
        categories = post.get("category",[])
        categories = [categories] if type(categories) is dict else categories
        
        try:
            retval = {
                "title":post.get("title"),
                "handle":post.get("post_name"),
                "status":"active" if post.get("status")=="publish" else "draft",
                "url":post.get("link"),
                "articleImage":self.attachment(self.postMeta(post,"_thumbnail_id")),
                "description":self.postMeta(post,"_yoast_wpseo_metadesc"),
                "category":self.category(self.postMeta(post,"_yoast_wpseo_primary_category")),
                "published":post.get("post_date","").split(" ")[0],
                "excerpt":post.get("excerpt:encoded",""),
                "author":self.author(post.get("dc:creator")),
                "wordpress_id":post.get("post_id"),
                "tags":[tag["#text"] for tag in filter(lambda cat:cat["@domain"]=="post_tag",categories)],
                "categories":[tag["#text"] for tag in filter(lambda cat:cat["@domain"]=="category",categories)],
            }
            if retval["status"]=="active":
                print(f"Downloading from: {post.get('link')}",file=sys.stderr)
                retval["html"]=self.postContent(post.get("link"),retval.get("handle"))
                time.sleep(1)
            else:
                retval["html"] = post.get("content:encoded")
            return retval
        except Exception as e:
            traceback.print_exc()
            sys.exit()