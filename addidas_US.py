# difficulty{
#  "Adidas US": {
#    "blocking_difficulty_level": "Moderate Blocking",
#    "setup_difficulty_level": "Easy Setup",
#    "extra_services": ["curl_cffi"],
#    "proxies": ["LOCAL_PROXY_SERVER"]
#  }
# }difficulty

"""
Adidas
"""
import json
import math
import sys
from typing import Dict, Union, List
from urllib.parse import urlparse, urlencode, urlunparse, parse_qsl
import re
from scrapy.http import Response
from engine.spiders.retailers.base import Retailer
import os


class AdidasUS(Retailer):
    """adidas-us"""
    name = "adidas-us"
    page_size = 48
    pdp_url = True
    proxy_enabled = True
    formatter = {
        "pdp_url": "https://www.adidas.com{}",
        'api_url': 'https://www.adidas.com/api/products/{}?sitePath=us'
    }
    handle_httpstatus_list = [403]
    paths = {
        "product": {
            "product_details": {
                "sku": {
                    "paths": [
                        ""
                    ],
                    "default": ""
                },
                "color": {
                    "paths": [
                        "attribute_list.color"
                    ],
                    "default": ""
                },
                "product_name": {
                    "paths": [
                        "name"
                    ],
                    "default": ""
                },
                "brand": {
                    "paths": [
                        ""
                    ],
                    "default": "Adidas"
                },
                "breadcrumbs": {
                    "paths": ["breadcrumb_list.*.text"],
                    "default": []
                },
                "description": {
                    "paths": [
                        "product_description.subtitle",
                        "product_description.text"
                    ],
                    "default": ""
                },
                "feature_list": {
                    "paths": [
                        "product_description.usps.*"
                    ],
                    "default": []
                },
                "offer_list": {
                    "paths": [],
                    "default": []
                },
                "feature_image": {
                    "paths": [
                    ],
                    "default": ""
                },
                "pdp_images": {
                    "paths": [
                        "view_list..image_url"
                    ],
                    "default": []
                },
                "pdp_url": {
                    "paths": [],
                    "default": ""
                },
                "mrp": {
                    "paths": [
                        "pricing_information.standard_price"
                    ],
                    "default": 0
                },
                "selling_price": {
                    "paths": [
                        "pricing_information.currentPrice"
                    ],
                    "default": 0
                },
                "rating": {
                    "paths": [],
                    "default": None
                },
                "rating_count": {
                    "paths": [],
                    "default": None
                },
                "review_count": {
                    "paths": [

                    ],
                    "default": None
                },
                "style_attributes": {
                    "paths": [
                        "product_description.product_highlights.*"
                    ],
                    "default": {}
                },
                "variants": {
                    "paths": [
                        ""
                    ],
                    "default": []
                },
                "related_products": {
                    "paths": [],
                    "default": []
                },
                "relation": {
                    "paths": [],
                    "default": ""
                }
            },
            "offer_list": {
                "code": {
                    "paths": [],
                    "default": ""
                },
                "title": {
                    "paths": [
                        "title"
                    ],
                    "default": ""
                },
                "description": {
                    "paths": [
                        ""
                    ],
                    "default": ""
                },
                "savings": {
                    "paths": [],
                    "default": ""
                }
            },
            "variants": {
                "size": {
                    "paths": [
                        ""
                    ],
                    "default": ""
                },
                "stock": {
                    "paths": [
                        ""
                    ],
                    "default": None
                },
                "available": {
                    "paths": [
                        ""
                    ],
                    "default": None
                },
                "color": {
                    "paths": [],
                    "default": None
                },
                "variant_sku": {
                    "paths": [
                        "skuId"
                    ],
                    "default": ""
                }
            },
            "style_attributes": {
                "style_key": {
                    "paths": [
                        "headline"
                    ],
                    "default": ""
                },
                "style_value": {
                    "paths": [
                        "copy"
                    ],
                    "default": ""
                }
            },
            "related_products": {
                "pdp_url": {
                    "paths": [],
                    "default": []
                },
                "sku": {
                    "paths": [],
                    "default": ""
                },
                "color": {
                    "paths": [],
                    "default": ""
                }
            }
        },
        "category": {
            "product_collection": [
                "$.raw.itemList.items.*"
            ],
            "product_details": {
                "sku": {
                    "paths": [
                        "productId"
                    ],
                    "default": ""
                },
                "rank": {
                    "paths": [],
                    "default": ""
                },
                "pdp_url": {
                    "paths": [
                        "link"
                    ],
                    "default": ""
                }
            },
            "pagination": {
                "next_page": "",
                "total_pages": "",
                "total_count": "$..totalCount",
                "page_size": ""
            }
        }
    }
    custom_settings = {
        "DOWNLOADER_MIDDLEWARES": {
            'engine.middlewares.proxy.PrivateProxyMiddleware': 450,
            'engine.middlewares.curl_cffi.CurlCffiMiddleware':451
        }
    }

    @staticmethod
    def get_proxy() -> str:
        return os.getenv("LOCAL_PROXY_SERVER")

    def get_product_url(self, msg_dict) -> str:
        """get_product_url"""
        forced_url = msg_dict.get("forced_url", None)
        if forced_url:
            return forced_url
        url = msg_dict.get("pdp_url") or msg_dict.get("url") or ""
        url_parts = list(urlparse(url))
        query = dict(parse_qsl(url_parts[4]))
        url_parts[4] = urlencode(query)
        msg_dict["extra"] = {"curl_cffi":True, "impersonate": "chrome131"}
        return self.formatter.get("api_url", "").format(msg_dict["sku"])


    @staticmethod
    def get_headers(msg_dict) -> Dict:
        if msg_dict.get('crawl_type') == "category":
            headers = {
                'accept': '*/*',
                'accept-language': 'en-US,en;q=0.9',
                'sec-fetch-dest': 'empty',
                'sec-fetch-mode': 'cors',
                'sec-fetch-site': 'same-origin',
                'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36',
            }
            return headers

    def get_category_url(self, msg_dict) -> str:
        if msg_dict.get('forced_url'):
            return msg_dict['forced_url']
        url_parts = list(urlparse(msg_dict.get("url")))
        query = {
            "sort": "top-sellers",
            "sitePath": 'us',
            "start": 0,
            "query": re.findall(r'us\/(.*)', url_parts[2])[0]
        }
        url_parts[4] = urlencode(query)
        url_parts[2] = "/api/plp/content-engine"
        return urlunparse(url_parts)

    def product_parser(self, response: Response, **kwargs) -> Union[Dict, None]:
        try:
            msg_dict = response.meta.get("msg_dict", {})
            message = response.meta.get("message", None)
            first_hit = response.meta.get("first_hit", True)
            extra = msg_dict.get("extra")
            if first_hit:
                product_json = json.loads(response.text)
                pd = self.populate_product_j_path(product_json, {
                    "pdp_url": msg_dict.get("pdp_url"),
                    "sku": msg_dict.get("sku"),
                }, self.paths.get("product", {}))
                pd['breadcrumbs'].insert(0, 'Home')
                pd['pdp_images'] = [img.replace('w_600', 'w_auto') for img in pd['pdp_images']]
                pd['feature_image'] = pd['pdp_images'][0]
                if self.j_extract(product_json, "product_description..care_instructions.*.description", []):
                    pd['style_attributes']['care_instructions'] = self.j_extract(product_json, "product_description..care_instructions.*.description")
                if self.j_extract(product_json, "product_description..extra_care_instructions.*", []):
                    pd['style_attributes']['extra_care_instructions'] = self.j_extract(product_json, "product_description..extra_care_instructions.*")

                model_number = self.j_extract_first(product_json, "model_number")
                api_url = "https://www.adidas.com/api/products/{}/availability?sitePath=us".format(msg_dict.get("sku"))
                new_msg_dict = msg_dict.copy()
                new_msg_dict["current_retry"] = 0
                new_msg_dict["forced_url"] = api_url
                new_msg_dict["first_hit"] = False
                new_msg_dict["extra"]["pd"]= pd
                new_msg_dict["extra"]["model_num"]= model_number
                self.push_message_in_queue(message_body=new_msg_dict)
                self.delete_sqs_message(message)
                return
            else:
                pd = extra.get("pd")
                size_json = json.loads(response.text)
                size_json = self.j_extract(size_json, "$..variation_list.*")
                variants = [
                    {
                        "stock": i.get("availability"),
                        "size": "ONE SIZE" if i.get("size") in ["OSFA"] else i.get("size"),
                        "color": pd["color"],
                        "variant_sku": i.get("sku"),
                        "available": True if i.get("availability") > 0 else False
                    } for i in size_json or []
                ]
                pd["variants"] = variants
                review_api = f'https://www.adidas.com/api/models/{extra.get("model_num")}/ratings?bazaarVoiceLocale=en_US&offset=0&includeLocales=en%2A'
                review_api = [{"url": review_api}]
                headers = {
                    'accept': '*/*',
                    'accept-language': 'en-US,en;q=0.9',
                    'sec-fetch-dest': 'empty',
                    'sec-fetch-mode': 'cors',
                    'sec-fetch-site': 'same-origin',
                    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36',
                }
                review_json = self.make_async_requests(review_api, headers=headers, proxies=self.get_proxy(),method="GET")
                try:
                    review_json = json.loads(review_json[0])
                except:
                    self.logger.warning("Retrying <%(url)s>Blocking", {"url": response.url})
                    self.push_message_in_queue_for_retry(message=message)
                    return
                rating = self.j_extract_first(review_json, "overallRating")
                review_count = self.j_extract_first(review_json, "reviewCount")
                pd['rating'] = float(rating) if rating else None
                pd['review_count'] = int(review_count) if review_count else None
                product_item = self.populate_product_item(response, details=pd)
                yield product_item
                self.delete_sqs_message(message)
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            line_number = exc_traceback.tb_lineno
            self.logger.info(f"Error {e} on line No. {line_number}")

    def category_parser(self, response: Response, **kwargs) -> Union[Dict, None]:
        try:
            msg_dict: dict = response.meta.get("msg_dict", {})
            message = response.meta.get("message", None)
            current_page = response.meta.get("current_page", 1)
            first_hit = response.meta.get("first_hit", True)
            rank = response.meta.get("start_rank", 0)
            if response.status in [403]:
                self.logger.warning("Retrying <%(url)s>Blocking", {"url": response.url})
                self.push_message_in_queue_for_retry(message=message)
                return
            category_json = json.loads(response.text)
            if first_hit:
                total_count = self.j_extract_first(category_json, "$.raw.itemList.count")
                total_pages = int(math.ceil(total_count / self.page_size))
                url_parts = list(urlparse(response.url))
                query = dict(parse_qsl(url_parts[4]))
                msg_dict["total_count"] = total_count
                msg_dict["pages_to_crawl"] = total_pages
                msg_dict["page_size"] = self.page_size
                for page in range(current_page, total_pages):
                    query["start"] = page * self.page_size
                    url_parts[4] = urlencode(query)
                    next_url = urlunparse(url_parts)
                    new_msg_dict = msg_dict.copy()
                    new_msg_dict["forced_url"] = next_url
                    new_msg_dict["start_rank"] = page * self.page_size
                    new_msg_dict["current_retry"] = 0
                    new_msg_dict["current_page"] = page + 1
                    new_msg_dict["first_hit"] = False
                    self.push_next_page_to_crawl(new_msg_dict=new_msg_dict)
            msg_dict.pop("forced_url", None)
            product_collection = []
            products = self.get_product_collection_j_path(category_json, crawl_type="category")
            for p in products:
                rank += 1
                pd = self.populate_product_details_j_path(p, {
                    "rank": rank
                }, self.paths.get("category").get("product_details", {}))
                self.push_ranked_product_to_crawl(pd=pd, msg_dict=msg_dict)
                pd.pop("pdp_url", None)
                product_collection.append(pd)
            yield self.populate_category_item(response, msg_dict, products=product_collection)
            self.delete_sqs_message(message)
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            line_number = exc_traceback.tb_lineno
            self.logger.info(f"Error {e} on line No. {line_number}")
