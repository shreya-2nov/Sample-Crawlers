"""
Amazon
"""
import html
import json
import os
import re
import traceback
import uuid
import random
from datetime import datetime
from urllib.parse import parse_qsl, urlparse, urlencode, urlunparse, parse_qs

import sys
from typing import Dict, Union, List
import requests
from jsonpath import jsonpath
from scrapy import Selector
from scrapy.http import Response

from engine.spiders.retailers.base import Retailer


class Amazon(Retailer):
    """
    Amazon

    """

    name = "amazon"
    proxy_enabled = True
    handle_httpstatus_list = [503]
    pdp_url = True
    formatter = {"pdp_url": ""}
    paths = {
        "product": {
            "product_details": {
                "sku": {"paths": [], "default": ""},
                "color": {"paths": [
                    "//div[contains(@id,'variation') and contains(@id,'color')]//span[@class='selection']/text()",
                    "//div[contains(@class,'inline-twister-scroller-color_name')]//span[contains(@id,'color_name') and contains(@class,'a-button-selected')]//input/@aria-label",
                    "//span[@class='a-size-base a-color-secondary' and contains(text(),'Color')]/following-sibling::span//text()",
                    "//span[@class='a-size-base a-color-secondary' and contains(text(), 'Colour')]/following-sibling::span/text()",
                    "//span[@class='a-size-base a-color-secondary inline-twister-dim-title' and contains(text(),'Color')]/following-sibling::span//text()",
                    "//span[@class='a-size-base a-color-secondary inline-twister-dim-title' and contains(text(),'Colour')]/following-sibling::span//text()",
                    "//tr[contains(@class,'color')]//text()[2]"
                ], "default": ""},
                "product_name": {"paths": [
                    "//span[@id='productTitle']/text()"
                ], "default": ""},
                "brand": {"paths": [
                    "//div[@data-feature-name='bylineInfo']//a[@id='bylineInfo']/text()",
                    "//div[@id='amznStoresBylineLogoTextContainer']/a[contains(@class, 'a-text-bold')]/text()"
                ], "default": ""},
                "breadcrumbs": {
                    "paths": [
                        "//div[contains(@id,'wayfinding-breadcrumbs')]//ul/li/span//a//text()"
                    ],
                    "default": [],
                },
                "description": {"paths": [
                    "//div[@id='productDescription']/p//text()",
                    "//div[contains(@id, 'important-info')]//text()"
                ], "default": ""},
                "feature_list": {"paths": [
                    "//div[@id='feature-bullets']//li//text()",
                    "//span[contains(@class,'a-list-item a-size-base')]//text()"
                ], "default": []},
                "offer_list": {"paths": [], "default": []},
                "feature_image": {"paths": [], "default": ""},
                "pdp_images": {"paths": [], "default": []},
                "pdp_url": {"paths": [], "default": ""},
                "mrp": {"paths": [
                    "//span[@id='priceblock_saleprice']//text()",
                    "//span[@id='priceblock_ourprice']//text()",
                    "//span[@class='priceBlockStrikePriceString a-text-strike']//text()",
                    "//span[@id='priceblock_dealprice']//text()",
                    "//span[@id='_price']//span[contains(@class,'normal-price')]//text()",
                    "//span[@id='_price']//span[contains(@class,'price-strikethrough')]//text()",
                    "//span[@id='atfRedesign_strikeThroughPrice']//span[contains(@class,'restOfPrice a-text-strike')]//text()",
                    "//div[@id='corePriceDisplay_desktop_feature_div']//span[contains(@class, 'priceToPay')]//span[@class='a-offscreen']//text()",
                    "//div[contains(@id,'apex_desktop_new') and not (contains(@style, 'none'))]//span[contains(@class, 'basisPrice')]//span[@class='a-offscreen']//text()",
                    "//div[@id='corePriceDisplay_desktop_feature_div']//span[@data-a-color='secondary']//span[@class='a-offscreen']//text()",
                    "//div[@id='corePrice_desktop']//span[@data-a-color='secondary']//span[@class='a-offscreen']//text()",
                    "//div[@id='corePrice_desktop']//span[contains(@class, 'apexPriceToPay')]//span[@class='a-offscreen']//text()",
                    "//div[contains(@id,'corePriceDisplay')]//span[contains(@class, 'PriceToPay')]//span[@class='a-offscreen']//text()",
                    "//div[contains(@id,'corePriceDisplay')]//span[contains(@class, 'basisPrice')]//span[@class='a-offscreen']//text()",
                    "//span[contains(@class, 'riceToPay')]//span[@class='a-offscreen']/text()",
                    "//div[contains(@id,'apex_desktop_new') and not (contains(@style, 'none'))]//span[@data-a-color='secondary']//span[@class='a-offscreen']//text()"
                ], "default": 0},
                "selling_price": {"paths": [
                    "//span[@id='priceblock_saleprice']//text()",
                    "//span[@id='priceblock_ourprice']//text()",
                    "//span[@class='priceBlockStrikePriceString a-text-strike']//text()",
                    "//span[@id='priceblock_dealprice']//text()",
                    "//span[@id='_price']//span[contains(@class,'normal-price')]//text()",
                    "//span[@id='_price']//span[contains(@class,'price-strikethrough')]//text()",
                    "//span[@id='atfRedesign_strikeThroughPrice']//span[contains(@class,'restOfPrice a-text-strike')]//text()",
                    "//div[@id='corePriceDisplay_desktop_feature_div']//span[contains(@class, 'priceToPay')]//span[@class='a-offscreen']//text()",
                    "//div[@id='corePrice_desktop']//span[contains(@class, 'apexPriceToPay')]//span[@class='a-offscreen']//text()",
                    "//div[contains(@id,'corePriceDisplay')]//span[contains(@class, 'PriceToPay')]//span[@class='a-offscreen']//text()",
                    "//div[contains(@id,'apex_desktop_new') and not (contains(@style, 'none'))]//span[contains(@class, 'basisPrice')]//span[@class='a-offscreen']//text()",
                    "//div[contains(@id,'corePriceDisplay')]//span[contains(@class, 'basisPrice')]//span[@class='a-offscreen']//text()",
                    "//span[contains(@class, 'riceToPay')]//span[@class='a-offscreen']/text()"
                ], "default": 0},
                "rating": {"paths": [], "default": None},
                "rating_count": {"paths": [], "default": None},
                "review_count": {"paths": [], "default": None},
                "style_attributes": {"paths": [], "default": {}},
                "variants": {"paths": [], "default": []},
                "related_products": {"paths": [], "default": []},
                "relation": {"paths": [], "default": ""},
            },
            "offer_list": {
                "code": {"paths": [], "default": ""},
                "title": {"paths": [], "default": ""},
                "description": {"paths": [], "default": ""},
                "savings": {"paths": [], "default": ""},
            },
            "variants": {
                "size": {"paths": [], "default": ""},
                "stock": {"paths": [], "default": None},
                "available": {"paths": [], "default": None},
                "color": {"paths": [], "default": None},
                "variant_sku": {"paths": [], "default": ""},
            },
            "style_attributes": {
                "style_key": {"paths": [], "default": ""},
                "style_value": {"paths": [], "default": ""},
            },
            "related_products": {
                "pdp_url": {"paths": [], "default": []},
                "sku": {"paths": [], "default": ""},
                "color": {"paths": [], "default": ""},
            },
        },
        "category": {
            "product_collection": [
                "//*[@id='mainResults']/ul/li",
                "//div[contains(@class, 'result-list') and contains(@class, 'main-slot')]/div[@data-uuid]"
            ],
            "product_details": {
                "sku": {"paths": ["./@data-asin"], "default": ""},
                "rank": {"paths": [], "default": ""},
                "pdp_url": {"paths": [".//a[@class='a-link-normal s-no-outline']/@href",
                                      ".//a[contains(@class, 'a-link-normal s-faceout-link a-text-normal')]/@href"],
                            "default": ""}
            },
            "pagination": {"next_page": "", "total_pages": "", "total_count": "", "page_size": ""},
        },
    }
    user_pass = f"{os.getenv('PRIVATE_PROXY_USERNAME')}:{os.getenv('PRIVATE_PROXY_PASSWORD').replace('@', '%40')}"
    proxy = os.getenv("LOCAL_PROXY_SERVER").replace("http://", "")
    proxies = {"http://": f"http://{user_pass}@{proxy}",
               "https://": f"http://{user_pass}@{proxy}"}
    custom_settings = {
        "DOWNLOADER_MIDDLEWARES": {
            'engine.middlewares.captcha.AmazonCaptchaMiddleware': 450,
        }
    }

    def __init__(self):
        super().__init__(__name__)

    def send_url_to_webhook(self, url, reason):
        webhook_url = "https://chat.googleapis.com/v1/spaces/AAAAUVnzxlg/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=7jUQz77m4pAxfLCcKCLRq_Wz5pkBrs5-NGqbGADMVD4"
        app_message = {
            "text": f"product_url:- {url}\n"
                    f"{reason}",
        }
        message_headers = {"Content-Type": "application/json; charset=UTF-8"}
        requests.request(
            url=webhook_url,
            method="POST",
            headers=message_headers,
            data=json.dumps(app_message),
        )

    def product_parser(self, response: Response, **kwargs) -> Union[Dict, None]:
        msg_dict: dict = response.meta.get("msg_dict", {})
        try:
            message = response.meta.get("message", None)
            extra = response.meta.get("extra", True)
            first_hit = response.meta.get("first_hit", True)
            accepted_retries = 8
            # response_json = json.loads(response.text)
            # if response_json.get("status") == 404 and not first_hit:
            #     yield self.populate_product_item(response=response, details=extra.get("pd"), msg_dict=msg_dict)
            #     self.delete_sqs_message(message)
            #     return
            if (not msg_dict.get(
                    "sku") and first_hit) or '/music/player' in response.url or 'Prime Video (streaming online video)' in response.text:
                self.logger.warning("Product <%(url)s> no longer available", {"url": response.url})
                self.delete_sqs_message(message)
                return
            # browser_res = self.j_extract_first(response_json, "response")
            if first_hit:
                selector = Selector(text=response.text)
                availability_text = "".join(selector.xpath("//div[@id='availability']//text()").extract()).lower()
                if "available from these sellers" in availability_text or \
                        "temporarily out of stock" in availability_text or selector.xpath("//div[@id='outOfStock']"):
                    self.logger.warning("Product <%(url)s> Out of stock", {"url": response.url})
                    self.delete_sqs_message(message)
                    return
                style_attributes = {}
                for attr in selector.xpath("//div[@data-feature-name='productOverview']//tr"):
                    style_attributes[" ".join([
                        x.strip() for x in attr.xpath("./td[1]//text()").extract() if x.strip()])] = [
                        " ".join([x.encode('ascii', 'ignore').decode().strip() for x in attr.xpath(
                            "./td[2]//text()").extract() if x.strip()])]
                for attr in selector.xpath(
                        "//div[@id='detail_bullets_id']//div[@class='content']//li"
                ) or selector.xpath(
                    "//div[@id='detailBulletsWrapper_feature_div']//ul[contains(@class, 'detail-bullet-list')]/li"
                ) or selector.xpath("//div[@id='detailBullets_feature_div']//li") or []:
                    key = attr.xpath("./b//text()").extract_first() or attr.xpath(
                        ".//span[contains(@class, 'a-text-bold')]//text()").extract_first()
                    if key:
                        style_attributes[key.encode('ascii', 'ignore').decode().replace(':', '').strip()] = [" ".join([
                            v.strip() for v in
                            attr.xpath(".//descendant::text()[not(parent::style) and not(parent::script)"
                                       " and not(parent::span[contains(@class, 'a-text-bold')])]").extract()
                            if v.strip()])]
                if not (selector.xpath(
                        "//div[@id='prodDetails']//table[contains(@id, 'productDetails')]//tr") or selector.xpath(
                    "//ul[contains(@class, 'a-unordered-list a-nostyle a-vertical a-spacing-none detail-bullet-list')]/li") or selector.xpath(
                    "//table[contains(@class, 'a-keyvalue prodDetTable')]//tr") or selector.xpath(
                    "//table[contains(@class, 'a-bordered')]//tr") or selector.xpath(
                    "//div[@id='tech']//tr") or selector.xpath(
                    "//table[contains(@class, 'a-keyvalue a-spacing-mini')]//tr")) and msg_dict.get("current_retry",
                                                                                                    0) < accepted_retries:
                    self.logger.warning("Retrying for attributes <%(url)s> ", {"url": response.url})
                    msg_dict['current_retry'] = msg_dict.get("current_retry", 0) + 1
                    msg_dict['total_retries'] = accepted_retries + 1
                    self.push_message_in_queue(message_body=msg_dict)
                    self.delete_sqs_message(message)
                    return
                # if not (selector.xpath(
                #         "//div[@id='prodDetails']//table[contains(@id, 'productDetails')]//tr") or selector.xpath(
                #     "//ul[contains(@class, 'a-unordered-list a-nostyle a-vertical a-spacing-none detail-bullet-list')]/li") or selector.xpath(
                #     "//table[contains(@class, 'a-keyvalue prodDetTable')]//tr") or selector.xpath(
                #     "//table[contains(@class, 'a-bordered')]//tr") or selector.xpath(
                #     "//div[@id='tech']//tr") or selector.xpath("//table[contains(@class, 'a-keyvalue a-spacing-mini')]//tr")) and msg_dict.get("current_retry", 0)>=7:
                #     self.send_url_to_webhook(msg_dict.get("pdp_url", ""), "style attributes not found")
                for attr in selector.xpath("//div[contains(@class, 'product-facts-detail')]"):
                    style_attributes[" ".join([
                        x.strip() for x in attr.xpath(
                            ".//div[contains(@class, 'a-col-left')]//span[contains(@class, 'a-color-base')]//text()").extract()
                        if x.strip()])] = [
                        " ".join([x.strip() for x in attr.xpath(
                            ".//div[contains(@class, 'a-col-right')]//span[contains(@class, 'a-color-base')]//text()").extract()
                                  if x.strip()])]
                for attr in selector.xpath(
                        "//div[@id='prodDetails']//table[contains(@id, 'productDetails')]//tr") or selector.xpath(
                    "//table[contains(@class, 'a-keyvalue prodDetTable')]//tr"):
                    style_attributes[attr.xpath(
                        "./th[contains(@class, 'prodDetSectionEntry')]//text()").extract_first().strip()
                    ] = [" ".join([x.encode('ascii', 'ignore').decode().strip() for x in attr.xpath(
                        "./td//descendant::text()[not(parent::style) and not(parent::script)]"
                    ).extract() if x.strip()])]
                for attr in selector.xpath("//div[@class='a-section a-spacing-small a-spacing-top-small']//div"):
                    if attr.xpath(".//span[contains(@class,'text-bold')]/text()").extract_first() is not None:
                        style_attributes[
                            attr.xpath(".//span[contains(@class,'text-bold')]/text()").extract_first().strip().replace(
                                ":", "") or attr.xpath(
                                ".//span[@class='a-size-base a-text-bold']//text()").extract_first().strip().replace(
                                ":", "")
                            ] = [" ".join([
                            x.encode('ascii', 'ignore').decode().strip() for x in
                            attr.xpath(".//span[@class='a-size-base po-break-word']//text()").extract()
                            if x.strip()
                        ])]
                for attr in selector.xpath("//div[@id='tech']//tr"):
                    style_attributes[" ".join([
                        x.strip() for x in attr.xpath("./td[@style]//text()").extract() if x.strip()])] = [
                        " ".join([x.encode('ascii', 'ignore').decode().strip() for x in attr.xpath(
                            "./td[not(@style)]//text()").extract() if x.strip()])]
                for attr in selector.xpath("//table[contains(@class, 'a-keyvalue a-spacing-mini')]//tr"):
                    style_attributes[attr.xpath(
                        "./th//text()").extract_first().strip()
                    ] = [" ".join([x.encode('ascii', 'ignore').decode().strip() for x in attr.xpath(
                        "./td//text()"
                    ).extract() if x.strip()])]
                
                image_text = selector.xpath("//script[contains(text(), 'ImageBlockATF')]/text()").extract_first()
                if image_text:
                    image_json = re.findall(r"colorImage.*initial': (.*)}", image_text)
                    image_json = json.loads(image_json[0]) if image_json else {}
                    pdp_images = [x['hiRes'] or x['large'] for x in image_json]
                    feature_image = self.j_extract_first(
                        image_json, "$..[?(@.variant=='MAIN')].hiRes") or self.j_extract_first(
                        image_json, "$..[?(@.variant=='MAIN')].large")
                else:
                    pdp_images = selector.xpath("//div[@class='a-immersive-image-wrapper']//div[@data-a-image-name="
                                                "'immersiveViewMainImage']/@data-a-hires").extract()
                    feature_image = pdp_images[0] if pdp_images else ''
                rating = selector.xpath("//span[@id='acrPopover']/@title").extract_first() or selector.xpath(
                    "//span[@data-hook='average-stars-rating-text']//text()").extract_first()
                rating_count = selector.xpath(
                    "//span[@id='acrCustomerReviewText']/text()").extract_first() or selector.xpath(
                    "//a[@id='acrCustomerReviewLink']//span//text()").extract_first()
                pd = {
                    "feature_image": re.sub("images.+images", "images", feature_image),
                    "pdp_images": [re.sub("images.+images", "images", image) for image in pdp_images],
                    "pdp_url": msg_dict.get("pdp_url"),
                    "sku": msg_dict.get("sku"),
                }
                pd = self.populate_product_x_path(selector, pd, self.paths.get("product", {}))
                tags = extra.get("tags", [])

                xpaths = [
                    # Promo / deal badges
                    "//span[contains(@id,'dealBadge')]/span//text()",
                    "//div[contains(@class,'delightPricingBadge')]//span//text()",

                    # General badges
                    "//div[contains(@class,'badge-wrapper')]//a//i//text()",
                    "//span[@class='climatePledgeFriendlyProgramName']//text()",

                    # Amazon Choice badge
                    "//div[contains(@class, 'badge-wrapper')]//span[contains(@class, 'ac-badge-rectangle')]//span//text()",

                    # Best seller badge
                    "//div[contains(@class, 'badge-wrapper')]//span[contains(@class, 'mvt-best-seller-badge')]//text()",

                    #stock tag
                    "//div[@id='availability']/span/text()"
                ]

                for xp in xpaths:
                    values = selector.xpath(xp).extract()
                    for val in values:
                        val = val.strip()
                        if val:
                            tags.append(val)

                # Deduplicate
                tags = list(set(tags))
                pd["related_media"] = []
                pd["related_media"].append({'Tags': list(set(tags))}) if tags else []
                if not pd["pdp_images"]:
                    # self.send_url_to_webhook(msg_dict.get("pdp_url", ""), "product missing pdp images")
                    self.logger.warning("Product not available <%(url)s>", {"url": msg_dict.get("pdp_url")})
                    self.delete_sqs_message(message)
                    return
                whole_selling_price = selector.xpath(
                    "//span[contains(@class, 'riceToPay')]//span[contains(@class, 'price-whole')]//text()").extract_first()
                fraction_selling_price = selector.xpath(
                    "//span[contains(@class, 'riceToPay')]//span[contains(@class, 'price-fraction')]//text()").extract_first() if selector.xpath(
                    "//span[contains(@class, 'riceToPay')]//span[contains(@class, 'price-fraction')]//text()") else "00"
                if whole_selling_price:
                    selling_price = self.format_price(whole_selling_price + '.' + fraction_selling_price)
                    if pd["selling_price"]:
                        pd["selling_price"] = selling_price
                s_a = selector.xpath("//span[contains(@class, 'price-whole')]//text()").extract_first()
                s_b = selector.xpath("//span[contains(@class, 'price-fraction')]//text()").extract_first()
                if not pd["selling_price"] and not pd["mrp"] and s_a is not None and s_b is not None:
                    pd["selling_price"] = pd["mrp"] = self.format_price(s_a + '.' + s_b)
                if not pd["mrp"] or not pd["selling_price"]:
                    # self.send_url_to_webhook(msg_dict.get("pdp_url", ""),
                    #                          "product not availabe as mrp selling price not found")
                    self.logger.warning("Product not available <%(url)s>", {"url": msg_dict.get("pdp_url")})
                    self.delete_sqs_message(message)
                    return
                pd['style_attributes'] = style_attributes
                if rating and rating_count:
                    pd["rating"] = float(re.findall(r'\d+\.*\d* ', rating)[0])
                    pd["rating_count"] = int(re.findall(r'\d+', rating_count.replace(',', '').strip())[0])
                pd["brand"] = pd["brand"].replace("Brand:", "")
                if not pd["product_name"]:
                    pd["product_name"] = selector.xpath("//h1[@id='title']//text()").extract_first()
                price = selector.xpath(
                    "//div[@id='olp_feature_div']//span[@class='a-size-base a-color-price']//text()").extract_first()
                if not pd["mrp"]:
                    if price:
                        pd["mrp"] = self.format_price(price)
                        pd["selling_price"] = pd["mrp"]
                    else:
                        # self.send_url_to_webhook(msg_dict.get("pdp_url", ""),
                        #                          "Product not available or sold through different sellers")
                        self.logger.warning("Product not available or sold through different sellers <%(url)s>",
                                            {"url": extra.get("pdp_url")})
                        self.delete_sqs_message(message)
                        return
                if pd['color'] == '':
                    for x in pd['feature_list']:
                        x = x.strip()
                        if 'Color' in x or 'Colour' in x and pd['color'] == '':
                            pd['color'] = (re.findall(r'Colou?r.*?:[ ,-]?(.*?)[;,]', x)
                                           or re.findall(r'Colou?r.*?:[ ,-]?(.*)', x) or [''])[0]
                            [j.update({'color': pd['color']}) for j in pd['variants']]
                if pd['color'] == '':
                    pd['color'] = (
                            style_attributes.get('Color') or style_attributes.get('Colour', [''])
                    )[0]
                if ":" in pd["brand"]:
                    brand = pd['brand'].split(":")
                    pd["brand"] = brand[-1]
                if "Visit" in pd["brand"]:
                    brand = re.findall("Visit the (.*) Store", pd["brand"])[0]
                    pd["brand"] = brand
                pd['breadcrumbs'] = [x.strip() for x in pd['breadcrumbs'] if x != '›' and x.strip()]
                pd['feature_list'] = [x.strip() for x in pd['feature_list'] if x.strip()]
                if self.formatter.get("related_media_needed", False):
                    video_links = []
                    video_data = selector.xpath(
                        "//script[contains(text(), 'triggerVideoAjax')]//text()").extract_first()
                    if video_data:
                        video_json_data = re.findall(r"parseJSON\('({.*})", video_data)
                        if video_json_data:
                            video_json = json.loads(self.fix_json_from_string(video_json_data[0]))
                            video_links.extend(self.j_extract(video_json, "videos[?(@.groupType=='IB_G1')].url", []))
                            video_links.extend(self.j_extract(video_json, "videos[?(@.groupType=='IB_G2')].url", []))
                            video_links.extend(self.j_extract(video_json, "videos[?(@.groupType=='IB_G2_T1')].url", []))
                    video_data2 = selector.xpath("//script[contains(text(), 'ImageBlockBTF')]//text()").extract_first()
                    if video_data2:
                        video_json_data = re.findall(r"parseJSON\('({.*})", video_data2)
                        if video_json_data:
                            video_json = json.loads(self.fix_json_from_string(video_json_data[0]))
                            video_links.extend(self.j_extract(video_json, "videos[?(@.groupType=='IB_G1')].url", []))
                            video_links.extend(self.j_extract(video_json, "videos[?(@.groupType=='IB_G2')].url", []))
                            video_links.extend(self.j_extract(video_json, "videos[?(@.groupType=='IB_G2_T1')].url", []))
                    if video_links:
                        pd["related_media"].append({
                            'Video_Links': [{"product_videos": video_links}]
                        })
                    if selector.xpath("//script[contains(text(), 'spriteURLs')]//text()").extract_first():
                        pd["related_media"].append({
                            '360_Images': [True]
                        })
                    if selector.xpath("//div[@cel_widget_id='aplus']").extract_first():
                        pd["related_media"].append({
                            'A Content': [True]
                        })
                    pdf_links = []
                    for pdf in selector.xpath("//div[contains(@id, 'productDocuments')]//a", []):
                        key = pdf.xpath(".//text()").extract_first(default="").replace(' ', '_').replace('(',
                                                                                                         "").replace(
                            ')',
                            "")
                        value = pdf.xpath(".//@href").extract()
                        if key and value:
                            pdf_links.append({
                                key.lower(): value
                            })
                    if pdf_links:
                        pd["related_media"].append({"PDF": pdf_links})
                pd['variants'] = [{
                    'available': False if 'dropdownUnavailable' in var.xpath("./@class").extract_first() else True,
                    'color': pd['color'],
                    'size': html.unescape(var.xpath("./@data-a-html-content").extract_first()),
                    'stock': None,
                    'variant_sku': re.sub(r'.*,', '', var.xpath("./@value").extract_first()) or ''
                } for var in selector.xpath("//select[@name='dropdown_selected_size_name']/option[@class]")]
                if not pd['variants']:
                    pd['variants'] = [{
                        'available': False if var.xpath(
                            ".//span[@class='a-size-small default-slot-unavailable']"
                        ).extract_first() else True,
                        'color': pd['color'],
                        'size': var.xpath(
                            ".//span[@class='a-size-base swatch-title-text-display swatch-title-text']//text()").extract_first(),
                        'stock': None,
                        'variant_sku': ''
                    } for var in selector.xpath("""//ul[@data-a-button-group='{"name":"size_name"}']//li""") if
                        var.xpath(
                            ".//span[@class='a-size-base swatch-title-text-display swatch-title-text']//text()").extract_first()]
                if not pd['variants']:
                    pd['variants'] = [{
                        'available': False if var.xpath(
                            "./span[contains(@id,'size_name') and contains(@class, 'unavailable')]"
                        ).extract_first() else True,
                        'color': pd['color'],
                        'size': var.xpath(".//label//text()").extract_first(),
                        'stock': None,
                        'variant_sku': ''
                    } for var in selector.xpath("//div[contains(@class,'inline-twister-scroller-size_name')]//ul[cont"
                                                "ains(@data-a-button-group, 'size_name')]/li[contains(@class, 'listitem"
                                                "')]/span[@class='a-list-item']")]
                if not pd['variants']:
                    pd['variants'] = [{
                        'available': True if "swatchAvailable" in var.xpath(
                            "./@data-csa-c-content-id").extract_first() else False,
                        'color': pd['color'],
                        'size': var.xpath(".//*[contains(@class,'a-size-base')]//text()").extract_first(),
                        'stock': None,
                        'mrp': pd["mrp"],
                        'selling_price': self.format_price(
                            var.xpath(".//*[contains(@class,'a-size-mini')]//text()").extract_first(
                                default=pd["selling_price"])),
                        'variant_sku': var.xpath("./@data-defaultasin").extract_first() or ''
                    } for var in selector.xpath(
                        """//ul[contains(@data-a-button-group,"size_name") or contains(@data-a-button-group, "style_name")]//li""")
                        if
                        var.xpath(".//*[contains(@class,'a-size-base')]//text()").extract_first()]
                if not pd['variants']:
                    if any([i in availability_text for i in
                            ['in stock', 'available to ship', 'usually dispatched', 'order soon',
                             'usually ships within']]) and 'unavailable' not in availability_text:
                        available = True
                    else:
                        available = False
                    pd['variants'] = [{
                        'available': available,
                        'color': pd['color'],
                        'size': selector.xpath("//div[contains(@class,'inline-twister-scroller-size_name')]//span["
                                               "contains(@id,'size_name') and contains(@class,'a-button-selected')]//"
                                               "label//text()").extract_first() or selector.xpath(
                            "//div[contains(@id,'size_name')]//span/text()").extract_first() or selector.xpath(
                            "//tr[contains(@class,'po-size')]//span[contains(@class,'break')]/text()").extract_first() or selector.xpath(
                            "//li[@class='swatchSelect'][contains(@id,'size')]//p//text()"
                        ).extract_first() or 'One Size',
                        'stock': None,
                        'variant_sku': pd['sku']
                    }]
                else:
                    pd['style_attributes'].pop('Size', None)
                if pd["pdp_images"]:
                    pd["pdp_images"] = self.unique_elements_seq(pd["pdp_images"])
                pd["style_attributes"]["SubscribeAndSave Discount"] = []
                pd["style_attributes"]["SubscribeAndSave Frequency"] = []
                pd["style_attributes"]["SubscribeAndSave Price"] = []
                if selector.xpath("//div[@id = 'snsAccordionRowMiddle']"):
                    discounted_percentage = selector.xpath(
                        "//div[@id = 'snsAccordionRowMiddle']//div[contains(@id, 'snsDiscountPill')]//span[contains(@class, 'discountTextLeft')]//text()").extract() or \
                                            selector.xpath(
                                                "//div[@id = 'snsAccordionRowMiddle']//div[contains(@id, 'snsDiscountPill')]//span[contains(@class, 'discountText')]//text()").extract()
                    discounted_price = selector.xpath(
                        "//div[@id = 'snsAccordionRowMiddle']//span[contains(@id, 'sns-base-price')]//span[contains(@class, 'a-offscreen')]//text()").extract_first()
                    discounted_price = self.format_price(discounted_price)
                    frequency_value = selector.xpath(
                        "//div[contains(@id, 'snsFrequencyAccordionRow')]/@data-frequency-label").extract()
                    for f in frequency_value:
                        if 'common' in f.lower():
                            pd["style_attributes"]["SubscribeAndSave Frequency"] = [f]

                    pd["style_attributes"]["SubscribeAndSave Discount"] = discounted_percentage
                    pd["style_attributes"]["SubscribeAndSave Price"] = [str(discounted_price)]

                review_url = selector.xpath(
                    "//a[@data-hook='see-all-reviews-link-foot']/@href").extract_first() or selector.xpath(
                    "//a[@data-hook='reviews-summary-mobile']/@href").extract_first()

                ship_by = selector.xpath(
                    "//div[contains(@class,'offer-display-feature-text')][@offer-display-feature-name='desktop-fulfiller-info']//span//text()").extract_first().strip() if selector.xpath(
                    "//div[contains(@class,'offer-display-feature-text')][@offer-display-feature-name='desktop-fulfiller-info']//span//text()").extract_first() else ""
                sold_by = selector.xpath(
                    "//div[contains(@class,'offer-display-feature-text')][@offer-display-feature-name='desktop-merchant-info']//span[contains(@class, 'message')]//text()").extract_first().strip() if selector.xpath(
                    "//div[contains(@class,'offer-display-feature-text')][@offer-display-feature-name='desktop-merchant-info']//span[contains(@class, 'message')]//text()").extract_first() else ""
                heading_shipper_seller = selector.xpath("//div[contains(@class,'offer-display-feature-label')][@offer-display-feature-name='desktop-merchant-info']//span//text()").extract_first() or ''
                if 'seller' in heading_shipper_seller.lower().strip() and 'shipper' in heading_shipper_seller.lower().strip():
                    ship_by = sold_by

                if not ship_by:
                    ship_by = selector.xpath(
                        "//div[contains(@class,'offer-display-feature-text')][@offer-display-feature-name='desktop-fulfiller-info']//a//span//text()").extract_first()
                if not sold_by:
                    sold_by = selector.xpath(
                        "//div[contains(@class,'offer-display-feature-text')][@offer-display-feature-name='desktop-merchant-info']//a//span//text()").extract_first().strip() if selector.xpath(
                        "//div[contains(@class,'offer-display-feature-text')][@offer-display-feature-name='desktop-merchant-info']//a//span//text()").extract_first() else ""

                if ship_by and sold_by:
                    pd["style_attributes"]["sold_by"] = [sold_by]
                    pd["style_attributes"]["fulfilled_by"] = [ship_by]
                else:
                    pd["style_attributes"]["sold_by"] = []
                    pd["style_attributes"]["fulfilled_by"] = []



                if review_url:
                    new_msg_dict = msg_dict.copy()
                    new_msg_dict["forced_url"] = self.formatter.get("pdp_url").format(review_url)
                    new_msg_dict["first_hit"] = False
                    new_msg_dict["headers"] = {'referer': pd['pdp_url'],
                                               'downlink': '10',
                                               'ect': '4g',
                                               'sec-fetch-site': 'same-origin',
                                               'sec-fetch-dest': 'document',
                                               'rtt': '150'
                                               }
                    new_msg_dict["current_retry"] = 0
                    new_msg_dict["extra"] = {"pd": pd}
                    self.push_message_in_queue(message_body=new_msg_dict)
                    self.delete_sqs_message(message)
                    return
                yield self.populate_product_item(response=response, details=pd, msg_dict=msg_dict)
            else:
                pd = extra.get('pd')
                selector = Selector(text=response.text)  # Returning byte object
                review_count_text = selector.xpath(
                    "substring-after(//div[@data-hook='cr-filter-info-review-rating-count']//text()[normalize-space()]"
                    ", 'ratings')").extract_first().strip()
                # Format is 2,554 global ratings | 1,502 global reviews and 2,409 total ratings, 472 with reviews
                review_count = re.findall(r'(\d+)', review_count_text.replace(",", ''))
                pd["review_count"] = int(review_count[0]) if review_count else None
                yield self.populate_product_item(response=response, details=pd, msg_dict=msg_dict)
            self.delete_sqs_message(message)
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            line_number = exc_traceback.tb_lineno
            self.logger.info(f"Error {e} on line No. {line_number}")

    def category_parser(self, response: Response, **kwargs) -> Union[Dict, None]:
        msg_dict: dict = response.meta.get("msg_dict", {})
        try:
            message = response.meta.get("message", None)
            current_page = response.meta.get("current_page", 1)
            rank = response.meta.get("start_rank", 0)
            # response_json = json.loads(response.text)
            # browser_res = self.j_extract_first(response_json, "response")
            selector = Selector(text=response.text)
            if selector.xpath("//h3//span[contains(text(), 'No results')]").extract_first():
                self.logger.warning("Deleting <%(url)s> No Results Found", {"url": response.url})
                self.delete_sqs_message(message)
                return
            if 'See all results' in response.text:
                category_url = selector.xpath(
                    "//div[@class='a-box a-text-center apb-browse-searchresults-footer']//a[@class='a-link-normal']/@href"
                ).extract_first() or selector.xpath(
                    "//div[contains(@class,'a-cardui _octopus-search-result-card')]//a[@class='a-link-normal']/@href"
                ).extract_first()
                category_url = self.formatter.get("pdp_url").format(category_url)
                msg_dict["forced_url"] = category_url
                self.push_message_in_queue(message_body=msg_dict)
                self.delete_sqs_message(message)
                return
            products = self.get_product_collection_x_path(selector, crawl_type="category")
            if len(products) == 0 and msg_dict.get("current_retry", 0) < 3:
                products = selector.xpath("//div[@id='mainResults']//li")
                if not products:
                    self.logger.warning("Retrying <%(url)s> No Products Found", {"url": response.url})
                    self.push_message_in_queue_for_retry(message)
                    return
            if len(products) == 0 and msg_dict.get("current_retry", 0) >= 3:
                self.push_message_in_dead_letter_queue(msg_dict)
                self.delete_sqs_message(message)
                return
            msg_dict.pop("forced_url", None)
            msg_dict["current_page"] = current_page
            product_collection = []
            for p in products:
                if 'sponsored' in "".join(p.xpath('.//text()').extract()).lower():
                    continue
                sku_val = p.xpath('./@data-asin').extract_first()
                if not sku_val:
                    continue
                rank += 1
                pd = self.populate_product_details_x_path(p, {
                    "rank": rank,
                }, self.paths.get("category").get("product_details", {}))
                tags = []
                bought_tag = p.xpath(".//div[@data-cy = 'reviews-block']//div[2]/span/text()").extract_first()
                if bought_tag:
                    tags.append(bought_tag)
                url_parts = list(urlparse(pd["pdp_url"]))
                query = dict(parse_qsl(url_parts[4]))
                query['psc'] = 1
                url_parts[4] = urlencode(query)
                pd["pdp_url"] = urlunparse(url_parts)

                # product_pdp_urls = [
                #     self.formatter.get("pdp_url").format(f"/gp/product/{pd['sku']}"),
                #     self.formatter.get("pdp_url").format(f"/dp/{pd['sku']}?th=1&psc=1"),
                #     self.formatter.get("pdp_url").format(f"/dp/{pd['sku']}"),
                #     self.formatter.get("pdp_url").format(f"/dp/product/{pd['sku']}")
                # ]
                # pd['pdp_url'] = random.choices(product_pdp_urls)[0]
                # pd['pdp_url'] = self.formatter.get("pdp_url").format(f"/dp/{pd['sku']}?th=1&psc=1")
                self.push_ranked_product_to_crawl(
                    pd=pd,
                    msg_dict=msg_dict,
                    extra={'tags': tags}
                )
                pd.pop('pdp_url', None)
                product_collection.append(pd)
            if current_page == 1:
                msg_dict["page_size"] = rank
                msg_dict["pages_to_crawl"] = current_page
            category_item = self.populate_category_item(response, msg_dict, products=product_collection)
            yield category_item
            next_page_url = selector.xpath("//li[@class='a-last']/a/@href").extract_first() or selector.xpath(
                "//div[contains(@class, 's-pagination-container')]//a[contains(@class,'s-pagination-next')]/@href"
            ).extract_first() or selector.xpath("//a[@id='pagnNextLink']/@href").extract_first()
            if next_page_url is not None:
                msg_dict["forced_url"] = self.formatter.get("pdp_url").format(next_page_url)
                msg_dict["start_rank"] = rank
                msg_dict["current_retry"] = 0
                msg_dict["current_page"] = current_page + 1
                msg_dict["pages_to_crawl"] = current_page + 1
                self.push_next_page_to_crawl(new_msg_dict=msg_dict)
            else:
                # For sending msg in gchat group to find out how many pages a category url have
                webhook_url = "https://chat.googleapis.com/v1/spaces/AAAAUVnzxlg/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=7jUQz77m4pAxfLCcKCLRq_Wz5pkBrs5-NGqbGADMVD4"
                app_message = {
                    "text": f"category_url:- {msg_dict.get('url')}\n"
                            f"last_page_url:- {response.url}\n"
                            f"no of pages:- {current_page}",
                }
                message_headers = {"Content-Type": "application/json; charset=UTF-8"}
                requests.request(
                    url=webhook_url,
                    method="POST",
                    headers=message_headers,
                    data=json.dumps(app_message),
                )
            self.delete_sqs_message(message)
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            line_number = exc_traceback.tb_lineno
            self.logger.info(f"Error {e} on line No. {line_number}")


class AmazonAE(Amazon):
    """
    Amazon AE
    """

    name = "amazon-ae"
    formatter = {"pdp_url": "https://www.amazon.ae{}"}

    def get_cookies(self, msg_dict) -> Dict:
        forced_cookies = msg_dict.get("get_cookies", None)
        if forced_cookies:
            return forced_cookies
        return {
            'i18n-prefs': 'AED'
        }


class AmazonUS(Amazon):
    """
    Amazon US
    """

    def get_cookies(self, msg_dict) -> Dict:
        return {
            'i18n-prefs': 'USD',
        }

    name = "amazon-us"
    formatter = {"pdp_url": "https://www.amazon.com{}", "related_media_needed": True}

    @staticmethod
    def format_price(text: (str, int, float)):
        try:
            if isinstance(text, (int, float)):
                return text
            text = text.strip()
            text = text.replace("$", "")
            if text.strip():
                return float(re.sub(r"[^0-9.}]+", "", text))
        except Exception as err:
            print(err)


class AmazonJP(Amazon):
    """
    Amazon JP
    """

    name = "amazon-jp"
    formatter = {"pdp_url": "https://www.amazon.co.jp{}", "related_media_needed": False}

    @staticmethod
    def format_price(text: (str, int, float)):
        try:
            if isinstance(text, (int, float)):
                return text
            text = text.strip()
            text = text.replace("¥", "")
            if text.strip():
                return float(re.sub(r"[^0-9.}]+", "", text))
        except Exception as err:
            print(err)

    def get_cookies(self, msg_dict) -> Dict:
        forced_cookies = msg_dict.get("get_cookies", None)
        if forced_cookies:
            return forced_cookies
        return {
            'i18n-prefs': 'JPY',
            'lc-acbjp': 'en_US',
            'session-id': '355-5235786-2334302'
        }


class AmazonAU(Amazon):
    """
    Amazon AU
    """

    name = "amazon-au"
    formatter = {"pdp_url": "https://www.amazon.com.au{}"}
    paths = {
        "review": {
            # "review_collection": [
            #     "//div[contains(@id, 'cm_cr-review_list')]//div[contains(@data-hook, 'review')]/div[contains(@class, 'a-row')]"],
            "review_collection": [
                "//span[contains(@data-hook, 'cr-widget-FocalReviews')]//li[contains(@class, 'review aok-relative')]"],
            # "review_details": {
            #     "review_id": {"paths": ["./@id"], "default": ""},
            #     "title": {"paths": [], "default": ""},
            #     "rating": {"paths": [], "default": None},
            #     "description": {"paths": [], "default": ""},
            #     "review_date": {"paths": [], "default": None},
            #     "rating_range": {"paths": [], "default": 5},
            #     "helpful": {"paths": [], "default": None},
            #     "quality": {"paths": [], "default": None},
            #     "value": {"paths": [], "default": None},
            #     "recommended": {"paths": [], "default": None}
            # }
            "review_details": {
                "review_id": {"paths": ["./@id"], "default": ""},
                "title": {"paths": [], "default": ""},
                "rating": {"paths": [], "default": None},
                "description": {"paths": [], "default": ""},
                "review_date": {"paths": [], "default": None},
                "rating_range": {"paths": [], "default": 5},
                "helpful": {"paths": [], "default": None},
                "quality": {"paths": [], "default": None},
                "value": {"paths": [], "default": None},
                "recommended": {"paths": [], "default": None}
            }
        },
        "product": {
            "product_details": {
                "sku": {"paths": [], "default": ""},
                "color": {"paths": [
                    "//div[contains(@id,'variation') and contains(@id,'color')]//span[@class='selection']/text()",
                    "//div[contains(@class,'inline-twister-scroller-color_name')]//span[contains(@id,'color_name') and contains(@class,'a-button-selected')]//input/@aria-label",
                    "//span[@class='a-size-base a-color-secondary' and contains(text(),'Color')]/following-sibling::span//text()",
                    "//span[@class='a-size-base a-color-secondary' and contains(text(), 'Colour')]/following-sibling::span/text()",
                    "//span[@class='a-size-base a-color-secondary inline-twister-dim-title' and contains(text(),'Color')]/following-sibling::span//text()",
                    "//span[@class='a-size-base a-color-secondary inline-twister-dim-title' and contains(text(),'Colour')]/following-sibling::span//text()",
                    "//tr[contains(@class,'color')]//text()[2]"
                ], "default": ""},
                "product_name": {"paths": [
                    "//span[@id='productTitle']/text()"
                ], "default": ""},
                "brand": {"paths": [
                    "//div[@data-feature-name='bylineInfo']//a[@id='bylineInfo']/text()",
                    "//div[@id='amznStoresBylineLogoTextContainer']/a[contains(@class, 'a-text-bold')]/text()"
                ], "default": ""},
                "breadcrumbs": {
                    "paths": [
                        "//div[@id='wayfinding-breadcrumbs_container']//ul/li/span//text()",
                        "//a[contains(@class, 'breadcrumbInlineLinks')]//text()",
                        "//div[contains(@id,'wayfinding-breadcrumbs')]//ul/li/span//a//text()"
                    ],
                    "default": [],
                },
                "description": {"paths": [
                    "//div[@id='productDescription']/p//text()",
                    "//div[contains(@id, 'important-info')]//text()",
                    "//div[contains(@data-a-expander-name, 'book_description_expander')]/div[contains(@class, 'a-expander-content')]//span//text()",
                    "//div[contains(@id, 'bookDescription_feature_div')]//div[contains(@class, 'books-expander-content')]//span//text()",
                    "//div[@id='productDescription']//p/span/text()"
                ], "default": ""},
                "feature_list": {"paths": [
                    "//div[@id='feature-bullets']//li//text()",
                    "//span[contains(@class,'a-list-item a-size-base')]//text()"
                ], "default": []},
                "offer_list": {"paths": [], "default": []},
                "feature_image": {"paths": [], "default": ""},
                "pdp_images": {"paths": [], "default": []},
                "pdp_url": {"paths": [], "default": ""},
                "mrp": {"paths": [
                    "//span[@id='priceblock_saleprice']//text()",
                    "//span[@id='priceblock_ourprice']//text()",
                    "//span[@class='priceBlockStrikePriceString a-text-strike']//text()",
                    "//span[@id='priceblock_dealprice']//text()",
                    "//span[@id='_price']//span[contains(@class,'normal-price')]//text()",
                    "//span[@id='_price']//span[contains(@class,'price-strikethrough')]//text()",
                    "//span[@id='atfRedesign_strikeThroughPrice']//span[contains(@class,'restOfPrice a-text-strike')]//text()",
                    "//div[@id='corePriceDisplay_desktop_feature_div']//span[contains(@class, 'priceToPay')]//span[@class='a-offscreen']//text()",
                    "//div[contains(@id,'apex_desktop_new') and not (contains(@style, 'none'))]//span[contains(@class, 'basisPrice')]//span[@class='a-offscreen']//text()",
                    "//div[@id='corePriceDisplay_desktop_feature_div']//span[@data-a-color='secondary']//span[@class='a-offscreen']//text()",
                    "//div[@id='corePrice_desktop']//span[@data-a-color='secondary']//span[@class='a-offscreen']//text()",
                    "//div[@id='corePrice_desktop']//span[contains(@class, 'apexPriceToPay')]//span[@class='a-offscreen']//text()",
                    "//div[contains(@id,'corePriceDisplay')]//span[contains(@class, 'PriceToPay')]//span[@class='a-offscreen']//text()",
                    "//div[contains(@id,'corePriceDisplay')]//span[contains(@class, 'basisPrice')]//span[@class='a-offscreen']//text()",
                    "//span[contains(@class, 'riceToPay')]//span[@class='a-offscreen']/text()",
                    "//div[contains(@id,'apex_desktop_new') and not (contains(@style, 'none'))]//span[@data-a-color='secondary']//span[@class='a-offscreen']//text()",
                    "//span[contains(@class, 'aok-offscreen')]//text()"
                ], "default": 0},
                "selling_price": {"paths": [
                    "//span[@id='priceblock_saleprice']//text()",
                    "//span[@id='priceblock_ourprice']//text()",
                    "//span[@class='priceBlockStrikePriceString a-text-strike']//text()",
                    "//span[@id='priceblock_dealprice']//text()",
                    "//span[@id='_price']//span[contains(@class,'normal-price')]//text()",
                    "//span[@id='_price']//span[contains(@class,'price-strikethrough')]//text()",
                    "//span[@id='atfRedesign_strikeThroughPrice']//span[contains(@class,'restOfPrice a-text-strike')]//text()",
                    "//div[@id='corePriceDisplay_desktop_feature_div']//span[contains(@class, 'priceToPay')]//span[@class='a-offscreen']//text()",
                    "//div[@id='corePrice_desktop']//span[contains(@class, 'apexPriceToPay')]//span[@class='a-offscreen']//text()",
                    "//div[contains(@id,'corePriceDisplay')]//span[contains(@class, 'PriceToPay')]//span[@class='a-offscreen']//text()",
                    "//div[contains(@id,'apex_desktop_new') and not (contains(@style, 'none'))]//span[contains(@class, 'basisPrice')]//span[@class='a-offscreen']//text()",
                    "//span[contains(@class, 'aok-align-center')]/span[contains(@class, 'a-offscreen')]//text()",
                    "//div[contains(@id,'corePriceDisplay')]//span[contains(@class, 'basisPrice')]//span[@class='a-offscreen']//text()",
                    "//span[contains(@class, 'riceToPay')]//span[@class='a-offscreen']/text()",
                    "//span[contains(@id, 'tp_price_block_total_price_ww')]//span[contains(@class, 'a-offscreen')]//text()"
                ], "default": 0},
                "rating": {"paths": [], "default": None},
                "rating_count": {"paths": [], "default": None},
                "review_count": {"paths": [], "default": None},
                "style_attributes": {"paths": [], "default": {}},
                "variants": {"paths": [], "default": []},
                "related_products": {"paths": [], "default": []},
                "relation": {"paths": [], "default": ""},
            },
            "offer_list": {
                "code": {"paths": [], "default": ""},
                "title": {"paths": [], "default": ""},
                "description": {"paths": [], "default": ""},
                "savings": {"paths": [], "default": ""},
            },
            "variants": {
                "size": {"paths": [], "default": ""},
                "stock": {"paths": [], "default": None},
                "available": {"paths": [], "default": None},
                "color": {"paths": [], "default": None},
                "variant_sku": {"paths": [], "default": ""},
            },
            "style_attributes": {
                "style_key": {"paths": [], "default": ""},
                "style_value": {"paths": [], "default": ""},
            },
            "related_products": {
                "pdp_url": {"paths": [], "default": []},
                "sku": {"paths": [], "default": ""},
                "color": {"paths": [], "default": ""},
            },
        },
        "category": {
            "product_collection": [
                "//*[@id='mainResults']/ul/li",
                "//div[contains(@class, 'result-list') and contains(@class, 'main-slot')]/div[@data-uuid]"
            ],
            "product_details": {
                "sku": {"paths": ["./@data-asin"], "default": ""},
                "rank": {"paths": [], "default": ""},
                "pdp_url": {"paths": [".//a[@class='a-link-normal s-no-outline']/@href"], "default": ""},
            },
            "pagination": {"next_page": "", "total_pages": "", "total_count": "", "page_size": ""},
        },
    }
    user_pass = f"{os.getenv('PRIVATE_PROXY_USERNAME')}:{os.getenv('PRIVATE_PROXY_PASSWORD').replace('@', '%40')}"
    proxy = os.getenv("LOCAL_PROXY_SERVER").replace("http://", "")
    proxies = {"http://": f"http://{user_pass}@{proxy}",
               "https://": f"http://{user_pass}@{proxy}"}
    custom_settings = {
        "DOWNLOADER_MIDDLEWARES": {
            'engine.middlewares.captcha.AmazonCaptchaMiddleware': 450,
        }
    }

    @staticmethod
    def format_price(text: (str, int, float)):
        try:
            if isinstance(text, (int, float)):
                return text
            text = text.strip()
            text = text.replace("¥", "")
            if text.strip():
                if re.sub(r"[^0-9.}]+", "", text):
                    return float(re.sub(r"[^0-9.}]+", "", text))
        except Exception as err:
            print(err)

    def get_cookies(self, msg_dict) -> Dict:
        if msg_dict.get("crawl_type") == "category":
            return {
                # 'session-id': '356-8248075-4959505',
                # 'i18n-prefs': 'AUD',
                # 'ubid-acbau': '358-9514834-3813313',
                # 'session-token': 'QfwQpJJWUV0FqRQeYYuB3aTcFEJqHjtuyXJ7H1T33pm3EdWUojInBcsZO6mF0uAuvh0Y0wv/wpopaNSD5DBsrLILfKAEM4Cybk7QAH11J+XdEkChWvYsVN6NWzTz0KuqVxlSV8ALY139CqcFNqBqgfxWmvLRGRlHIqCk7lVdiEoqc2VzDT96RroHkaFyUrbKoRjY6T2zmU8mdlwq24bzbRaSkDk5GP0urc1g3ewiIRSdse8VARuhTKl3YlIcTBPWnjdld7J5yor+1Nl1EZ6ZVOc1K9a36iKqE26MOlP7D8U/jkpp+f3+fnm0mlCn1ugsIl6EaD1PPsuyKpHdtduW84YDDPUh5Mkg5OiZtGqse8I=',
                'session-id': '355-9027298-7615961',
                'i18n-prefs': 'AUD',
                'ubid-acbau': '357-2549819-0852237',
                'session-id-time': '2082787201l',
                'session-token': '"Zo5y5AQRTP74cZI3qNczFkO+sjfdAOVMziW1es/WLtE6b7l9Ium+4BQmxapQYmkop5SLaNdwRBJ7gvUYMIgW8MvuBoLemwT3wLV9Ycuq8Um0GDHNkCmJwQvYeVVqgtkKSeAi9VXdJS/64u59zx/O2Tx46zOlgd/MMOMiw/moGRw32cL3M2xSJoDqKUTBhYpthvPJ5U1uIQ9We9yiAkOHxdFCdhhT0Jfij1Bbi78kqEwVqAD6uQXcv409HG8dV5+Qn2gKzYtAdXcoZSg68kro25LZCSADv9slIK7naydvLw3s2sShrDFnRhul0YccYLiORVpHyg2mzFqZ+6C/L+RFZ+CaD2arhBs5M8YsXrfdSzo="',
                'csm-hit': 'tb:s-8MXSWWF676EE1864RKVR|1713773491774&t:1713773493240&adb:adblk_yes'
            }
        return {
            # 'session-id': '356-8248075-4959505',
            # 'i18n-prefs': 'AUD',
            # 'ubid-acbau': '358-9514834-3813313',
            # 'session-token': '"eSCdfrG/EnqHgrQbVuT3WfsjjPvkIzofmaUDEeFVIvklZIkD8boOrl6U20w67WRp98bu5idR1Yc1oVpb3jeR2ILQlHPtK+1fLZT/XWtf3t4qRuxrFcZFI8aBm/YU0574q1VMPkK8eaUFHHDSKfnehF1EcnYsRf5AGsrkHQaKLuCVWONm58hvH/scDq/VkkUM/ydMeQxGV+RLgF/AbnGW+tCA66n7U44hbIiffWMvd2Htl0MZuyybSummaAqrb9ODcDYBtNS3PqbJ3Jk9OusF2bbt5OYdSRdhamYRVAS2YSsM6gU+mdQx8WlWpJEl/H92+wS6XSYDlw6F00dNNJXqaSEj2QA370CVaC5OZsUGQU0="',
            'session-id': '355-9027298-7615961',
            'i18n-prefs': 'AUD',
            'ubid-acbau': '357-2549819-0852237',
            'session-id-time': '2082787201l',
            'session-token': '"Z1lhK62yrM571g4E8Ma8Y1RRidPV8iZAtYTTMxsqhtNhoIw+URxKJylVmwZmQp74RWxIwKaRk/9QqOsO4qxTs2+s2QyTU7Q0hr7UINOkvs2mN6o+mVUHl4yPTh8/dg85cIaQgROVeN2jGbWreZkWC/s2yTS4nr6BxhZdGX21NpcakfYgjufF+J4GZpxKHs/QqV7PSuLNcT03ghYMraYVG2O5732oRG+KAgVJQ1tqUaEDlvSJl1spBuusOJTBlHEKok6cEqX5zziqY9NcgTa7WsFWXPnVnmnMGlyQke+SOukhroVlZ8T22+C+Nni/A2RLK9Qc1UmClBY+fxXqjjmqYw0rhevhVrl27uZBq5UFKMk="',
            'csm-hit': 'tb:s-AC3JVGHF4M6CAD74T64Y|1713770685174&t:1713770687586&adb:adblk_yes',

        }

    # def get_review_url(self, msg_dict) -> str:
    #     """get_review_url"""
    #     forced_url = msg_dict.get('forced_url', None)
    #     if forced_url:
    #         return forced_url
    # url = msg_dict.get('pdp_url')
    # url_str1 = url.split('/ref')[0]
    # url_str1 = url_str1.replace('dp', 'product-reviews')
    # review_api_url = f"{url_str1}/ref=cm_cr_dp_d_show_all_btm?ie=UTF8&reviewerType=all_reviews&sortBy=recent"
    # msg_dict["forced_headers"] = {'referer': msg_dict.get('pdp_url'),
    #                               'downlink': '10',
    #                               'ect': '4g',
    #                               'sec-fetch-site': 'same-origin',
    #                               'sec-fetch-dest': 'document',
    #                               'rtt': '150'
    #                               }
    # return review_api_url

    def get_review_url(self, msg_dict) -> str:
        """get_review_url"""
        forced_url = msg_dict.get('forced_url', None)
        if forced_url:
            return forced_url
        return msg_dict.get("pdp_url")

    # def review_parser(self, response, **kwargs) -> Union[Dict, None]:
    #     msg_dict: dict = response.meta.get("msg_dict", {})
    #     message = response.meta.get("message", None)
    #     first_hit = msg_dict.get('first_hit', True)
    #     selector = Selector(text=response.text)
    #     extra = msg_dict.get('extra', {})
    #     reviews = self.get_review_collection_x_path(selector, crawl_type="review")
    #     review_collection = extra.get("review_collection", [])
    #     for r in reviews:
    #         review = self.populate_review_details_x_path(r, {}, self.paths.get("review").get("review_details", {}))
    #         review["review_id"] = review["review_id"].split('-review-card')[0]
    #         review_date_str = r.xpath(".//span[contains(@data-hook, 'review-date')]/text()").extract_first()
    #         date_str = re.search(r'on (\d+ \w+ \d+)', review_date_str).group(1)
    #         dt = datetime.strptime(date_str, '%d %B %Y')
    #         title = r.xpath(".//span[contains(@data-hook, 'review-title')]/span/text()").extract_first() or r.xpath(
    #             ".//a[contains(@data-hook, 'review-title')]/span/text()").extract_first()
    #         rating = r.xpath(".//i[contains(@class, 'review-rating')]//text()").extract_first()
    #         if rating:
    #             rating_range = rating.split('of')[1]
    #             rating_range = re.findall(r"\d+", rating_range)[0]
    #             review["rating_range"] = int(rating_range)
    #             rating = rating.split('out')[0]
    #             rating = rating.strip()
    #             review["rating"] = float(rating)
    #         helpful_count_text = r.xpath(".//span[contains(@class, 'cr-vote-text')]//text()").extract_first()
    #         if helpful_count_text:
    #             num_value = re.findall(r'\d+', helpful_count_text)
    #             if num_value:
    #                 helpful_count = num_value[0]
    #                 review["helpful"] = bool(helpful_count)
    #             if not num_value:
    #                 helpful_word_count = helpful_count_text.split('person')[0]
    #                 helpful_word_count = helpful_word_count.strip()
    #                 review["helpful"] = bool(helpful_word_count)
    #         description_list = r.xpath(".//span[contains(@data-hook, 'review-body')]//text()").extract()
    #         description_list = [d.strip() for d in description_list if d.strip()]
    #         description_list = [d for d in description_list if d not in ['The media could not be loaded.']]
    #         description = ""
    #         if description_list:
    #             description = " ".join(description_list)
    #             description = description.replace('\xa0', ' ')
    #         review["review_date"] = dt.strftime('%Y-%m-%d')
    #         review["description"] = description
    #         review["title"] = title
    #         review_collection.append(review)
    #     next_review_page_url = selector.xpath(
    #         "//ul[contains(@class, 'pagination')]/li[contains(@class, 'a-last')]/a/@href").extract_first()
    #     if first_hit and next_review_page_url:
    #         url_parts = list(urlparse(next_review_page_url))
    #         query = dict(parse_qsl(url_parts[4]))
    #         if query.get('pageNumber') is None:
    #             next_review_page_url = query.get('openid.return_to')
    #         new_msg_dict = msg_dict.copy()
    #         next_review_page_url = self.formatter.get("pdp_url").format(next_review_page_url)
    #         new_msg_dict["forced_url"] = next_review_page_url
    #         new_msg_dict["first_hit"] = False
    #         new_msg_dict["current_retry"] = 0
    #         new_msg_dict["extra"] = {"review_collection": review_collection}
    #         self.push_message_in_queue(message_body=new_msg_dict)
    #     else:
    #         review_item = self.populate_review_item(response, msg_dict=msg_dict, reviews=review_collection)
    #         yield review_item
    #     self.delete_sqs_message(message)

    def review_parser(self, response, **kwargs) -> Union[Dict, None]:
        msg_dict: dict = response.meta.get("msg_dict", {})
        message = response.meta.get("message", None)
        first_hit = msg_dict.get('first_hit', True)
        selector = Selector(text=response.text)
        extra = msg_dict.get('extra', {})
        reviews = self.get_review_collection_x_path(selector, crawl_type="review")
        review_collection = extra.get("review_collection", [])
        for r in reviews:
            review = self.populate_review_details_x_path(r, {}, self.paths.get("review").get("review_details", {}))
            review["review_id"] = review["review_id"].split('-review-card')[0]
            review_date_str = r.xpath(".//span[contains(@data-hook, 'review-date')]/text()").extract_first()
            date_str = re.search(r'on (\d+ \w+ \d+)', review_date_str).group(1)
            dt = datetime.strptime(date_str, '%d %B %Y')
            title = r.xpath(".//span[contains(@data-hook, 'review-title')]/span/text()").extract_first() or r.xpath(
                ".//a[contains(@data-hook, 'review-title')]/span/text()").extract_first()
            rating = r.xpath(".//i[contains(@class, 'review-rating')]//text()").extract_first()
            if rating:
                rating_range = rating.split('of')[1]
                rating_range = re.findall(r"\d+", rating_range)[0]
                review["rating_range"] = int(rating_range)
                rating = rating.split('out')[0]
                rating = rating.strip()
                review["rating"] = float(rating)
            helpful_count_text = r.xpath(".//span[contains(@class, 'cr-vote-text')]//text()").extract_first()
            if helpful_count_text:
                num_value = re.findall(r'\d+', helpful_count_text)
                if num_value:
                    helpful_count = num_value[0]
                    review["helpful"] = bool(helpful_count)
                if not num_value:
                    helpful_word_count = helpful_count_text.split('person')[0]
                    helpful_word_count = helpful_word_count.strip()
                    review["helpful"] = bool(helpful_word_count)
            description_list = r.xpath(".//span[contains(@data-hook, 'review-body')]//text()").extract()
            description_list = [d.strip() for d in description_list if d.strip()]
            description_list = [d for d in description_list if d not in ['The media could not be loaded.', 'Read more']]
            description = ""
            if description_list:
                description = " ".join(description_list)
                description = description.replace('\xa0', ' ')
            review["review_date"] = dt.strftime('%Y-%m-%d')
            review["description"] = description
            review["title"] = title
            review_collection.append(review)
        # next_review_page_url = selector.xpath(
        #     "//ul[contains(@class, 'pagination')]/li[contains(@class, 'a-last')]/a/@href").extract_first()
        # if first_hit and next_review_page_url:
        #     url_parts = list(urlparse(next_review_page_url))
        #     query = dict(parse_qsl(url_parts[4]))
        #     if query.get('pageNumber') is None:
        #         next_review_page_url = query.get('openid.return_to')
        #     new_msg_dict = msg_dict.copy()
        #     next_review_page_url = self.formatter.get("pdp_url").format(next_review_page_url)
        #     new_msg_dict["forced_url"] = next_review_page_url
        #     new_msg_dict["first_hit"] = False
        #     new_msg_dict["current_retry"] = 0
        #     new_msg_dict["extra"] = {"review_collection": review_collection}
        #     self.push_message_in_queue(message_body=new_msg_dict)
        # else:
        review_item = self.populate_review_item(response, msg_dict=msg_dict, reviews=review_collection)
        yield review_item
        self.delete_sqs_message(message)

    def product_parser(self, response: Response, **kwargs) -> Union[Dict, None]:
        msg_dict: dict = response.meta.get("msg_dict", {})
        try:
            message = response.meta.get("message", None)
            extra = response.meta.get("extra", True)
            first_hit = response.meta.get("first_hit", True)
            accepted_retries = 8
            # response_json = json.loads(response.text)
            # if response_json.get("status") == 404 and not first_hit:
            #     yield self.populate_product_item(response=response, details=extra.get("pd"), msg_dict=msg_dict)
            #     self.delete_sqs_message(message)
            #     return
            if (not msg_dict.get(
                    "sku") and first_hit) or '/music/player' in response.url or 'Prime Video (streaming online video)' in response.text:
                self.logger.warning("Product <%(url)s> no longer available", {"url": response.url})
                self.delete_sqs_message(message)
                return
            # browser_res = self.j_extract_first(response_json, "response")
            if first_hit:
                selector = Selector(text=response.text)
                availability_text = "".join(selector.xpath("//div[@id='availability']//text()").extract()).lower()
                if "available from these sellers" in availability_text or \
                        "temporarily out of stock" in availability_text or selector.xpath("//div[@id='outOfStock']"):
                    self.logger.warning("Product <%(url)s> Out of stock", {"url": response.url})
                    self.delete_sqs_message(message)
                    return
                style_attributes = {}
                for attr in selector.xpath("//div[@data-feature-name='productOverview']//tr"):
                    style_attributes[" ".join([
                        x.strip() for x in attr.xpath("./td[1]//text()").extract() if x.strip()])] = [
                        " ".join([x.encode('ascii', 'ignore').decode().strip() for x in attr.xpath(
                            "./td[2]//text()").extract() if x.strip()])]
                for attr in selector.xpath(
                        "//div[@id='detail_bullets_id']//div[@class='content']//li"
                ) or selector.xpath(
                    "//div[@id='detailBulletsWrapper_feature_div']//ul[contains(@class, 'detail-bullet-list')]/li"
                ) or selector.xpath("//div[@id='detailBullets_feature_div']//li") or []:
                    key = attr.xpath("./b//text()").extract_first() or attr.xpath(
                        ".//span[contains(@class, 'a-text-bold')]//text()").extract_first()
                    if key:
                        style_attributes[key.encode('ascii', 'ignore').decode().replace(':', '').strip()] = [" ".join([
                            v.strip() for v in
                            attr.xpath(".//descendant::text()[not(parent::style) and not(parent::script)"
                                       " and not(parent::span[contains(@class, 'a-text-bold')])]").extract()
                            if v.strip()])]
                if not (selector.xpath(
                        "//div[@id='prodDetails']//table[contains(@id, 'productDetails')]//tr") or selector.xpath(
                    "//ul[contains(@class, 'a-unordered-list a-nostyle a-vertical a-spacing-none detail-bullet-list')]/li") or selector.xpath(
                    "//table[contains(@class, 'a-keyvalue prodDetTable')]//tr") or selector.xpath(
                    "//table[contains(@class, 'a-bordered')]//tr") or selector.xpath(
                    "//div[@id='tech']//tr") or selector.xpath(
                    "//div[contains(@id,'audibleProductDetails')]//tr")) and msg_dict.get("current_retry",
                                                                                          0) < accepted_retries:
                    self.logger.warning("Retrying for attributes <%(url)s> ", {"url": response.url})
                    msg_dict['current_retry'] = msg_dict.get("current_retry", 0) + 1
                    msg_dict['total_retries'] = accepted_retries + 1
                    self.push_message_in_queue(message_body=msg_dict)
                    self.delete_sqs_message(message)
                    return
                if not (selector.xpath(
                        "//div[@id='prodDetails']//table[contains(@id, 'productDetails')]//tr") or selector.xpath(
                    "//ul[contains(@class, 'a-unordered-list a-nostyle a-vertical a-spacing-none detail-bullet-list')]/li") or selector.xpath(
                    "//table[contains(@class, 'a-keyvalue prodDetTable')]//tr") or selector.xpath(
                    "//table[contains(@class, 'a-bordered')]//tr") or selector.xpath(
                    "//div[@id='tech']//tr") or selector.xpath(
                    "//div[contains(@id,'audibleProductDetails')]//tr")) and msg_dict.get("current_retry",
                                                                                          0) >= accepted_retries:
                    webhook_url = "https://chat.googleapis.com/v1/spaces/AAAAzl84RbI/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=YrTDVMIYjrMtUP_50F_f9mzn6X-7dql5EzjfV1_mypI"
                    app_message = {
                        "text": f"product_url:- {msg_dict.get('pdp_url')}\n"
                                f"style attributes not found",
                    }
                    message_headers = {"Content-Type": "application/json; charset=UTF-8"}
                    requests.request(
                        url=webhook_url,
                        method="POST",
                        headers=message_headers,
                        data=json.dumps(app_message),
                    )
                for attr in selector.xpath(
                        "//div[@id='prodDetails']//table[contains(@id, 'productDetails')]//tr") or selector.xpath(
                    "//table[contains(@class, 'a-keyvalue prodDetTable')]//tr"):
                    style_attributes[attr.xpath(
                        "./th[contains(@class, 'prodDetSectionEntry')]//text()").extract_first().strip()
                    ] = [" ".join([x.encode('ascii', 'ignore').decode().strip() for x in attr.xpath(
                        "./td//descendant::text()[not(parent::style) and not(parent::script)]"
                    ).extract() if x.strip()])]
                for attr in selector.xpath("//div[@class='a-section a-spacing-small a-spacing-top-small']//div"):
                    if attr.xpath(".//span[contains(@class,'text-bold')]/text()").extract_first() is not None:
                        style_attributes[
                            attr.xpath(".//span[contains(@class,'text-bold')]/text()").extract_first().strip().replace(
                                ":", "") or attr.xpath(
                                ".//span[@class='a-size-base a-text-bold']//text()").extract_first().strip().replace(
                                ":", "")
                            ] = [" ".join([
                            x.encode('ascii', 'ignore').decode().strip() for x in
                            attr.xpath(".//span[@class='a-size-base po-break-word']//text()").extract()
                            if x.strip()
                        ])]
                for attr in selector.xpath("//div[@id='tech']//tr"):
                    style_attributes[" ".join([
                        x.strip() for x in attr.xpath("./td[@style]//text()").extract() if x.strip()])] = [
                        " ".join([x.encode('ascii', 'ignore').decode().strip() for x in attr.xpath(
                            "./td[not(@style)]//text()").extract() if x.strip()])]
                for attr in selector.xpath("//div[contains(@id,'audibleProductDetails')]//tr"):
                    style_attributes[attr.xpath(
                        "./th[contains(@class, 'a-color-secondary')]/span//text()").extract_first() or attr.xpath(
                        "./th[contains(@class, 'prodDetSectionEntry')]//text()").extract_first()
                                     ] = [" ".join([x.encode('ascii', 'ignore').decode().strip() for x in attr.xpath(
                        "./td//descendant::text()[not(parent::style) and not(parent::script)]"
                    ).extract() if x.strip()])]
                for attr in selector.xpath(
                        "//div[@id = 'twisterContainer']//div[contains(@class, 'a-section a-spacing-small')]"):
                    key_name = attr.xpath(".//label[contains(@class, 'a-form-label')]//text()").extract_first()
                    val = attr.xpath(".//span[contains(@class, 'selection')]//text()").extract()
                    if key_name and val:
                        key_name = key_name.encode('ascii', 'ignore').decode().replace(':', '').strip()
                        style_attributes[key_name] = [
                            " ".join([x.encode('ascii', 'ignore').decode().strip() for x in val if x.strip()])]
                image_text = selector.xpath("//script[contains(text(), 'ImageBlockATF')]/text()").extract_first()
                if image_text:
                    image_json = re.findall(r"colorImage.*initial': (.*)}", image_text)
                    image_json = json.loads(image_json[0]) if image_json else {}
                    pdp_images = [x['hiRes'] or x['large'] for x in image_json]
                    feature_image = self.j_extract_first(
                        image_json, "$..[?(@.variant=='MAIN')].hiRes") or self.j_extract_first(
                        image_json, "$..[?(@.variant=='MAIN')].large")
                else:
                    pdp_images = selector.xpath("//div[@class='a-immersive-image-wrapper']//div[@data-a-image-name="
                                                "'immersiveViewMainImage']/@data-a-hires").extract()
                    feature_image = pdp_images[0] if pdp_images else ''
                rating = selector.xpath("//span[@id='acrPopover']/@title").extract_first() or selector.xpath(
                    "//span[@data-hook='average-stars-rating-text']//text()").extract_first()
                rating_count = selector.xpath(
                    "//span[@id='acrCustomerReviewText']/text()").extract_first() or selector.xpath(
                    "//a[@id='acrCustomerReviewLink']//span//text()").extract_first()
                pd = {
                    "feature_image": re.sub("images.+images", "images", feature_image),
                    "pdp_images": [re.sub("images.+images", "images", image) for image in pdp_images],
                    "pdp_url": msg_dict.get("pdp_url"),
                    "sku": msg_dict.get("sku"),
                }
                pd = self.populate_product_x_path(selector, pd, self.paths.get("product", {}))
                if not pd["pdp_images"]:
                    self.logger.warning("Product not available <%(url)s>", {"url": msg_dict.get("pdp_url")})
                    self.delete_sqs_message(message)
                    return
                s_a = selector.xpath("//span[contains(@class, 'price-whole')]//text()").extract_first()
                s_b = selector.xpath("//span[contains(@class, 'price-fraction')]//text()").extract_first()
                if not pd["selling_price"] and not pd["mrp"] and s_a is not None and s_b is not None:
                    pd["selling_price"] = pd["mrp"] = self.format_price(s_a + '.' + s_b)
                if not pd["mrp"] or not pd["selling_price"]:
                    if not pd["selling_price"] and pd["mrp"]:
                        pd["selling_price"] = pd["selling_price"] or pd["mrp"]
                    if not pd["mrp"] and pd["selling_price"]:
                        pd["mrp"] = pd["mrp"] or pd["selling_price"]
                    if not pd["mrp"] and not pd["selling_price"]:
                        self.logger.warning("Product not available <%(url)s>", {"url": msg_dict.get("pdp_url")})
                        self.delete_sqs_message(message)
                        return
                pd['style_attributes'] = style_attributes
                if rating and rating_count:
                    pd["rating"] = float(re.findall(r'\d+\.*\d* ', rating)[0])
                    pd["rating_count"] = int(re.findall(r'\d+', rating_count.replace(',', '').strip())[0])
                pd["brand"] = pd["brand"].replace("Brand:", "")
                if not pd["product_name"]:
                    pd["product_name"] = selector.xpath("//h1[@id='title']//text()").extract_first() or selector.xpath(
                        "//div[contains(@id, 'title_feature_div')]//span[contains(@id, 'title')]/text()").extract_first()
                    if pd["product_name"]:
                        pd["product_name"] = pd["product_name"].strip()
                price = selector.xpath(
                    "//div[@id='olp_feature_div']//span[@class='a-size-base a-color-price']//text()").extract_first()
                if not pd["mrp"]:
                    if price:
                        pd["mrp"] = self.format_price(price)
                        pd["selling_price"] = pd["mrp"]
                    else:
                        self.logger.warning("Product not available or sold through different sellers <%(url)s>",
                                            {"url": extra.get("pdp_url")})
                        self.delete_sqs_message(message)
                        return
                # if pd['color'] == '':
                #     for x in pd['feature_list']:
                #         x = x.strip()
                #         if 'Color' in x or 'Colour' in x and pd['color'] == '':
                #             pd['color'] = (re.findall(r'Colou?r.*?:[ ,-]?(.*?)[;,]', x)
                #                            or re.findall(r'Colou?r.*?:[ ,-]?(.*)', x) or [''])[0]
                #             [j.update({'color': pd['color']}) for j in pd['variants']]
                if pd['color'] == '':
                    pd['color'] = (
                            style_attributes.get('Color') or style_attributes.get('Colour', [''])
                    )[0]
                if not pd["brand"]:
                    pd["brand"] = selector.xpath("//a[contains(@id, 'bylineInfo')]/text()").extract_first()
                if not pd['brand']:
                    if pd['style_attributes'] and pd['style_attributes'].get('Brand'):
                        pd['brand'] = pd['style_attributes'].get('Brand')[0]
                if not pd["brand"]:
                    pd["brand"] = "Not-Branded"
                if ":" in pd["brand"]:
                    brand = pd['brand'].split(":")
                    pd["brand"] = brand[-1]
                if "Visit" in pd["brand"]:
                    brand = re.findall("Visit the (.*) Store", pd["brand"])[0]
                    pd["brand"] = brand
                pd['breadcrumbs'] = [x.strip() for x in pd['breadcrumbs'] if x != '›' and x.strip()]
                pd['feature_list'] = [x.strip() for x in pd['feature_list'] if x.strip()]
                if not pd["product_name"]:
                    self.logger.warning("Retrying for product name <%(url)s>", {"url": response.url})
                    self.push_message_in_queue_for_retry(message)
                    return
                if pd["pdp_images"]:
                    pd["pdp_images"] = sorted(set(pd["pdp_images"]))
                if self.formatter.get("related_media_needed", False):
                    pd["related_media"] = []
                    video_links = []
                    video_data = selector.xpath(
                        "//script[contains(text(), 'triggerVideoAjax')]//text()").extract_first()
                    if video_data:
                        video_json_data = re.findall(r"parseJSON\('({.*})", video_data)
                        if video_json_data:
                            video_json = json.loads(self.fix_json_from_string(video_json_data[0]))
                            video_links.extend(self.j_extract(video_json, "videos[?(@.groupType=='IB_G1')].url", []))
                            video_links.extend(self.j_extract(video_json, "videos[?(@.groupType=='IB_G2')].url", []))
                            video_links.extend(self.j_extract(video_json, "videos[?(@.groupType=='IB_G2_T1')].url", []))
                    video_data2 = selector.xpath("//script[contains(text(), 'ImageBlockBTF')]//text()").extract_first()
                    if video_data2:
                        video_json_data = re.findall(r"parseJSON\('({.*})", video_data2)
                        if video_json_data:
                            video_json = json.loads(self.fix_json_from_string(video_json_data[0]))
                            video_links.extend(self.j_extract(video_json, "videos[?(@.groupType=='IB_G1')].url", []))
                            video_links.extend(self.j_extract(video_json, "videos[?(@.groupType=='IB_G2')].url", []))
                            video_links.extend(self.j_extract(video_json, "videos[?(@.groupType=='IB_G2_T1')].url", []))
                    if video_links:
                        pd["related_media"].append({
                            'Video_Links': [{"product_videos": video_links}]
                        })
                    if selector.xpath("//script[contains(text(), 'spriteURLs')]//text()").extract_first():
                        pd["related_media"].append({
                            '360_Images': [True]
                        })
                    pdf_links = []
                    for pdf in selector.xpath("//div[contains(@id, 'productDocuments')]//a", []):
                        key = pdf.xpath(".//text()").extract_first(default="").replace(' ', '_').replace('(',
                                                                                                         "").replace(
                            ')',
                            "")
                        value = pdf.xpath(".//@href").extract()
                        if key and value:
                            pdf_links.append({
                                key.lower(): value
                            })
                    if pdf_links:
                        pd["related_media"].append({"PDF": pdf_links})
                pd['variants'] = [{
                    'available': False if 'dropdownUnavailable' in var.xpath("./@class").extract_first() else True,
                    'color': pd['color'],
                    'size': html.unescape(var.xpath("./@data-a-html-content").extract_first()),
                    'stock': None,
                    'variant_sku': re.sub(r'.*,', '', var.xpath("./@value").extract_first()) or ''
                } for var in selector.xpath("//select[@name='dropdown_selected_size_name']/option[@class]")]
                if not pd['variants']:
                    pd['variants'] = [{
                        'available': False if var.xpath(
                            ".//span[@class='a-size-small default-slot-unavailable']"
                        ).extract_first() else True,
                        'color': pd['color'],
                        'size': var.xpath(
                            ".//span[@class='a-size-base swatch-title-text-display swatch-title-text']//text()").extract_first(),
                        'stock': None,
                        'variant_sku': ''
                    } for var in selector.xpath("""//ul[@data-a-button-group='{"name":"size_name"}']//li""") if
                        var.xpath(
                            ".//span[@class='a-size-base swatch-title-text-display swatch-title-text']//text()").extract_first()]
                if not pd['variants']:
                    pd['variants'] = [{
                        'available': False if var.xpath(
                            "./span[contains(@id,'size_name') and contains(@class, 'unavailable')]"
                        ).extract_first() else True,
                        'color': pd['color'],
                        'size': var.xpath(".//label//text()").extract_first(),
                        'stock': None,
                        'variant_sku': ''
                    } for var in selector.xpath("//div[contains(@class,'inline-twister-scroller-size_name')]//ul[cont"
                                                "ains(@data-a-button-group, 'size_name')]/li[contains(@class, 'listitem"
                                                "')]/span[@class='a-list-item']")]
                if not pd['variants']:
                    pd['variants'] = [{
                        'available': True if var.xpath("./@class").extract_first() in ['swatchAvailable',
                                                                                       'swatchSelect'] else False,
                        'color': pd['color'],
                        'size': var.xpath(".//*[contains(@class,'a-size-base')]//text()").extract_first(),
                        'stock': None,
                        'mrp': pd["mrp"],
                        'selling_price': self.format_price(
                            var.xpath(".//*[contains(@class,'a-size-mini')]//text()").extract_first(
                                default=pd["selling_price"])),
                        'variant_sku': var.xpath("./@data-defaultasin").extract_first() or ''
                    } for var in selector.xpath(
                        """//ul[contains(@data-a-button-group,"size_name") or contains(@data-a-button-group, "style_name")]//li""")
                        if
                        var.xpath(".//*[contains(@class,'a-size-base')]//text()").extract_first()]
                if not pd['variants']:
                    if any([i in availability_text for i in
                            ['in stock', 'available to ship', 'usually dispatched', 'order soon',
                             'usually ships within']]) and 'unavailable' not in availability_text:
                        available = True
                    else:
                        available = False
                    pd['variants'] = [{
                        'available': available,
                        'color': pd['color'],
                        'size': selector.xpath("//div[contains(@class,'inline-twister-scroller-size_name')]//span["
                                               "contains(@id,'size_name') and contains(@class,'a-button-selected')]//"
                                               "label//text()").extract_first() or selector.xpath(
                            "//div[contains(@id,'size_name')]//span/text()").extract_first() or selector.xpath(
                            "//tr[contains(@class,'po-size')]//span[contains(@class,'break')]/text()").extract_first() or selector.xpath(
                            "//li[@class='swatchSelect'][contains(@id,'size')]//p//text()"
                        ).extract_first() or 'One Size',
                        'stock': None,
                        'variant_sku': pd['sku']
                    }]
                else:
                    pd['style_attributes'].pop('Size', None)
                # desc_list = selector.xpath("//h2[contains(text(), 'Product description')]/following::div[contains(@class, 'a-section a-spacing-small')]//span[not(contains(@class,'expander') or @class='a-dropdown-prompt')]/text()").extract() +
                # desc_list = selector.xpath("//h2[contains(text(), 'Product description')]/following::div[contains(@class, 'a-section a-spacing-small')]/span/text()").extract() + selector.xpath("//h2[contains(text(), 'Product description')]/following::div[contains(@class, 'a-section a-spacing-small')]/p/span//text()").extract()
                desc_list = selector.xpath(
                    "//h2[contains(text(), 'Product description')]/following::div[contains(@class, 'a-section a-spacing-small')]/span/text()").extract()
                if desc_list:
                    desc_list = [st.strip() for st in desc_list if st.strip()]
                    if not desc_list:
                        desc_list = selector.xpath(
                            "//h2[contains(text(), 'Product description')]/following::div[contains(@class, 'a-section a-spacing-small')]/p/span//text()").extract()
                        desc_list = [st.strip() for st in desc_list if st.strip()]
                    desc = ' '.join(desc_list)
                    if pd["description"] in ['']:
                        pd["description"] = desc
                    else:
                        pd["description"] = pd["description"] + desc
                pd["feature_list"] = [x.encode('ascii', 'ignore').decode() for x in pd['feature_list']]
                pd['breadcrumbs'] = [x.encode('ascii', 'ignore').decode() for x in pd['breadcrumbs']]
                if pd["description"]:
                    pd["description"] = pd['description'].encode('ascii', 'ignore').decode()
                pd["category_breadcrumbs"] = extra.get("category_breadcrumbs", [])
                pd["product_breadcrumbs"] = pd['breadcrumbs']
                pd['breadcrumbs'] = []
                # for care instructions
                for attr in selector.xpath("//div[contains(@class, 'product-facts-detail')]"):
                    pd["style_attributes"][attr.xpath(
                        ".//div[contains(@class, 'a-col-left')]//span[contains(@class, 'a-color-base')]//text()").extract_first().strip()] = \
                        [" ".join([
                            x.encode('ascii', 'ignore').decode().strip() for x in
                            attr.xpath(
                                ".//div[contains(@class, 'a-col-right')]//span[contains(@class, 'a-color-base')]//text()").extract()
                            if x.strip()
                        ])]
                tags_list = selector.xpath(
                    "//span[contains(@class, 'ac-badge-rectangle')]//span//text()").extract() or selector.xpath(
                    "//a[contains(@class, 'badge-link')]/i//text()").extract() or selector.xpath(
                    "//span[contains(@id, 'dealBadgeSupportingText')]/span//text()").extract()
                if tags_list:
                    tags_list = [tag.strip() for tag in tags_list if tag.strip()]
                    pd["style_attributes"]["Tag"] = tags_list
                # keys modification
                for key, value in list(pd["style_attributes"].items()):
                    # if key in ['Product dimensions', 'Package Dimensions']:
                    #     dimensions_str = value[0] if isinstance(value, list) else value
                    #     dimensions_part = dimensions_str.split(';')[0]
                    #     dimensions = dimensions_part.split('x')
                    #     if len(dimensions) == 3:
                    #         unit_val = dimensions[2].strip()
                    #         dimensions2 = unit_val.split(' ')[0]
                    #         unit_val = unit_val.split(' ')[1]
                    #         pd["style_attributes"]['product_length'] = [dimensions[0].strip() + f' {unit_val}']
                    #         pd["style_attributes"]['product_width'] = [dimensions[1].strip() + f' {unit_val}']
                    #         pd["style_attributes"]['product_height'] = [dimensions2 + f' {unit_val}']
                    # elif key == 'Dimensions':
                    #     dimensions_str = value[0] if isinstance(value, list) else value
                    #     dimensions = dimensions_str.split('x')
                    #     if len(dimensions) == 3:
                    #         unit_val = dimensions[2].strip()
                    #         dimensions2 = unit_val.split(' ')[0]
                    #         unit_val = unit_val.split(' ')[1]
                    #         pd["style_attributes"]['product_length'] = [dimensions[0].strip() + f' {unit_val}']
                    #         pd["style_attributes"]['product_width'] = [dimensions[1].strip() + f' {unit_val}']
                    #         pd["style_attributes"]['product_height'] = [dimensions2 + f' {unit_val}']
                    if key in ['Item dimensions L x W x H']:
                        dimensions_str = value[0] if isinstance(value, list) else value
                        dimensions = dimensions_str.split('x')
                        if len(dimensions) == 3:
                            unit_val = dimensions[2].strip()
                            dimensions2 = unit_val.split(' ')[0]
                            unit_val = unit_val.split(' ')[1]
                            pd["style_attributes"]['product_length'] = [dimensions[0].strip() + f' {unit_val}']
                            pd["style_attributes"]['product_width'] = [dimensions[1].strip() + f' {unit_val}']
                            pd["style_attributes"]['product_height'] = [dimensions2 + f' {unit_val}']
                    if key == 'Manufacturer Part Number':
                        pd["style_attributes"]['MPN'] = pd["style_attributes"][key]
                        pd["style_attributes"].pop(key, None)
                    if key in ['Item Model Number', 'Model Number']:
                        pd["style_attributes"]['modelNumber'] = pd["style_attributes"][key]
                        pd["style_attributes"].pop(key, None)
                review_url = selector.xpath(
                    "//a[@data-hook='see-all-reviews-link-foot']/@href").extract_first() or selector.xpath(
                    "//a[@data-hook='reviews-summary-mobile']/@href").extract_first()
                pd["category_sku"] = msg_dict.get("sku")
                ship_by = selector.xpath(
                    "//div[contains(@class,'offer-display-feature-text')][@offer-display-feature-name='desktop-fulfiller-info']//span//text()").extract_first().strip() if selector.xpath(
                    "//div[contains(@class,'offer-display-feature-text')][@offer-display-feature-name='desktop-fulfiller-info']//span//text()").extract_first() else ""
                sold_by = selector.xpath(
                    "//div[contains(@class,'offer-display-feature-text')][@offer-display-feature-name='desktop-merchant-info']//span[contains(@class, 'text-message')]/text()").extract_first().strip() if selector.xpath(
                    "//div[contains(@class,'offer-display-feature-text')][@offer-display-feature-name='desktop-merchant-info']//span[contains(@class, 'text-message')]/text()").extract_first() else ""
                if sold_by:
                    pd['style_attributes']['sold_by'] = [sold_by]
                pd['style_attributes']['free_delivery'] = ['FREE' in selector.xpath('//@data-csa-c-delivery-price').extract()]
                pd['style_attributes']['buy_box'] = [True if sold_by and ship_by else False]
                coupon =[i.strip() for i in selector.xpath("//span[@class='couponLabelText']/text()").extract() if i.strip()]
                ends_in = [i.strip() for i in selector.xpath("//span[contains(@class, 'dealBadgeTextColor')]/span/text()").extract() if i.strip()]
                promotions = coupon + ([' '.join(ends_in)] if ends_in else [])
                if promotions:
                    pd['style_attributes']['promotions'] = promotions
                if review_url:
                    new_msg_dict = msg_dict.copy()
                    new_msg_dict["forced_url"] = self.formatter.get("pdp_url").format(review_url)
                    new_msg_dict["first_hit"] = False
                    new_msg_dict["headers"] = {'referer': pd['pdp_url'],
                                               'downlink': '10',
                                               'ect': '4g',
                                               'sec-fetch-site': 'same-origin',
                                               'sec-fetch-dest': 'document',
                                               'rtt': '150'
                                               }
                    new_msg_dict["current_retry"] = 0
                    new_msg_dict["extra"] = {"pd": pd}
                    self.push_message_in_queue(message_body=new_msg_dict)
                    self.delete_sqs_message(message)
                    return
                yield self.populate_product_item(response=response, details=pd, msg_dict=msg_dict)
            else:
                pd = extra.get('pd')
                selector = Selector(text=response.text)  # Returning byte object
                review_count_text = selector.xpath(
                    "substring-after(//div[@data-hook='cr-filter-info-review-rating-count']//text()[normalize-space()]"
                    ", 'ratings')").extract_first().strip()
                # Format is 2,554 global ratings | 1,502 global reviews and 2,409 total ratings, 472 with reviews
                review_count = re.findall(r'(\d+)', review_count_text.replace(",", ''))
                pd["review_count"] = int(review_count[0]) if review_count else None
                # call Review parser for amazon au
                # self.push_product_for_reviews(
                #     pd={"pdp_url": msg_dict.get('pdp_url'), "sku": msg_dict.get("sku")},
                #     msg_dict=msg_dict)
                yield self.populate_product_item(response=response, details=pd, msg_dict=msg_dict)
            self.delete_sqs_message(message)
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            line_number = exc_traceback.tb_lineno
            self.logger.info(f"Error {e} on line No. {line_number}")

    def category_parser(self, response: Response, **kwargs) -> Union[Dict, None]:
        msg_dict: dict = response.meta.get("msg_dict", {})
        try:
            message = response.meta.get("message", None)
            current_page = response.meta.get("current_page", 1)
            rank = response.meta.get("start_rank", 0)
            category_count_mapping = {
                82: 300,
                83: 400,
                84: 350,
                85: 300,
                86: 350,
                87: 100
            }
            category_id = int(msg_dict.get('category_id', 0))
            # response_json = json.loads(response.text)
            # browser_res = self.j_extract_first(response_json, "response")
            selector = Selector(text=response.text)
            if selector.xpath("//h3//span[contains(text(), 'No results')]").extract_first():
                self.logger.warning("Deleting <%(url)s> No Results Found", {"url": response.url})
                self.delete_sqs_message(message)
                return

            if 'See all results' in response.text:
                category_url = selector.xpath(
                    "//div[@class='a-box a-text-center apb-browse-searchresults-footer']//a[@class='a-link-normal']/@href"
                ).extract_first() or selector.xpath(
                    "//div[contains(@class,'a-cardui _octopus-search-result-card')]//a[@class='a-link-normal']/@href"
                ).extract_first()
                category_url = self.formatter.get("pdp_url").format(category_url)
                msg_dict["forced_url"] = category_url
                self.push_message_in_queue(message_body=msg_dict)
                self.delete_sqs_message(message)
                return
            products = self.get_product_collection_x_path(selector, crawl_type="category")
            if len(products) == 0 and msg_dict.get("current_retry", 0) < 3:
                products = selector.xpath("//div[@id='mainResults']//li")
                if not products:
                    self.logger.warning("Retrying <%(url)s> No Products Found", {"url": response.url})
                    self.push_message_in_queue_for_retry(message)
                    return
            if len(products) == 0 and msg_dict.get("current_retry", 0) >= 3:
                self.push_message_in_dead_letter_queue(msg_dict)
                self.delete_sqs_message(message)
                return
            msg_dict.pop("forced_url", None)
            msg_dict["current_page"] = current_page
            product_collection = []
            for p in products:
                if 'sponsored' in "".join(p.xpath('.//text()').extract()).lower():
                    continue
                sku_val = p.xpath('./@data-asin').extract_first()
                if not sku_val:
                    continue

                rank += 1
                pd = self.populate_product_details_x_path(p, {
                    "rank": rank,
                }, self.paths.get("category").get("product_details", {}))
                # if 795 <= category_id <= 1646:
                #     url_parts = urlparse(msg_dict.get('url'))
                #     query_dict = parse_qs(url_parts.query)
                #     brand = query_dict.get('k', [''])[0]
                #
                #     pdp_url_parts_path = urlparse(pd['pdp_url']).path
                #
                #     product_name = p.xpath('.//div[@data-cy="title-recipe"]/a//text()').extract_first()
                #     brand_name = p.xpath('.//div[@data-cy="title-recipe"]/div/h2//text()').extract_first()
                #     author = p.xpath(
                #         ".//div[@data-cy='title-recipe']/div[contains(@class,'a-size-base ')]//span//text()").extract()
                #     if author:
                #         author = ' '.join(author)
                #     # Common non-brand stopwords
                #     STOPWORDS = {
                #         "the", "and", "of", "co", "ltd", "inc", "group", "llc",
                #         "company", "pty", "plc", "corp", "limited", "entertain"
                #     }
                #
                #     def tokenize(text: str):
                #         """Split text into lowercase alphanumeric tokens."""
                #         if not isinstance(text, str):
                #             return set()
                #         return {
                #             token for token in re.split(r'[^0-9a-zA-Z]+', text.lower())
                #             if token
                #         }
                #
                #     def clean_tokens(tokens):
                #         """Remove stopwords and single-letter tokens."""
                #         return {t for t in tokens if t not in STOPWORDS and len(t) > 1}
                #
                #     def normalize_brand(brand: str):
                #         tokens = tokenize(brand)
                #         return clean_tokens(tokens)
                #
                #     def brand_in_record(brand, record_fields):
                #         """
                #         Two-stage brand match without dictionary/synonyms.
                #         1. Token overlap check
                #         2. Full brand string substring check
                #         """
                #         # Normalize brand into tokens
                #         brand_tokens = normalize_brand(brand)
                #
                #         # Combine all record fields
                #         text = " ".join([str(field).lower() for field in record_fields if field])
                #         text_tokens = normalize_brand(text)
                #
                #         # Stage 1: token overlap
                #         if not brand_tokens.isdisjoint(text_tokens):
                #             return brand
                #
                #         # Stage 2: full brand substring check
                #         norm_brand = brand.lower().strip()
                #         if norm_brand and norm_brand in text:
                #             return brand
                #
                #         return None
                #
                #     if not brand_in_record(brand, [brand_name, product_name, author, pdp_url_parts_path]):
                #         rank -= 1
                #         self.logger.warning("Required brand not match with product brand<%(url)s>", {"url": pd['pdp_url']})
                #         continue

                # product_pdp_urls = [
                #     self.formatter.get("pdp_url").format(f"/gp/product/{pd['sku']}"),
                #     self.formatter.get("pdp_url").format(f"/dp/{pd['sku']}?th=1&psc=1"),
                #     self.formatter.get("pdp_url").format(f"/dp/{pd['sku']}"),
                #     self.formatter.get("pdp_url").format(f"/dp/product/{pd['sku']}")
                # ]
                # pd['pdp_url'] = random.choices(product_pdp_urls)[0]
                # pd['pdp_url'] = self.formatter.get("pdp_url").format(f"/dp/{pd['sku']}?th=1&psc=1")
                category_breadcrumbs = selector.xpath(
                    "//div[contains(@id, 'departments')]//li[contains(@class, 'a-spacing-micro')]//span[contains(@class, 's-back-arrow')]/following::span[1]/text()").extract()
                if selector.xpath(
                        "//div[contains(@id, 'departments')]//li[contains(@class, 'a-spacing-micro')]//span[contains(@class, 'a-size-base a-color-base a-text-bold')]/text()").extract_first():
                    category_breadcrumbs.append(selector.xpath(
                        "//div[contains(@id, 'departments')]//li[contains(@class, 'a-spacing-micro')]//span[contains(@class, 'a-size-base a-color-base a-text-bold')]/text()").extract_first())
                category_breadcrumbs = [b.strip() for b in category_breadcrumbs]
                category_breadcrumbs = [b for b in category_breadcrumbs if 'Department' not in b]
                extra = {"category_breadcrumbs": category_breadcrumbs}
                pd["category_breadcrumbs"] = extra.get('category_breadcrumbs')
                if not pd["pdp_url"]:
                    self.logger.warning("Retrying <%(url)s> Proper Data Not Got", {"url": response.url})
                    self.push_message_in_queue_for_retry(message)
                    return
                self.push_ranked_product_to_crawl(
                    pd=pd,
                    msg_dict=msg_dict,
                    extra=extra
                )
                # self.push_product_for_reviews(
                #     pd=pd,
                #     msg_dict=msg_dict,
                #     extra=extra
                # )
                product_collection.append(pd)
                if category_count_mapping.get(category_id) and rank >= category_count_mapping.get(category_id):
                    break
            if current_page == 1:
                msg_dict["page_size"] = rank
                msg_dict["pages_to_crawl"] = current_page
            category_item = self.populate_category_item(response, msg_dict, products=product_collection)
            yield category_item
            next_page_url = selector.xpath("//li[@class='a-last']/a/@href").extract_first() or selector.xpath(
                "//div[contains(@class, 's-pagination-container')]//a[contains(@class,'s-pagination-next')]/@href"
            ).extract_first() or selector.xpath("//a[@id='pagnNextLink']/@href").extract_first()
            if not category_count_mapping.get(category_id) or (
                    category_count_mapping.get(category_id) and rank < category_count_mapping.get(category_id)):
                if next_page_url is not None:
                    msg_dict["forced_url"] = self.formatter.get("pdp_url").format(next_page_url)
                    msg_dict["start_rank"] = rank
                    msg_dict["current_retry"] = 0
                    msg_dict["current_page"] = current_page + 1
                    msg_dict["pages_to_crawl"] = current_page + 1
                    self.push_next_page_to_crawl(new_msg_dict=msg_dict)
            self.delete_sqs_message(message)
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            line_number = exc_traceback.tb_lineno
            self.logger.info(f"Error {e} on line No. {line_number}")


class AmazonIN(Amazon):
    """
    Amazon IN
    """

    def get_cookies(self, msg_dict) -> Dict:
        return {
            'i18n-prefs': 'INR',
        }

    name = "amazon-in"
    formatter = {"pdp_url": "https://www.amazon.in{}", "related_media_needed": True}

    @staticmethod
    def format_price(text: (str, int, float)):
        try:
            if isinstance(text, (int, float)):
                return text
            text = text.strip()
            text = text.replace(",", "")
            if text.strip():
                return float(re.sub(r"[^0-9.}]+", "", text))
        except Exception as err:
            print(err)
