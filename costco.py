"""
Costco
"""
import json
import math
import re
import sys
import os
from typing import Dict, Union
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse, quote
from scrapy import Selector
from scrapy.http import Response
import copy

from engine.spiders.retailers.base import Retailer


class CostcoUS(Retailer):
    """
    CostcoUS
    """
    
    pdp_url = True
    proxy_enabled = True
    page_size = 24
    name = "costco-us"
    formatter = {
        "pdp_url": "https://www.costco.com/p/-/{}/{}",
    }
    user_pass = f"{os.getenv('PRIVATE_PROXY_USERNAME')}:{os.getenv('PRIVATE_PROXY_PASSWORD').replace('@', '%40')}"
    proxy = os.getenv("US_PROXY_SERVER").replace("http://", "")
    proxies = {"http://": f"http://{user_pass}@{proxy}",
               "https://": f"http://{user_pass}@{proxy}"}
    locale_map = {
        # nearest warehouse
        122: {'pincode': '77304', 'warehouse': '1189-wh',
              'WH_header': '1254-3pl%2C1321-wm%2C1470-3pl%2C283-wm%2C561-wm%2C725-wm%2C731-wm%2C758-wm%2C759-wm%2C847_0-cor%2C847_0-cwt%2C847_0-edi%2C847_0-ehs%2C847_0-membership%2C847_0-mpt%2C847_0-spc%2C847_0-wm%2C847_1-edi%2C847_d-fis%2C847_lg_ntx-edi%2C847_NA-cor%2C847_NA-pharmacy%2C847_NA-wm%2C847_ss_u359-edi%2C847_wp_r455-edi%2C951-wm%2C952-wm%2C9847-wcs%2C655-bd'},
        123: {'pincode': '30260', 'warehouse': '1084-wh',
              'WH_header': '1258-3pl%2C1321-wm%2C1473-3pl%2C283-wm%2C561-wm%2C725-wm%2C729-dz%2C731-wm%2C758-wm%2C759-wm%2C847_0-cor%2C847_0-cwt%2C847_0-edi%2C847_0-ehs%2C847_0-membership%2C847_0-mpt%2C847_0-spc%2C847_0-wm%2C847_1-edi%2C847_d-fis%2C847_lg_n1a-edi%2C847_NA-cor%2C847_NA-pharmacy%2C847_NA-wm%2C847_ss_u367-edi%2C847_wp_r453-edi%2C951-wm%2C952-wm%2C9847-wcs%2C579-bd%2C651-bd'},
        124: {'pincode': '32547', 'warehouse': '1084-wh',
              'WH_header': '1258-3pl%2C1321-wm%2C1473-3pl%2C283-wm%2C561-wm%2C725-wm%2C729-dz%2C731-wm%2C758-wm%2C759-wm%2C847_0-cor%2C847_0-cwt%2C847_0-edi%2C847_0-ehs%2C847_0-membership%2C847_0-mpt%2C847_0-spc%2C847_0-wm%2C847_1-edi%2C847_d-fis%2C847_lg_n1a-edi%2C847_NA-cor%2C847_NA-pharmacy%2C847_NA-wm%2C847_ss_u367-edi%2C847_wp_r453-edi%2C951-wm%2C952-wm%2C9847-wcs%2C579-bd%2C651-bd'},
        59: {'pincode': '96797', 'warehouse': '485-wh',
             'WH_header': '1252-3pl%2C1321-wm%2C1409-3pl%2C283-wm%2C561-wm%2C725-wm%2C731-wm%2C758-wm%2C759-wm%2C847_0-cor%2C847_0-cwt%2C847_0-edi%2C847_0-ehs%2C847_0-membership%2C847_0-mpt%2C847_0-spc%2C847_0-wm%2C847_1-edi%2C847_d-fis%2C847_lg_n1f-edi%2C847_NA-cor%2C847_NA-pharmacy%2C847_NA-wm%2C847_ss_u357-edi%2C847_wp_r461-edi%2C951-wm%2C952-wm%2C9847-wcs'},
        125: {'pincode': '94931', 'warehouse': '659-wh',
              'WH_header': '1251-3pl%2C1321-wm%2C1480-3pl%2C283-wm%2C561-wm%2C653-dz%2C725-wm%2C731-wm%2C758-wm%2C759-wm%2C847_0-cor%2C847_0-cwt%2C847_0-edi%2C847_0-ehs%2C847_0-membership%2C847_0-mpt%2C847_0-spc%2C847_0-wm%2C847_1-edi%2C847_d-fis%2C847_lg_n1f-edi%2C847_NA-cor%2C847_NA-pharmacy%2C847_NA-wm%2C847_ss_u357-edi%2C847_wp_r460-edi%2C951-wm%2C952-wm%2C9847-wcs%2C653-bd%2C893-bd'},
        126: {'pincode': '97222', 'warehouse': '97-wh',
              'WH_header': '1250-3pl%2C1321-wm%2C1455-3pl%2C283-wm%2C561-wm%2C653-dz%2C725-wm%2C731-wm%2C758-wm%2C759-wm%2C847_0-cor%2C847_0-cwt%2C847_0-edi%2C847_0-ehs%2C847_0-membership%2C847_0-mpt%2C847_0-spc%2C847_0-wm%2C847_1-edi%2C847_d-fis%2C847_lg_n1f-edi%2C847_NA-cor%2C847_NA-pharmacy%2C847_NA-wm%2C847_ss_u362-edi%2C847_wp_r458-edi%2C951-wm%2C952-wm%2C9847-wcs%2C115-bd'},
        127: {'pincode': '81505', 'warehouse': '637-wh',
              'WH_header': '1321-wm%2C283-wm%2C561-wm%2C653-dz%2C725-wm%2C731-wm%2C758-wm%2C759-wm%2C847_0-cor%2C847_0-cwt%2C847_0-edi%2C847_0-ehs%2C847_0-membership%2C847_0-mpt%2C847_0-spc%2C847_0-wm%2C847_1-edi%2C847_d-fis%2C847_lg_ntx-edi%2C847_NA-cor%2C847_NA-pharmacy%2C847_NA-wm%2C951-wm%2C952-wm%2C9847-wcs%2C563-bd'},
        128: {'pincode': '20716', 'warehouse': '1078-wh',
              'WH_header': '1260-3pl%2C1321-wm%2C1459-3pl%2C283-wm%2C561-wm%2C725-wm%2C729-dz%2C731-wm%2C758-wm%2C759-wm%2C847_0-cor%2C847_0-cwt%2C847_0-edi%2C847_0-ehs%2C847_0-membership%2C847_0-mpt%2C847_0-spc%2C847_0-wm%2C847_1-edi%2C847_d-fis%2C847_lg_n1a-edi%2C847_NA-cor%2C847_NA-pharmacy%2C847_NA-wm%2C847_ss_u358-edi%2C847_wp_r451-edi%2C951-wm%2C952-wm%2C9847-wcs%2C729-bd'},
        129: {'pincode': '22802', 'warehouse': '1078-wh',
              'WH_header': '1260-3pl%2C1321-wm%2C1459-3pl%2C283-wm%2C561-wm%2C725-wm%2C729-dz%2C731-wm%2C758-wm%2C759-wm%2C847_0-cor%2C847_0-cwt%2C847_0-edi%2C847_0-ehs%2C847_0-membership%2C847_0-mpt%2C847_0-spc%2C847_0-wm%2C847_1-edi%2C847_d-fis%2C847_lg_n1a-edi%2C847_NA-cor%2C847_NA-pharmacy%2C847_NA-wm%2C847_ss_u358-edi%2C847_wp_r451-edi%2C951-wm%2C952-wm%2C9847-wcs%2C729-bd'},
        130: {'pincode': '47802', 'warehouse': '1577-wh',
              'WH_header': '1257-3pl%2C1321-wm%2C1503-3pl%2C283-wm%2C561-wm%2C725-wm%2C729-dz%2C731-wm%2C758-wm%2C759-wm%2C847_0-cor%2C847_0-cwt%2C847_0-edi%2C847_0-ehs%2C847_0-membership%2C847_0-mpt%2C847_0-spc%2C847_0-wm%2C847_1-edi%2C847_d-fis%2C847_lg_n1a-edi%2C847_NA-cor%2C847_NA-pharmacy%2C847_NA-wm%2C847_ss_u358-edi%2C847_wp_r451-edi%2C951-wm%2C952-wm%2C9847-wcs%2C580-bd'},
    }

    locale_map2 = {1: {'zipcode': '95829', 'stateCode': 'CA', 'distributionCenters': ['1251-3pl', '1321-wm', '1479-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_aa_00-spc', '847_aa_u610-edi', '847_d-fis', '847_lg_n1f-edi', '847_lux_us51-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u357-edi', '847_wp_r460-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '464-wh', 'groceryCenters': ['653-bd', '893-bd']}, 2: {'zipcode': '90019', 'stateCode': 'CA', 'distributionCenters': ['1252-3pl', '1321-wm', '1462-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_aa_00-spc', '847_aa_u610-edi', '847_d-fis', '847_lg_n1f-edi', '847_lux_us51-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u357-edi', '847_wp_r461-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '130-wh', 'groceryCenters': ['653-bd']}, 3: {'zipcode': '33761', 'stateCode': 'FL', 'distributionCenters': ['1259-3pl', '1321-wm', '1570-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1a-edi', '847_lux_us71-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u366-edi', '847_wp_r422-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '336-wh', 'groceryCenters': ['651-bd']}, 4: {'zipcode': '11215', 'stateCode': 'NY', 'distributionCenters': ['1260-3pl', '1321-wm', '1474-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1a-edi', '847_lux_us81-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u360-edi', '847_wp_r428-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '318-wh', 'groceryCenters': ['729-bd']}, 5: {'zipcode': '77008', 'stateCode': 'TX', 'distributionCenters': ['1254-3pl', '1321-wm', '1470-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_ntx-edi', '847_lux_us11-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u359-edi', '847_wp_r455-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '1018-wh', 'groceryCenters': ['655-bd']}, 6: {'zipcode': '63111', 'stateCode': 'MO', 'distributionCenters': ['1255-3pl', '1321-wm', '1524-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1a-edi', '847_lux_us41-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u358-edi', '847_wp_r452-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '368-wh', 'groceryCenters': ['1665-bd']}, 7: {'zipcode': '26301', 'stateCode': 'WV', 'distributionCenters': ['1257-3pl', '1321-wm', '1502-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1a-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u358-edi', '847_wp_r451-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '649-wh', 'groceryCenters': ['1663-bd']}, 8: {'zipcode': '95928', 'stateCode': 'CA', 'distributionCenters': ['1251-3pl', '1321-wm', '1479-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_aa_00-spc', '847_aa_u610-edi', '847_d-fis', '847_lg_n1f-edi', '847_lux_us51-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u357-edi', '847_wp_r460-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '1011-wh', 'groceryCenters': ['653-bd', '893-bd']}, 9: {'zipcode': '36301', 'stateCode': 'AL', 'distributionCenters': ['1258-3pl', '1321-wm', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1a-edi', '847_lux_us31-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '1026-wh', 'groceryCenters': ['579-bd']}, 10: {'zipcode': '17331', 'stateCode': 'PA', 'distributionCenters': ['1260-3pl', '1321-wm', '1548-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1a-edi', '847_lux_us81-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u360-edi', '847_wp_r428-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '1326-wh', 'groceryCenters': ['729-bd']}, 11: {'zipcode': '81008', 'stateCode': 'CO', 'distributionCenters': ['1252-3pl', '1253-3pl', '1321-wm', '1483-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_ntx-edi', '847_lux_us21-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u359-edi', '847_wp_r457-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '1014-wh', 'groceryCenters': ['563-bd']}, 12: {'zipcode': '79912', 'stateCode': 'TX', 'distributionCenters': ['1254-3pl', '1321-wm', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_ntx-edi', '847_lux_us11-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u359-edi', '847_wp_r455-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '768-wh', 'groceryCenters': ['655-bd']}, 13: {'zipcode': '99654', 'stateCode': 'AK', 'distributionCenters': ['1250-3pl', '1321-wm', '1408-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1f-edi', '847_lux_us01-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u362-edi', '847_wp_r458-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '1661-wh', 'groceryCenters': ['1661-bd']}, 14: {'zipcode': '48162', 'stateCode': 'MI', 'distributionCenters': ['1257-3pl', '1321-wm', '1460-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1a-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u358-edi', '847_wp_r451-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '1007-wh', 'groceryCenters': ['1663-bd']}, 15: {'zipcode': '43701', 'stateCode': 'OH', 'distributionCenters': ['1257-3pl', '1321-wm', '1498-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1a-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u358-edi', '847_wp_r451-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '1160-wh', 'groceryCenters': ['1663-bd']}, 16: {'zipcode': '19083', 'stateCode': 'PA', 'distributionCenters': ['1260-3pl', '1321-wm', '1476-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1a-edi', '847_lux_us81-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u360-edi', '847_wp_r428-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '245-wh', 'groceryCenters': ['729-bd']}, 17: {'zipcode': '28203', 'stateCode': 'NC', 'distributionCenters': ['1258-3pl', '1321-wm', '1538-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1a-edi', '847_lux_us31-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u367-edi', '847_wp_r453-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '359-wh', 'groceryCenters': ['579-bd']}, 18: {'zipcode': '30307', 'stateCode': 'GA', 'distributionCenters': ['1258-3pl', '1321-wm', '1473-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1a-edi', '847_lux_us31-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u367-edi', '847_wp_r453-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '1084-wh', 'groceryCenters': ['579-bd']}, 19: {'zipcode': '33713', 'stateCode': 'FL', 'distributionCenters': ['1259-3pl', '1321-wm', '1570-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1a-edi', '847_lux_us71-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u366-edi', '847_wp_r422-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '336-wh', 'groceryCenters': ['651-bd']}, 20: {'zipcode': '43211', 'stateCode': 'OH', 'distributionCenters': ['1257-3pl', '1321-wm', '1498-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1a-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u358-edi', '847_wp_r451-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '1160-wh', 'groceryCenters': ['1663-bd']}, 21: {'zipcode': '22306', 'stateCode': 'VA', 'distributionCenters': ['1260-3pl', '1321-wm', '1475-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1a-edi', '847_lux_us81-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u360-edi', '847_wp_r428-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '1115-wh', 'groceryCenters': ['729-bd']}, 22: {'zipcode': '96817', 'stateCode': 'HI', 'distributionCenters': ['1252-3pl', '1321-wm', '1409-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1f-edi', '847_lux_us51-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u357-edi', '847_wp_r461-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '687-wh', 'groceryCenters': []}, 23: {'zipcode': '77096', 'stateCode': 'TX', 'distributionCenters': ['1254-3pl', '1321-wm', '1470-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_ntx-edi', '847_lux_us11-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u359-edi', '847_wp_r455-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '1018-wh', 'groceryCenters': ['655-bd']}, 24: {'zipcode': '75244', 'stateCode': 'TX', 'distributionCenters': ['1254-3pl', '1321-wm', '1471-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_ntx-edi', '847_lux_us11-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u359-edi', '847_wp_r455-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '1266-wh', 'groceryCenters': ['655-bd']}, 25: {'zipcode': '02920', 'stateCode': 'RI', 'distributionCenters': ['1260-3pl', '1321-wm', '1549-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1a-edi', '847_lux_us81-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u360-edi', '847_wp_r428-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '1704-wh', 'groceryCenters': ['729-bd']}, 26: {'zipcode': '60620', 'stateCode': 'IL', 'distributionCenters': ['1255-3pl', '1321-wm', '1468-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1a-edi', '847_lux_us41-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u358-edi', '847_wp_r452-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '580-wh', 'groceryCenters': ['580-bd']}, 27: {'zipcode': '94124', 'stateCode': 'CA', 'distributionCenters': ['1251-3pl', '1321-wm', '1461-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_aa_00-spc', '847_aa_u610-edi', '847_d-fis', '847_lg_n1f-edi', '847_lux_us51-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u357-edi', '847_wp_r460-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '144-wh', 'groceryCenters': ['653-bd', '893-bd']}, 28: {'zipcode': '38122', 'stateCode': 'TN', 'distributionCenters': ['1258-3pl', '1321-wm', '1576-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1a-edi', '847_lux_us31-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u367-edi', '847_wp_r453-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '352-wh', 'groceryCenters': ['1665-bd']}, 29: {'zipcode': '33062', 'stateCode': 'FL', 'distributionCenters': ['1259-3pl', '1321-wm', '1484-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1a-edi', '847_lux_us71-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u366-edi', '847_wp_r422-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '88-wh', 'groceryCenters': ['651-bd']}, 30: {'zipcode': '07504', 'stateCode': 'NJ', 'distributionCenters': ['1260-3pl', '1321-wm', '1477-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1a-edi', '847_lux_us81-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u360-edi', '847_wp_r428-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '1214-wh', 'groceryCenters': ['729-bd']}, 31: {'zipcode': '23220', 'stateCode': 'VA', 'distributionCenters': ['1260-3pl', '1321-wm', '1553-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1a-edi', '847_lux_us81-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u360-edi', '847_wp_r428-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '205-wh', 'groceryCenters': ['729-bd']}, 32: {'zipcode': '45213', 'stateCode': 'OH', 'distributionCenters': ['1257-3pl', '1321-wm', '1506-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1a-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u358-edi', '847_wp_r451-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '384-wh', 'groceryCenters': ['1663-bd']}, 33: {'zipcode': '19114', 'stateCode': 'PA', 'distributionCenters': ['1260-3pl', '1321-wm', '1476-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1a-edi', '847_lux_us81-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u360-edi', '847_wp_r428-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '749-wh', 'groceryCenters': ['729-bd']}, 34: {'zipcode': '98144', 'stateCode': 'WA', 'distributionCenters': ['1250-3pl', '1321-wm', '1456-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1f-edi', '847_lux_us01-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u362-edi', '847_wp_r458-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '1-wh', 'groceryCenters': ['115-bd']}, 35: {'zipcode': '02026', 'stateCode': 'MA', 'distributionCenters': ['1260-3pl', '1321-wm', '1549-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1a-edi', '847_lux_us81-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u360-edi', '847_wp_r428-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '319-wh', 'groceryCenters': ['729-bd']}, 36: {'zipcode': '77077', 'stateCode': 'TX', 'distributionCenters': ['1254-3pl', '1321-wm', '1470-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_ntx-edi', '847_lux_us11-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u359-edi', '847_wp_r455-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '680-wh', 'groceryCenters': ['655-bd']}, 37: {'zipcode': '60639', 'stateCode': 'IL', 'distributionCenters': ['1255-3pl', '1321-wm', '1468-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1a-edi', '847_lux_us41-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u358-edi', '847_wp_r452-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '380-wh', 'groceryCenters': ['580-bd']}, 38: {'zipcode': '94066', 'stateCode': 'CA', 'distributionCenters': ['1251-3pl', '1321-wm', '1461-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_aa_00-spc', '847_aa_u610-edi', '847_d-fis', '847_lg_n1f-edi', '847_lux_us51-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u357-edi', '847_wp_r460-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '422-wh', 'groceryCenters': ['653-bd', '848-bd']}, 39: {'zipcode': '11422', 'stateCode': 'NY', 'distributionCenters': ['1260-3pl', '1321-wm', '1474-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1a-edi', '847_lux_us81-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u360-edi', '847_wp_r428-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '310-wh', 'groceryCenters': ['729-bd']}, 40: {'zipcode': '15120', 'stateCode': 'PA', 'distributionCenters': ['1257-3pl', '1321-wm', '1502-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1a-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u358-edi', '847_wp_r451-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '649-wh', 'groceryCenters': ['1663-bd']}, 41: {'zipcode': '20018', 'stateCode': 'DC', 'distributionCenters': ['1257-3pl', '1321-wm', '1459-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1a-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u358-edi', '847_wp_r451-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '1120-wh', 'groceryCenters': ['729-bd']}, 42: {'zipcode': '70607', 'stateCode': 'LA', 'distributionCenters': ['1254-3pl', '1321-wm', '1470-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_ntx-edi', '847_lux_us11-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '1201-wh', 'groceryCenters': ['655-bd']}, 43: {'zipcode': '32405', 'stateCode': 'FL', 'distributionCenters': ['1258-3pl', '1321-wm', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1a-edi', '847_lux_us31-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u367-edi', '847_wp_r453-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '1026-wh', 'groceryCenters': ['651-bd']}, 44: {'zipcode': '32563', 'stateCode': 'FL', 'distributionCenters': ['1258-3pl', '1321-wm', '1573-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1a-edi', '847_lux_us31-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u367-edi', '847_wp_r453-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '1192-wh', 'groceryCenters': ['579-bd']}, 45: {'zipcode': '93003', 'stateCode': 'CA', 'distributionCenters': ['1252-3pl', '1321-wm', '1462-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_aa_00-spc', '847_aa_u610-edi', '847_d-fis', '847_lg_n1f-edi', '847_lux_us51-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u357-edi', '847_wp_r461-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '420-wh', 'groceryCenters': ['653-bd']}, 46: {'zipcode': '28403', 'stateCode': 'NC', 'distributionCenters': ['1258-3pl', '1321-wm', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1a-edi', '847_lux_us31-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u367-edi', '847_wp_r453-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '635-wh', 'groceryCenters': ['651-bd']}, 47: {'zipcode': '98367', 'stateCode': 'WA', 'distributionCenters': ['1250-3pl', '1321-wm', '1456-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1f-edi', '847_lux_us01-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u362-edi', '847_wp_r458-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '624-wh', 'groceryCenters': ['115-bd']}, 48: {'zipcode': '11706', 'stateCode': 'NY', 'distributionCenters': ['1260-3pl', '1321-wm', '1474-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1a-edi', '847_lux_us81-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u360-edi', '847_wp_r428-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '240-wh', 'groceryCenters': ['729-bd']}, 49: {'zipcode': '14580', 'stateCode': 'NY', 'distributionCenters': ['1260-3pl', '1321-wm', '1508-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1a-edi', '847_lux_us81-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u358-edi', '847_wp_r451-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '1195-wh', 'groceryCenters': ['1663-bd']}, 50: {'zipcode': '19958', 'stateCode': 'DE', 'distributionCenters': ['1260-3pl', '1321-wm', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1a-edi', '847_lux_us81-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u360-edi', '847_wp_r428-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '246-wh', 'groceryCenters': ['729-bd']}, 51: {'zipcode': '49418', 'stateCode': 'MI', 'distributionCenters': ['1255-3pl', '1321-wm', '1526-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1a-edi', '847_lux_us41-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u358-edi', '847_wp_r452-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '744-wh', 'groceryCenters': ['1663-bd']}, 52: {'zipcode': '16509', 'stateCode': 'PA', 'distributionCenters': ['1257-3pl', '1321-wm', '1502-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1a-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u358-edi', '847_wp_r451-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '1669-wh', 'groceryCenters': ['1663-bd']}, 53: {'zipcode': '33549', 'stateCode': 'FL', 'distributionCenters': ['1259-3pl', '1321-wm', '1570-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1a-edi', '847_lux_us71-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u366-edi', '847_wp_r422-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '1249-wh', 'groceryCenters': ['651-bd']}, 54: {'zipcode': '22192', 'stateCode': 'VA', 'distributionCenters': ['1260-3pl', '1321-wm', '1475-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1a-edi', '847_lux_us81-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u360-edi', '847_wp_r428-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '626-wh', 'groceryCenters': ['729-bd']}, 55: {'zipcode': '36526', 'stateCode': 'AL', 'distributionCenters': ['1258-3pl', '1321-wm', '1573-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1a-edi', '847_lux_us31-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u367-edi', '847_wp_r453-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '1192-wh', 'groceryCenters': ['579-bd']}, 56: {'zipcode': '78412', 'stateCode': 'TX', 'distributionCenters': ['1254-3pl', '1321-wm', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_ntx-edi', '847_lux_us11-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '1108-wh', 'groceryCenters': ['655-bd']}, 57: {'zipcode': '92591', 'stateCode': 'CA', 'distributionCenters': ['1252-3pl', '1321-wm', '1465-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_aa_00-spc', '847_aa_u610-edi', '847_d-fis', '847_lg_n1f-edi', '847_lux_us51-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u357-edi', '847_wp_r461-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '491-wh', 'groceryCenters': ['653-bd']}, 58: {'zipcode': '29588', 'stateCode': 'SC', 'distributionCenters': ['1258-3pl', '1321-wm', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1a-edi', '847_lux_us31-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u367-edi', '847_wp_r453-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '338-wh', 'groceryCenters': ['651-bd']}, 59: {'zipcode': '96797', 'stateCode': 'HI', 'distributionCenters': ['1252-3pl', '1321-wm', '1409-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1f-edi', '847_lux_us51-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u357-edi', '847_wp_r461-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '485-wh', 'groceryCenters': []}, 60: {'zipcode': '06810', 'stateCode': 'CT', 'distributionCenters': ['1260-3pl', '1321-wm', '1478-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1a-edi', '847_lux_us81-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u360-edi', '847_wp_r428-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '304-wh', 'groceryCenters': ['729-bd']}, 61: {'zipcode': '01801', 'stateCode': 'MA', 'distributionCenters': ['1260-3pl', '1321-wm', '1549-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1a-edi', '847_lux_us81-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u360-edi', '847_wp_r428-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '333-wh', 'groceryCenters': ['729-bd']}, 62: {'zipcode': '19934', 'stateCode': 'DE', 'distributionCenters': ['1260-3pl', '1321-wm', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1a-edi', '847_lux_us81-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u360-edi', '847_wp_r428-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '246-wh', 'groceryCenters': ['729-bd']}, 63: {'zipcode': '44224', 'stateCode': 'OH', 'distributionCenters': ['1257-3pl', '1321-wm', '1504-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1a-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u358-edi', '847_wp_r451-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '1226-wh', 'groceryCenters': ['1663-bd']}, 64: {'zipcode': '48197', 'stateCode': 'MI', 'distributionCenters': ['1257-3pl', '1321-wm', '1460-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1a-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u358-edi', '847_wp_r451-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '1106-wh', 'groceryCenters': ['1663-bd']}, 65: {'zipcode': '20155', 'stateCode': 'VA', 'distributionCenters': ['1260-3pl', '1321-wm', '1475-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1a-edi', '847_lux_us81-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u360-edi', '847_wp_r428-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '225-wh', 'groceryCenters': ['729-bd']}, 66: {'zipcode': '29464', 'stateCode': 'SC', 'distributionCenters': ['1258-3pl', '1321-wm', '1540-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1a-edi', '847_lux_us31-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u367-edi', '847_wp_r453-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '1319-wh', 'groceryCenters': ['651-bd']}, 67: {'zipcode': '32223', 'stateCode': 'FL', 'distributionCenters': ['1259-3pl', '1321-wm', '1574-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1a-edi', '847_lux_us71-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u366-edi', '847_wp_r422-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '1294-wh', 'groceryCenters': ['651-bd']}, 68: {'zipcode': '91331', 'stateCode': 'CA', 'distributionCenters': ['1252-3pl', '1321-wm', '1462-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_aa_00-spc', '847_aa_u610-edi', '847_d-fis', '847_lg_n1f-edi', '847_lux_us51-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u357-edi', '847_wp_r461-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '1071-wh', 'groceryCenters': ['653-bd']}, 69: {'zipcode': '98223', 'stateCode': 'WA', 'distributionCenters': ['1250-3pl', '1321-wm', '1456-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1f-edi', '847_lux_us01-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u362-edi', '847_wp_r458-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '642-wh', 'groceryCenters': ['115-bd']}, 70: {'zipcode': '70002', 'stateCode': 'LA', 'distributionCenters': ['1254-3pl', '1321-wm', '1566-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_ntx-edi', '847_lux_us11-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u359-edi', '847_wp_r455-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '1147-wh', 'groceryCenters': ['655-bd']}, 71: {'zipcode': '72802', 'stateCode': 'AR', 'distributionCenters': ['1254-3pl', '1321-wm', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_ntx-edi', '847_lux_us11-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '1442-wh', 'groceryCenters': ['655-bd']}, 72: {'zipcode': '35055', 'stateCode': 'AL', 'distributionCenters': ['1258-3pl', '1321-wm', '1729-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1a-edi', '847_lux_us31-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u367-edi', '847_wp_r453-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '1712-wh', 'groceryCenters': ['579-bd']}, 73: {'zipcode': '75156', 'stateCode': 'TX', 'distributionCenters': ['1254-3pl', '1321-wm', '1728-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_ntx-edi', '847_lux_us11-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u359-edi', '847_wp_r455-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '1049-wh', 'groceryCenters': ['655-bd']}, 74: {'zipcode': '37055', 'stateCode': 'TN', 'distributionCenters': ['1258-3pl', '1321-wm', '1562-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1a-edi', '847_lux_us31-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u367-edi', '847_wp_r453-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '630-wh', 'groceryCenters': ['1665-bd']}, 75: {'zipcode': '01082', 'stateCode': 'MA', 'distributionCenters': ['1260-3pl', '1321-wm', '1478-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1a-edi', '847_lux_us81-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u360-edi', '847_wp_r428-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '302-wh', 'groceryCenters': ['729-bd']}, 76: {'zipcode': '17202', 'stateCode': 'PA', 'distributionCenters': ['1260-3pl', '1321-wm', '1548-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1a-edi', '847_lux_us81-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u360-edi', '847_wp_r428-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '330-wh', 'groceryCenters': ['729-bd']}, 77: {'zipcode': '03042', 'stateCode': 'NH', 'distributionCenters': ['1260-3pl', '1321-wm', '1549-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1a-edi', '847_lux_us81-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u360-edi', '847_wp_r428-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '307-wh', 'groceryCenters': ['729-bd']}, 78: {'zipcode': '28557', 'stateCode': 'NC', 'distributionCenters': ['1258-3pl', '1321-wm', '1536-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1a-edi', '847_lux_us31-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '635-wh', 'groceryCenters': ['651-bd']}, 79: {'zipcode': '21502', 'stateCode': 'MD', 'distributionCenters': ['1257-3pl', '1321-wm', '1502-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1a-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u358-edi', '847_wp_r451-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '239-wh', 'groceryCenters': ['1663-bd']}, 80: {'zipcode': '38555', 'stateCode': 'TN', 'distributionCenters': ['1258-3pl', '1321-wm', '1537-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1a-edi', '847_lux_us31-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u367-edi', '847_wp_r453-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '1116-wh', 'groceryCenters': ['1665-bd']}, 81: {'zipcode': '77640', 'stateCode': 'TX', 'distributionCenters': ['1254-3pl', '1321-wm', '1470-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_ntx-edi', '847_lux_us11-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u359-edi', '847_wp_r455-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '1330-wh', 'groceryCenters': ['655-bd']}, 82: {'zipcode': '74804', 'stateCode': 'OK', 'distributionCenters': ['1254-3pl', '1321-wm', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_ntx-edi', '847_lux_us11-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u359-edi', '847_wp_r455-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '1444-wh', 'groceryCenters': ['655-bd']}, 83: {'zipcode': '70663', 'stateCode': 'LA', 'distributionCenters': ['1254-3pl', '1321-wm', '1470-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_ntx-edi', '847_lux_us11-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '1201-wh', 'groceryCenters': ['655-bd']}, 84: {'zipcode': '95370', 'stateCode': 'CA', 'distributionCenters': ['1251-3pl', '1321-wm', '1479-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_aa_00-spc', '847_aa_u610-edi', '847_d-fis', '847_lg_n1f-edi', '847_lux_us51-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u357-edi', '847_wp_r460-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '1683-wh', 'groceryCenters': ['653-bd', '848-bd']}, 85: {'zipcode': '74403', 'stateCode': 'OK', 'distributionCenters': ['1254-3pl', '1321-wm', '1534-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_ntx-edi', '847_lux_us11-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u359-edi', '847_wp_r455-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '1207-wh', 'groceryCenters': ['655-bd']}, 86: {'zipcode': '14424', 'stateCode': 'NY', 'distributionCenters': ['1260-3pl', '1321-wm', '1508-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1a-edi', '847_lux_us81-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u358-edi', '847_wp_r451-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '1195-wh', 'groceryCenters': ['1663-bd']}, 87: {'zipcode': '34601', 'stateCode': 'FL', 'distributionCenters': ['1259-3pl', '1321-wm', '1570-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1a-edi', '847_lux_us71-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u366-edi', '847_wp_r422-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '1249-wh', 'groceryCenters': ['651-bd']}, 88: {'zipcode': '29150', 'stateCode': 'SC', 'distributionCenters': ['1258-3pl', '1321-wm', '1540-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1a-edi', '847_lux_us31-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u367-edi', '847_wp_r453-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '1232-wh', 'groceryCenters': ['579-bd']}, 89: {'zipcode': '22701', 'stateCode': 'VA', 'distributionCenters': ['1260-3pl', '1321-wm', '1475-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1a-edi', '847_lux_us81-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u360-edi', '847_wp_r428-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '340-wh', 'groceryCenters': ['729-bd']}, 90: {'zipcode': '49221', 'stateCode': 'MI', 'distributionCenters': ['1257-3pl', '1321-wm', '1460-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1a-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u358-edi', '847_wp_r451-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '1007-wh', 'groceryCenters': ['1663-bd']}, 91: {'zipcode': '28443', 'stateCode': 'NC', 'distributionCenters': ['1258-3pl', '1321-wm', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1a-edi', '847_lux_us31-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u367-edi', '847_wp_r453-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '635-wh', 'groceryCenters': ['651-bd']}, 92: {'zipcode': '20619', 'stateCode': 'MD', 'distributionCenters': ['1257-3pl', '1321-wm', '1459-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1a-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u358-edi', '847_wp_r451-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '1039-wh', 'groceryCenters': ['729-bd']}, 93: {'zipcode': '07047', 'stateCode': 'NJ', 'distributionCenters': ['1260-3pl', '1321-wm', '1477-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1a-edi', '847_lux_us81-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u360-edi', '847_wp_r428-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '1062-wh', 'groceryCenters': ['729-bd']}, 94: {'zipcode': '76904', 'stateCode': 'TX', 'distributionCenters': ['1254-3pl', '1321-wm', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_ntx-edi', '847_lux_us11-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '1756-wh', 'groceryCenters': ['655-bd']}, 95: {'zipcode': '37075', 'stateCode': 'TN', 'distributionCenters': ['1258-3pl', '1321-wm', '1562-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1a-edi', '847_lux_us31-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u367-edi', '847_wp_r453-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '1659-wh', 'groceryCenters': ['1665-bd']}, 96: {'zipcode': '28001', 'stateCode': 'NC', 'distributionCenters': ['1258-3pl', '1321-wm', '1538-3pl', '283-wm', '561-wm', '725-wm', '731-wm', '758-wm', '759-wm', '847_0-cor', '847_0-cwt', '847_0-edi', '847_0-ehs', '847_0-membership', '847_0-mpt', '847_0-spc', '847_0-wm', '847_1-cwt', '847_1-edi', '847_d-fis', '847_lg_n1a-edi', '847_lux_us31-edi', '847_NA-cor', '847_NA-pharmacy', '847_NA-wm', '847_ss_u367-edi', '847_wp_r453-edi', '951-wm', '952-wm', '9847-wcs'], 'warehouse': '367-wh', 'groceryCenters': ['579-bd']}}
    paths = {
        "product": {
            "product_details": {
                "sku": {"paths": [], "default": ""},
                "color": {"paths": [], "default": ""},
                "product_name": {"paths": ["//div[@data-testid='Text_brand-name']/text()"], "default": ""},
                "brand": {"paths": ["//div[@itemprop='brand']/text()"], "default": "COSTCO"},
                "breadcrumbs": {"paths": ["//nav[@aria-label='Breadcrumb']//div/ul/li/a/span[@itemprop='name']/text()"],
                                "default": []},
                "description": {"paths": ["//div[@id='product-details-summary']//text()"], "default": ""},
                "feature_list": {"paths": ["//div[contains(@data-testid,'Text_feature')]//text()"], "default": []},
                "offer_list": {"paths": [], "default": []},
                "feature_image": {"paths": ["//img[@id='initialProductImage']/@src"], "default": ""},
                "pdp_images": {"paths": [""], "default": []},
                "pdp_url": {"paths": [], "default": ""},
                "mrp": {"paths": [], "default": 0},
                "selling_price": {"paths": [], "default": 0},
                "rating": {"paths": ["//span[@itemprop='ratingValue']/text()"], "default": None},
                "rating_count": {"paths": [], "default": None},
                "review_count": {"paths": ["//span[@itemprop='reviewCount']/text()"], "default": None},
                "style_attributes": {"paths": ["//table[@id='ProductSpecifications']//tr"],
                                     "default": {}},
                "variants": {"paths": [""], "default": []},
                "related_products": {"paths": [""], "default": []},
                "relation": {"paths": [], "default": ""},
            },
            "offer_list": {
                "code": {"paths": [], "default": ""},
                "title": {"paths": [], "default": ""},
                "description": {"paths": [], "default": ""},
                "savings": {"paths": [], "default": ""},
            },
            "variants": {
                "size": {"paths": [""], "default": ""},
                "stock": {"paths": [""], "default": None},
                "available": {"paths": [""], "default": None},
                "color": {"paths": [], "default": None},
                "variant_sku": {"paths": [""], "default": ""},
                "mrp": {"paths": [""], "default": 0},
                "selling_price": {"paths": [""], "default": 0},
            },
            "style_attributes": {
                "style_key": {"paths": [".//th/text()"], "default": None},
                "style_value": {"paths": [".//td/text()"], "default": ""},
            },
            "related_products": {
                "pdp_url": {"paths": [""], "default": []},
                "sku": {"paths": [""], "default": ""},
                "color": {"paths": [""], "default": ""},
            },
        },
        "category": {
            "product_collection": ["response.docs[*]"],
            "product_details": {
                "sku": {"paths": ["item_number"], "default": ""},
                "rank": {"paths": [], "default": ""},
                "pdp_url": {"paths": ["./@data-pdp-url"], "default": ""},
            },
            "pagination": {"next_page": "", "total_pages": "", "total_count": "", "page_size": ""
                           }
        }
    }
    
    custom_settings = {
        "DOWNLOADER_MIDDLEWARES": {
            "engine.middlewares.proxy.PrivateProxyMiddleware": 450,
            "engine.middlewares.proxy.BanDetectionMiddleware": 460,
            "engine.middlewares.curl_cffi.CurlCffiMiddleware": 451,
            "engine.middlewares.nimble.NimbleMiddleware": 470,
        }
    }
    
    @staticmethod
    def get_headers(msg_dict) -> Dict:
        if msg_dict.get("crawl_type") == "category":
            return {
                'Accept': 'application/json',
                'Accept-Language': 'en-US,en;q=0.9',
                'Connection': 'keep-alive',
                'Content-Type': 'application/json',
                'Origin': 'https://www.costco.com',
                'Referer': 'https://www.costco.com/',
                'Sec-Fetch-Dest': 'empty',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Site': 'same-site',
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
                'sec-ch-ua': '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Linux"',
                'x-api-key': '273db6be-f015-4de7-b0d6-dd4746ccd5c3',
            }
        else:
            headers = {
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'accept-language': 'en-US,en;q=0.9',
                'cache-control': 'max-age=0',
                'priority': 'u=0, i',
                'referer': msg_dict.get("pdp_url"),
                'sec-ch-ua': '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Linux"',
                'sec-fetch-dest': 'document',
                'sec-fetch-mode': 'navigate',
                'sec-fetch-site': 'same-origin',
                'upgrade-insecure-requests': '1',
                'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
            }

            msg_dict["forced_headers"] = headers
            return headers

    @staticmethod
    def get_proxy() -> str:
        return os.getenv("LOCAL_PROXY_SERVER")

    def get_category_url(self, msg_dict) -> str:
        forced_url = msg_dict.get("forced_url", None)
        if forced_url:
            return forced_url
        locale_id = msg_dict.get("locale_id", None)
        locale_dict = self.locale_map2.get(locale_id, {})
        url = msg_dict.get("url") or ""
        url_parts = list(urlparse(url))
        category_path = quote(url_parts[2])
        base_api_url = 'https://search.costco.com/api/apps/www_costco_com/query/www_costco_com_navigation'

        params = {
            'q': '*:*',
            'expoption': 'lw',
            'locale': 'en-US',
            'start': 0,
            'expand': 'false',
            'rows': 24,
            'url': category_path,
            'fq': '{!tag=item_program_eligibility}item_program_eligibility:("ShipIt")',
            'chdcategory': 'true',
            'chdheader': 'true'
        }

        if locale_id == 1:
            params['userLocation'] = 'WA'
            params['loc'] = '*'

        else:
            distributionCenters = locale_dict.get("distributionCenters", [])
            selected_warehouse = locale_dict.get("warehouse", "s-wh")
            user_location = locale_dict.get("stateCode", "WA")
            grocery_centres = locale_dict.get('groceryCenters', [])


            filtered_locs = [loc for loc in grocery_centres if loc]
            filtered_locs.extend(loc for loc in distributionCenters if loc)

            if selected_warehouse not in filtered_locs:
                filtered_locs.append(selected_warehouse)

            params['userLocation'] = user_location
            params['loc'] = ",".join(filtered_locs) if filtered_locs else '*'
            params['whloc'] = selected_warehouse

        api_url = f'{base_api_url}?{urlencode(params)}'
        return api_url

    def product_parser(self, response: Response, **kwargs) -> Union[Dict, None]:
        try:
            msg_dict: dict = response.meta.get("msg_dict", {})
            message = response.meta.get("message", None)
            extra: dict = msg_dict.get("extra", {})
            selector = Selector(response)
            pd = self.populate_product_x_path(selector, {
                "pdp_url": msg_dict.get("pdp_url"),
                "sku": msg_dict.get("sku"),
                "rating": float("{:.1f}".format(float(extra.get("rating")))) if extra.get("rating") else None,
                "review_count": int(extra.get("review_count")) if extra.get("review_count") else None
            }, self.paths.get("product", {}))
            breadcrumbs_json = selector.xpath("//script[contains(text(), 'BreadcrumbList')]//text()").extract_first()
            if breadcrumbs_json:
                breadcrumbs_json = json.loads(breadcrumbs_json)
                pd["breadcrumbs"] = self.j_extract(breadcrumbs_json, '$..itemListElement..name', [])
            script_text = selector.xpath(
                "//script[contains(@type,'application/ld+json') and not(contains(text(),'BreadcrumbList'))]/text()").extract_first()
            if script_text:
                script_json = json.loads(script_text)
            else:
                script_json = {}
            pd["feature_image"] = self.j_extract_first(script_json, '$..image', [])
            pd["brand"] = self.j_extract_first(script_json, '$..brand.name', []) or "Costco"
            is_variant = False
            if selector.xpath("//script[contains(text(), 'primaryItemNumber')]//text()").extract_first():
                script_text = selector.xpath(
                    "//script[contains(text(), 'primaryItemNumber')]//text()").extract_first().replace('\\\\"',
                                                                                                       '\\"').replace(
                    '\\"',
                    '"')
                script_json = re.findall(r'null,({.*})', script_text)
                script_json = json.loads(script_json[0]) if script_json else {}
                is_variant = True
            locale_dict = self.locale_map2.get(msg_dict.get('locale_id'))
            part_numbers = self.j_extract(script_json, "$.primaryVariantsGroupData..itemNumber") if is_variant else [
                pd["sku"]]
            instore_url = "https://ecom-api.costco.com/ebusiness/inventory/v1/inventorylevels/availability/batch/v2"
            d = locale_dict.get('distributionCenters')
            grocery_centres = locale_dict.get('groceryCenters', [])
            if grocery_centres:
                d.extend(g for g in grocery_centres if g)
            instore_payload = json.dumps({
                "distributionCenters": d,
                "selectedWarehouse": f"{locale_dict.get('warehouse')}",
                "itemNumbers": part_numbers
            })
            instore_headers = {
                'Accept': 'application/json, text/plain, */*',
                'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
                'Connection': 'keep-alive',
                'Content-Type': 'application/json-patch+json',
                'Origin': 'https://www.costco.com',
                'Referer': 'https://www.costco.com/',
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
                'client-identifier': '481b1aec-aa3b-454b-b81b-48187e28f205',
                'costco.env': 'PROD',
                'costco.service': 'restInventory'
            }
            instore_json = {"store_data": json.loads(self.make_async_requests(
                urls=[{"url": instore_url, "data": instore_payload}],
                proxies=self.get_proxy(), method="POST", headers=instore_headers,
                handle_httpstatus_list=[403])[0])}
            image_script = selector.xpath(
                "//script[contains(text(), 'null,{\\\"imageName')]/text()").extract_first().replace('\\\\"',
                                                                                                    '\\"').replace(
                '\\"',
                '"')

            image_json = re.findall(r'null,({.*})', image_script)
            image_json = json.loads(image_json[0]) if image_json else {}
            if len(part_numbers) > 1:
                slug = image_json.get("slug")
                image_api = f"https://gdx-api.costco.com/catalog/asset/asset/v1/bf/product/USBC-{slug}"
                headers = {
                    'Accept': '*/*',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Connection': 'keep-alive',
                    'Origin': 'https://www.costco.com',
                    'Referer': 'https://www.costco.com/',
                    'Sec-Fetch-Dest': 'empty',
                    'Sec-Fetch-Mode': 'cors',
                    'Sec-Fetch-Site': 'same-site',
                    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
                    'client-identifier': '67471e60-2b75-48b7-899b-2a86d74e6143',
                    'sec-ch-ua': '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"Linux"',
                }
                image_resp = self.make_async_requests(urls=[{'url': image_api}], headers=headers,
                                                      proxies=self.get_proxy())
                data_dict = json.loads(image_resp[0])

            else:
                images = self.j_extract(image_json, '$.brandfolderAsset.*.attachments.data.0.attributes.cdn_url', [])
                videos = []
                for img in images:
                    if ".mp4" in img:
                        videos.append(img)
                    else:
                        pd["pdp_images"].append(img)

                if videos:
                    pd["related_media"] = [
                        {"Video_Links": [
                            {"Unclassified_Videos_Links": videos}]}]

            member_only = False
            if selector.xpath('//p[@automation-id="memberOnly"]/text()').extract_first() or selector.xpath('//div[@data-testid="Text_Members Only_id"]//text()').extract_first() :
                member_only = True
            related_products = []
            for part_number in part_numbers:
                related_products.append({
                    'sku': part_number,
                    'color': '',
                    'pdp_url': pd["pdp_url"]
                })
            for var in part_numbers:
                new_pd = copy.deepcopy(pd)
                variants = []
                size_name = "One Size"
                key = self.j_extract_first(script_json,
                                           f"$.primaryVariantsGroupData[?(@.itemNumber=='{var}')].key") if is_variant else ""
                value = self.j_extract_first(script_json,
                                             f"$.primaryVariantsGroupData[?(@.itemNumber=='{var}')].value") if is_variant else ""
                if key and value:
                    new_pd["style_attributes"][key] = [value]
                # for opt in self.j_extract(var, "$.options.*", []):
                #     for outer_key, mapping in variant_map.items():
                #         if mapping.get(opt):
                #             new_pd["style_attributes"][outer_key] = [mapping.get(opt)]
                part_number = var
                price_api = "https://ecom-api.costco.com/ebusiness/product/v1/products/graphql"
                warehouse_number = locale_dict.get('warehouse').replace('-wh', '')
                price_payload = "{\"query\":\"\\n            query {\\n                products(\\n                    itemNumbers: [\\\"" + part_number + "\\\"],\\n                    clientId: \\\"4900eb1f-0c10-4bd9-99c3-c59e6c1ecebf\\\",\\n                    locale: \\\"en-us\\\",\\n                    warehouseNumber: \\\"" + warehouse_number + "\\\"\\n                ) {\\n                    catalogData {\\n                        itemNumber\\n                        itemId\\n                        published\\n                        locale\\n                        buyable\\n                        programTypes\\n                        priceData {\\n                            price\\n                            listPrice\\n                        }\\n                        attributes {\\n                            key\\n                            value\\n                            type\\n                            pills\\n                            identifier\\n                        }\\n                        description {\\n                            shortDescription\\n                            longDescription\\n                            marketingContent\\n                            auxDescription1\\n                            auxDescription2\\n                            marketingStatement\\n                            promotionalStatement\\n                            popupStatement\\n                        }\\n                        additionalFieldData {\\n                            rating\\n                            numberOfRating\\n                            dispPriceInCartOnly\\n                            eligibleForReviews\\n                            fsa\\n                            chdIndicator\\n                            linkFeeEligible\\n                            membershipReqd\\n                            productClassType\\n                            disponZeroInv\\n                            backOrderableType\\n                            backOrderQuantity\\n                            maxItemOrderQty\\n                            minItemOrderQty\\n                            linkFeePrices {\\n                                key\\n                                fees {\\n                                    feeCategory\\n                                    includeInPrice\\n                                    excludeFromPrice\\n                                }\\n                            }\\n                        }\\n                        fieldData {\\n                            mfPartNumber\\n                            mfName\\n                            addedDate\\n                            startDate\\n                            endDate\\n                            comparable\\n                            swatchable\\n                            imageName\\n                            variableWeight\\n                        }\\n                    }\\n                    fulfillmentData {\\n                        itemNumber\\n                        warehouseNumber\\n                        clientId\\n                        channel\\n                        currency\\n                        price\\n                        linkFee\\n                        listPrice\\n                        field1Data {\\n                            replacedItem\\n                            replacementType\\n                        }\\n                    }\\n                    childData {\\n                        catalogData {\\n                            itemNumber\\n                            buyable\\n                            published\\n                            parentId\\n                            programTypes\\n                            additionalFieldData {\\n                                membershipReqd\\n                                fsa\\n                                chdIndicator\\n                                disponZeroInv\\n                                backOrderableType\\n                                backOrderQuantity\\n                                rating\\n                                numberOfRating\\n                                \\n            linkFeeEligible\\n            linkFeePrices {\\n                                key\\n                                fees {\\n                                    feeCategory\\n                                    includeInPrice\\n                                    excludeFromPrice\\n                                }\\n                            }\\n        \\n                            }\\n                            attributes {\\n                                key\\n                                value\\n                                type\\n                                pills\\n                                identifier\\n                                swatchImage\\n                            }\\n                            fieldData {\\n                                mfPartNumber\\n                                mfName\\n                                addedDate\\n                                startDate\\n                                endDate\\n                                comparable\\n                                swatchable\\n                                imageName\\n                                variableWeight\\n                            }\\n                            priceData {\\n                                price\\n                                listPrice\\n                            }\\n                        }\\n                        fulfillmentData {\\n                            itemNumber\\n                            warehouseNumber\\n                            clientId\\n                            channel\\n                            currency\\n                            price\\n                            linkFee\\n                            listPrice\\n                            discounts {\\n                                promoAmount\\n                                promoType\\n                                promoStartDate\\n                                promoEndDate\\n                                maximumCount\\n                            }\\n                            shippingInfo {\\n                                unitOfMeasure\\n                                factor\\n                                externalCarrier\\n                                fulfillmentMethods\\n                                carrierServices\\n                            }\\n                            field1Data {\\n                                replacedItem\\n                                replacementType\\n                            }\\n                        }\\n                    }\\n                }\\n            }\\n        \",\"variables\":{}}"
                price_headers = {
                    'Accept': '*/*',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Connection': 'keep-alive',
                    'Content-Type': 'application/json',
                    'Origin': 'https://www.costco.com',
                    'Referer': 'https://www.costco.com/',
                    'Sec-Fetch-Dest': 'empty',
                    'Sec-Fetch-Mode': 'cors',
                    'Sec-Fetch-Site': 'same-site',
                    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
                    'client-identifier': '4900eb1f-0c10-4bd9-99c3-c59e6c1ecebf',
                    'costco.env': 'ecom',
                    'costco.service': 'restProduct',
                    'sec-ch-ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"Linux"'
                }

                price_json = json.loads(self.make_async_requests(
                    urls=[{"url": price_api, "data": price_payload}],
                    proxies=self.get_proxy(), method="POST", headers=price_headers,
                    handle_httpstatus_list=[403], retries=10)[0])
                mrp_path_1 = f"$.data.products.catalogData[?(@.itemNumber=='{part_number}')].priceData.listPrice"
                mrp_path_2 = f"$.data.products.catalogData[?(@.itemNumber=='{part_number}')].priceData.price"

                selling_price_path_1 = f"$.data.products.catalogData[?(@.itemNumber=='{part_number}')].priceData.price"
                selling_price_path_2 = f"$.data.products.catalogData[?(@.itemNumber=='{part_number}')].priceData.listPrice"

                mrp_from_path1 = float(self.j_extract_first(price_json, mrp_path_1) or 0)
                mrp_from_path2 = float(self.j_extract_first(price_json, mrp_path_2) or 0)
                mrp = max(mrp_from_path1, mrp_from_path2)

                selling_price_from_path1 = float(self.j_extract_first(price_json, selling_price_path_1) or 0)
                selling_price_from_path2 = float(self.j_extract_first(price_json, selling_price_path_2) or 0)
                selling_price = min(selling_price_from_path1, selling_price_from_path2)

                if mrp:
                    if mrp < 0:
                        mrp = selling_price
                    mrp = round(mrp, 2)
                if selling_price:
                    if selling_price < 0:
                        selling_price = 0
                    selling_price = round(selling_price, 2)
                if selling_price == 0 or selling_price > mrp:
                    selling_price = mrp
                if mrp == 0 or mrp < selling_price:
                    mrp = selling_price
                new_pd["mrp"] = mrp if not member_only else None
                new_pd["selling_price"] = selling_price if not member_only else None

                instore_bool = self.j_extract_first(instore_json,
                                                    f'$.store_data[?(@.itemNumber=="{part_number}")]..inWarehouse..availability',
                                                    ["NOSTOCK"]) != 'NOSTOCK'

                if selector.xpath(
                        '//button[@data-testid="Button_addToCartDrawer_pdp"]//text()').extract_first():
                    online_availability = True
                else:
                    online_availability = False
                variants.append({
                    'available': online_availability or instore_bool,
                    'size': size_name.replace("&#039;", "'"),
                    "color": '',
                    'variant_sku': part_number,
                    'stock': None,
                    'mrp': mrp if not member_only else None,
                    'selling_price': selling_price if not member_only else None
                })

                offline_availability = instore_bool
                new_pd['availability'] = [{"available": online_availability,
                                           "stock": None,
                                           "size": "Online",
                                           "color": '',
                                           "variant_sku": "",
                                           }, {
                                              'size': 'In Store',
                                              'stock': None,
                                              'available': offline_availability,
                                              'color': '',
                                              'variant_sku': ''
                                          }]
                if msg_dict.get('locale_id') == 1:
                    new_pd['availability'] = [{"available": online_availability,
                                               "stock": None,
                                               "size": "Online",
                                               "color": '',
                                               "variant_sku": "",
                                               }]

                new_pd['variants'] = variants
                if len(part_numbers) > 1:
                    image_dict = [
                        item
                        for item in data_dict["data"]
                        if any(
                            field.get("key") == "ItemNumbers" and field.get("value") == f"{part_number}"
                            for field in item["relationships"]["custom_field_values"]["data"]
                        )
                    ]
                    images = self.j_extract(image_dict, '$..attachments.data.0.attributes.cdn_url',
                                            [])
                    videos = []
                    for img in images:
                        if ".mp4" in img:
                            videos.append(img)
                        else:
                            new_pd["pdp_images"].append(img)

                    if videos:
                        new_pd["related_media"] = [
                            {"Video_Links": [
                                {"Unclassified_Videos_Links": videos}]}]
                    script_text1 = selector.xpath(
                        "//script[contains(text(), 'productAttributes')]//text()").extract_first().replace('\\\\"',
                                                                                                           '\\"').replace(
                        '\\"',
                        '"')
                    if script_text1:
                        script_json1 = re.findall(r'null,({.*})', script_text1)
                        script_json1 = json.loads(script_json1[0]) if script_json1 else {}
                        model_number = self.j_extract(script_json1,
                                                      f'$..childCatalogData[?(@.id=="{part_number}")].productAttributes..object[?(@.key=="Model")].value',
                                                      [])
                        if model_number:
                            new_pd["style_attributes"]["Model"] = model_number
                        feature_image = self.j_extract_first(script_json1,
                                                             f'$..childCatalogData[?(@.id=="{part_number}")].descriptions.0.object.imageName',
                                                             [])
                        if feature_image:
                            new_pd["feature_image"] = feature_image
                # url_parts = list(urlparse(pd["pdp_url"]))
                # query_p = dict(parse_qsl(url_parts[4]))
                # query_p["preselect"] = f"manufacturer color:{color.lower()}"
                # url_parts[4] = urlencode(query_p)
                # new_pd["pdp_url"] = urlunparse(url_parts)
                # pdp_images = self.j_extract(image_json,
                #                             f"$.itemDetailsList[?(@.itemDetail.itemId=='{part_number}')].itemDetail.imageDetails..cdn_url")
                # videos = self.j_extract(image_json,
                #                         f"$.itemDetailsList[?(@.itemDetail.itemId=='{part_number}')].itemDetail.videoItem.options.sources[*].src")
                # if videos:
                #     new_pd["related_media"] = [
                #         {"Video_Links": [
                #             {"Unclassified_Videos_Links": videos}]}]
                #
                # new_pd["pdp_images"] = pdp_images
                # new_pd["feature_image"] = self.j_extract_first(var,"img_url")
                new_pd['sku'] = part_number
                related_products_copy = copy.deepcopy(related_products)
                new_pd['related_products'] = [r for r in related_products_copy if r.get("sku") != new_pd['sku']]
                msg_dict['sku'] = new_pd['sku']
                new_pd['color'] = ''
                product_item = self.populate_product_item(response, details=new_pd, msg_dict=msg_dict)
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
            category_json = json.loads(response.text)
            if response.status in [403]:
                self.logger.warning("Retrying <%(url)s> 403 status code",
                                    {"url": msg_dict.get('forced_url', None) or msg_dict['url']})
                self.push_message_in_queue_for_retry(message=message)
                return
            if first_hit:
                total_count = self.j_extract_first(category_json, "$.response.numFound")
                total_pages = math.ceil(total_count / self.page_size)
                url_parts = list(urlparse(response.url))
                query = dict(parse_qsl(url_parts[4]))
                msg_dict["pages_to_crawl"] = total_pages
                msg_dict["total_products"] = total_count
                msg_dict["page_size"] = self.page_size
                for page in range(current_page, total_pages):
                    query['start'] = page * self.page_size
                    url_parts[4] = urlencode(query)
                    next_url = urlunparse(url_parts)
                    new_msg_dict = msg_dict.copy()
                    new_msg_dict["forced_url"] = next_url
                    new_msg_dict["start_rank"] = page * self.page_size
                    new_msg_dict["current_retry"] = 0
                    new_msg_dict["current_page"] = page + 1
                    self.push_next_page_to_crawl(new_msg_dict=new_msg_dict)
            msg_dict.pop("forced_url", None)
            products = self.get_product_collection_j_path(category_json, crawl_type="category")
            product_collection = []
            for p in products:
                rank += 1
                pd = self.populate_product_details_j_path(p, {"rank": rank},
                                                          self.paths.get("category").get("product_details", {}))
                handle = self.j_extract_first(p, "item_product_name").lower().split(" ")
                handle = "-".join(handle)
                # url = handle.replace('(', '').replace(')', '').replace('.', '').replace('/', '').replace('"','').replace(',','')
                url = re.sub(r'[^a-zA-Z0-9 \-]', '', handle)
                group_id = self.j_extract_first(p, '$.group_id')
                pd["pdp_url"] = self.formatter.get("pdp_url", "").format(url, group_id)
                mrp = self.j_extract_first(p, "$.item_location_pricing_listPrice")
                selling_price = self.j_extract_first(p, "minSalePrice")
                review_count = self.j_extract_first(p, "item_product_review_count", [None])
                ratings = self.j_extract_first(p, "item_ratings", [None])
                online_availability = self.j_extract_first(p, "isItemInStock")
                self.push_ranked_product_to_crawl(pd=pd, msg_dict=msg_dict,
                                                  extra={"review_count": review_count, "rating": ratings, "mrp": mrp,
                                                         "selling_price": selling_price,
                                                         "online_availability": online_availability,
                                                         "nimble": True, "nimble_config": {"format": "html",
                                                                                           "render": True,
                                                                                           "render_flow": [
                                                                                               {"wait": {
                                                                                                   "delay": 60000}}]}
                                                         })
                
                product_collection.append(pd)
            yield self.populate_category_item(response, msg_dict, products=product_collection)
            self.delete_sqs_message(message)
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            line_number = exc_traceback.tb_lineno
            self.logger.info(f"Error {e} on line No. {line_number}")
