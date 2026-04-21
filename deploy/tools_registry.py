import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.web_fetch import web_fetch
from tools.web_search import web_search
from tools.db_query import db_query
from tools.browser_tool import browser_fetch, browser_screenshot, browser_interact
import requests

API = "http://localhost:8000"

def get_top_merchants():
    return requests.get(f"{API}/merchants/top_distressed", timeout=10).json()

def get_merchant_contacts(merchant_id):
    return requests.get(f"{API}/merchant_contacts/{merchant_id}", timeout=10).json()

def get_merchant_profile(merchant_id):
    return requests.get(f"{API}/merchants/{merchant_id}", timeout=10).json()

TOOLS = {
    "web_fetch": web_fetch,
    "web_search": web_search,
    "db_query": db_query,
    "browser_fetch": browser_fetch,
    "browser_screenshot": browser_screenshot,
    "browser_interact": browser_interact,
    "get_top_merchants": get_top_merchants,
    "get_merchant_contacts": get_merchant_contacts,
    "get_merchant_profile": get_merchant_profile,
}