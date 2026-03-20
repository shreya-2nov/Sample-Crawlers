# 🛒 Retail Web Scraping Project (Scrapy-Based)

This project demonstrates production-level web scraping implementations for major e-commerce platforms using **Scrapy**.

It includes sample scrapers for:

* Amazon
* Costco
* Adidas (US)

> ⚠️ Note: These are **simplified versions** of production scrapers. The full implementation in a real-world environment includes additional modules, pipelines, proxy management, and queue systems which are not included here.

---

## 🚀 Project Overview

The goal of this project is to showcase how large-scale retail data scraping works in real-world scenarios, including:

* Product data extraction (PDP)
* Category/listing scraping (PLP)
* Pagination handling
* API reverse engineering
* Anti-bot handling strategies
* Structured data mapping

---

## 🧠 Key Concepts Demonstrated

### 1. Scrapy-Based Architecture

Each scraper is built using a modular architecture inspired by production systems:

* Spider logic separated from parsing logic
* Config-driven field extraction (`paths` mapping)
* Reusable base class (`Retailer`)

---

### 2. API-Based Scraping (Instead of HTML)

Instead of relying only on HTML parsing, these scrapers:

* Identify internal APIs used by websites
* Fetch structured JSON data directly
* Improve reliability and performance

Example:

* Adidas uses internal APIs like:

  * `/api/products/{sku}`
  * `/availability`
  * `/ratings`

---

### 3. Product Data Extraction (PDP)

Each scraper extracts:

* Product Name
* Brand
* Price (MRP & Selling Price)
* Images
* Description
* Features
* Ratings & Reviews
* Variants (size, stock, availability)

---

### 4. Category Scraping (PLP)

Handles:

* Product listing pages
* Pagination
* Ranking of products
* Total product count

---

### 5. Handling Anti-Bot Mechanisms

Different websites have different levels of protection:

| Website | Difficulty | Strategy                    |
| ------- | ---------- | --------------------------- |
| Adidas  | Moderate   | Headers + API calls + Proxy |
| Amazon  | High       | Headers + Delay + Selectors |
| Costco  | Moderate   | Session handling            |

Techniques used:

* Custom headers (User-Agent, etc.)
* Proxy support
* Request fingerprinting (e.g., `curl_cffi`)
* Retry mechanisms

---

### 6. Proxy & Middleware Support

Example (Adidas):

* Uses private proxy server
* Custom middleware integration:

  * Proxy middleware
  * curl_cffi for browser-like requests

---

### 7. Data Normalization via Mapping

Instead of hardcoding extraction logic everywhere, a **config-driven approach** is used:

```python
"product_name": {
    "paths": ["name"],
    "default": ""
}
```

This allows:

* Easy maintenance
* Scalable scraper design
* Faster onboarding for new retailers

---

## 📁 Project Structure

```
.
├── Amazon.py
├── Costco.py
├── Adidas.py
└── README.md
```

Each file represents a standalone scraper for a specific retailer.

---

## ⚙️ Example: Adidas Scraper Highlights

* Uses internal Adidas API for product data
* Handles:

  * Product details
  * Variant availability
  * Ratings via separate API
* Implements:

  * Multi-step request flow
  * Queue-based crawling (simulated here)
  * Retry on blocking (403)

---

## ⚠️ Limitations

* This is **not a runnable standalone project**
* Missing components:

  * Base framework (`Retailer` class)
  * Queue system (SQS/Kafka)
  * Proxy infrastructure
  * Middleware implementations

These are intentionally excluded as they belong to a larger production system.

---

## 🎯 Purpose of This Repository

This project is intended to:

* Demonstrate real-world scraping experience
* Showcase scalable scraper architecture
* Highlight handling of complex e-commerce platforms
* Serve as a reference for learning advanced scraping techniques

---

## 💡 Skills Demonstrated

* Python & Scrapy
* Web scraping at scale
* API reverse engineering
* Data extraction & normalization
* Handling anti-bot systems
* System design for crawlers

---

## 📌 Final Note

Even though this repository contains partial implementations, it reflects **production-grade thinking and design patterns** used in large-scale data extraction systems.

---

⭐ If you found this helpful, feel free to explore and build upon it!
