# Short description
Project created to scrape hoodies category on dresslily.com


# Requirements
1. db==mongodb
2. python==3.6.0
3. pip requirements stored in requirements.txt

# Launch
python3 <products_file_name.csv> <reviews_file_name.csv>
result files would be in management folder

### helpers
Package with helpers module
2. downloader_helper.py - Requests library wrapper with proxy usage, errors handling, etc.
3. helpers.py - single helper functions
4. proxy_helper - Proxy error handlings, prioritization, filtering, etc.

### management
1. management.py - main launch module

### scrapers
1. dresslily.py - main scraping module with 2 classes (Scraper and Inner page parser)

### storage
1.mongodb_storage.py - database module

### config.ini
1. NAME, PRODUCTS_COLLECTION, IP, LOGIN, PASSWORD - database credentials
2. TEST_ENV - If set as True, would connect to localhost
3. proxy_key - best-proxies.ru proxy_key
