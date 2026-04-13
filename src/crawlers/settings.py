# Minimal settings for reliability
BOT_NAME = 'cerberus'
SPIDER_MODULES = ['src.crawlers.spiders']
NEWSPIDER_MODULE = 'src.crawlers.spiders'
ROBOTSTXT_OBEY = True

# Use Scrapy's built-in feed export (bypasses our pipeline)
FEEDS = {
    'data/raw/items.json': {
        'format': 'json',
        'encoding': 'utf8',
        'ensure_ascii': False,
        'indent': 2,
        'overwrite': True,
    }
}

# Disable our custom pipelines for now
ITEM_PIPELINES = {}

# Logging
LOG_LEVEL = 'INFO'
LOG_FILE = 'logs/scrapy.log'