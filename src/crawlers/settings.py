"""
Scrapy Settings for Tri-Layer Intelligence Crawler.

This file configures the Scrapy engine. Settings are loaded from our
centralized settings.yaml via the ConfigLoader where appropriate.

Documentation: https://docs.scrapy.org/en/latest/topics/settings.html
"""

from src.core.config_loader import get_settings

# Load our application settings
app_settings = get_settings()

# -----------------------------------------------------------------------------
# Bot Identity
# -----------------------------------------------------------------------------
BOT_NAME = app_settings.crawler.name
USER_AGENT = app_settings.crawler.user_agent

# -----------------------------------------------------------------------------
# Crawling Behavior (Politeness)
# -----------------------------------------------------------------------------
# Respect robots.txt (True in production, False for our controlled testing)
ROBOTSTXT_OBEY = True

# Download delay between requests to same domain (from config)
DOWNLOAD_DELAY = app_settings.crawler.download_delay

# Maximum concurrent requests (from config)
CONCURRENT_REQUESTS = app_settings.crawler.concurrent_requests

# -----------------------------------------------------------------------------
# Pipelines (Order Matters!)
# -----------------------------------------------------------------------------
# Lower numbers = higher priority (executed first)
# 100: Validation MUST happen before writing
# 200: CSV writing happens after validation passes
ITEM_PIPELINES = {
    'src.crawlers.pipelines.ValidationPipeline': 100,
    'src.crawlers.pipelines.CsvWriterPipeline': 200,
}

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
LOG_LEVEL = app_settings.storage.log_level
LOG_FILE = 'logs/scrapy.log'
LOG_STDOUT = True  # Also print logs to console

# -----------------------------------------------------------------------------
# Extensions (Optional - we'll add more later)
# -----------------------------------------------------------------------------
# EXTENSIONS = {
#    'scrapy.extensions.telnet.TelnetConsole': None,  # Disable telnet for security
# }

# -----------------------------------------------------------------------------
# Feed Exports (We're using custom pipeline instead, but keep this disabled)
# -----------------------------------------------------------------------------
FEED_EXPORT_ENCODING = 'utf-8'