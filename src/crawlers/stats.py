"""
Crawler statistics and monitoring.
"""

import time
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


@dataclass
class CrawlStats:
    """Track crawler performance metrics."""
    
    start_time: float = field(default_factory=time.time)
    end_time: float = 0.0
    urls_attempted: int = 0
    urls_succeeded: int = 0
    urls_failed: int = 0
    urls_skipped: int = 0  # ✅ NEW: Track skipped URLs
    total_bytes_downloaded: int = 0
    retry_count: int = 0
    errors: List[str] = field(default_factory=list)
    skips: List[str] = field(default_factory=list)  # ✅ NEW: Track skip reasons
    domain_stats: Dict[str, Dict] = field(default_factory=dict)
    
    def record_attempt(self, url: str):
        """Record a crawl attempt."""
        self.urls_attempted += 1
    
    def record_success(self, url: str, bytes_downloaded: int, domain: str):
        """Record a successful crawl."""
        self.urls_succeeded += 1
        self.total_bytes_downloaded += bytes_downloaded
        
        if domain not in self.domain_stats:
            self.domain_stats[domain] = {'success': 0, 'failed': 0, 'skipped': 0, 'bytes': 0}
        self.domain_stats[domain]['success'] += 1
        self.domain_stats[domain]['bytes'] += bytes_downloaded
    
    def record_failure(self, url: str, error: str, domain: str = None):
        """Record a failed crawl."""
        self.urls_failed += 1
        self.errors.append(f"{url}: {error}")
        
        if domain:
            if domain not in self.domain_stats:
                self.domain_stats[domain] = {'success': 0, 'failed': 0, 'skipped': 0, 'bytes': 0}
            self.domain_stats[domain]['failed'] += 1
    
    def record_skipped(self, url: str, reason: str, domain: str = None):
        """✅ NEW METHOD: Record a skipped URL (e.g., blocked by robots.txt)."""
        self.urls_skipped += 1
        self.skips.append(f"{url}: {reason}")
        logger.debug(f"Skipped {url}: {reason}")
        
        if domain:
            if domain not in self.domain_stats:
                self.domain_stats[domain] = {'success': 0, 'failed': 0, 'skipped': 0, 'bytes': 0}
            self.domain_stats[domain]['skipped'] += 1
    
    def record_retry(self):
        """Record a retry attempt."""
        self.retry_count += 1
    
    def finish(self):
        """Mark crawl as complete."""
        self.end_time = time.time()
    
    @property
    def elapsed_seconds(self) -> float:
        """Total crawl duration."""
        if self.end_time > 0:
            return self.end_time - self.start_time
        return time.time() - self.start_time
    
    @property
    def success_rate(self) -> float:
        """Percentage of successful crawls (excluding skipped)."""
        if self.urls_attempted == 0:
            return 0.0
        return (self.urls_succeeded / self.urls_attempted) * 100
    
    @property
    def effective_success_rate(self) -> float:
        """Success rate considering only actually crawled URLs (excludes skipped)."""
        crawled_total = self.urls_succeeded + self.urls_failed
        if crawled_total == 0:
            return 0.0
        return (self.urls_succeeded / crawled_total) * 100
    
    def summary(self) -> str:
        """Generate a summary report."""
        lines = [
            "=" * 50,
            "CRAWL STATISTICS",
            "=" * 50,
            f"Duration: {self.elapsed_seconds:.2f} seconds",
            f"URLs Attempted: {self.urls_attempted}",
            f"URLs Succeeded: {self.urls_succeeded}",
            f"URLs Failed: {self.urls_failed}",
            f"URLs Skipped: {self.urls_skipped}",  # ✅ NEW LINE
            f"Success Rate (overall): {self.success_rate:.1f}%",
            f"Success Rate (crawled only): {self.effective_success_rate:.1f}%",
            f"Total Downloaded: {self._format_bytes(self.total_bytes_downloaded)}",
            f"Total Retries: {self.retry_count}",
            "",
            "Per Domain:",
        ]
        
        for domain, stats in self.domain_stats.items():
            lines.append(f"  {domain}:")
            lines.append(f"    Success: {stats['success']}, Failed: {stats['failed']}, Skipped: {stats.get('skipped', 0)}")
            lines.append(f"    Data: {self._format_bytes(stats['bytes'])}")
        
        if self.errors:
            lines.append("")
            lines.append("Errors (first 5):")
            for error in self.errors[:5]:
                lines.append(f"  - {error}")
        
        if self.skips:
            lines.append("")
            lines.append("Skipped URLs (first 5):")
            for skip in self.skips[:5]:
                lines.append(f"  - {skip}")
        
        lines.append("=" * 50)
        return "\n".join(lines)
    
    @staticmethod
    def _format_bytes(bytes_count: int) -> str:
        """Format bytes to human readable."""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if bytes_count < 1024:
                return f"{bytes_count:.1f} {unit}"
            bytes_count /= 1024
        return f"{bytes_count:.1f} TB"
    
    def to_dict(self) -> Dict:
        """Export stats as dictionary for JSON/API responses."""
        return {
            'duration_seconds': self.elapsed_seconds,
            'urls_attempted': self.urls_attempted,
            'urls_succeeded': self.urls_succeeded,
            'urls_failed': self.urls_failed,
            'urls_skipped': self.urls_skipped,
            'success_rate_overall': round(self.success_rate, 2),
            'success_rate_crawled': round(self.effective_success_rate, 2),
            'total_bytes_downloaded': self.total_bytes_downloaded,
            'total_bytes_formatted': self._format_bytes(self.total_bytes_downloaded),
            'retry_count': self.retry_count,
            'domain_stats': self.domain_stats,
            'error_count': len(self.errors),
            'skip_count': len(self.skips),
        }