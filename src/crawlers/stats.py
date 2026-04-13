"""
Crawler statistics and monitoring.
"""

import time
from dataclasses import dataclass, field
from typing import Dict, List
from datetime import datetime


@dataclass
class CrawlStats:
    """Track crawler performance metrics."""
    
    start_time: float = field(default_factory=time.time)
    end_time: float = 0.0
    urls_attempted: int = 0
    urls_succeeded: int = 0
    urls_failed: int = 0
    total_bytes_downloaded: int = 0
    retry_count: int = 0
    errors: List[str] = field(default_factory=list)
    domain_stats: Dict[str, Dict] = field(default_factory=dict)
    
    def record_attempt(self, url: str):
        """Record a crawl attempt."""
        self.urls_attempted += 1
    
    def record_success(self, url: str, bytes_downloaded: int, domain: str):
        """Record a successful crawl."""
        self.urls_succeeded += 1
        self.total_bytes_downloaded += bytes_downloaded
        
        if domain not in self.domain_stats:
            self.domain_stats[domain] = {'success': 0, 'failed': 0, 'bytes': 0}
        self.domain_stats[domain]['success'] += 1
        self.domain_stats[domain]['bytes'] += bytes_downloaded
    
    def record_failure(self, url: str, error: str, domain: str = None):
        """Record a failed crawl."""
        self.urls_failed += 1
        self.errors.append(f"{url}: {error}")
        
        if domain:
            if domain not in self.domain_stats:
                self.domain_stats[domain] = {'success': 0, 'failed': 0, 'bytes': 0}
            self.domain_stats[domain]['failed'] += 1
    
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
        """Percentage of successful crawls."""
        if self.urls_attempted == 0:
            return 0.0
        return (self.urls_succeeded / self.urls_attempted) * 100
    
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
            f"Success Rate: {self.success_rate:.1f}%",
            f"Total Downloaded: {self._format_bytes(self.total_bytes_downloaded)}",
            f"Total Retries: {self.retry_count}",
            "",
            "Per Domain:",
        ]
        
        for domain, stats in self.domain_stats.items():
            lines.append(f"  {domain}:")
            lines.append(f"    Success: {stats['success']}, Failed: {stats['failed']}")
            lines.append(f"    Data: {self._format_bytes(stats['bytes'])}")
        
        if self.errors:
            lines.append("")
            lines.append("Errors (first 5):")
            for error in self.errors[:5]:
                lines.append(f"  - {error}")
        
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