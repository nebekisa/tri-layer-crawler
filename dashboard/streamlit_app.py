"""
Real-time intelligence dashboard using Streamlit.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database.manager import get_db_session
from src.database.models import CrawledItem, AnalysisResultDB, ExtractedEntityDB
from src.intelligence.engine import IntelligenceEngine

st.set_page_config(
    page_title="Tri-Layer Intelligence",
    page_icon="🕷️",
    layout="wide"
)

st.title("🕷️ Tri-Layer Intelligence Crawler")
st.markdown("### Real-time Web Intelligence Dashboard")

# Sidebar
st.sidebar.header("Controls")
timeframe = st.sidebar.selectbox("Timeframe", [1, 6, 12, 24, 48], index=2)
refresh = st.sidebar.button("🔄 Refresh")

# Main metrics
col1, col2, col3, col4 = st.columns(4)

session = get_db_session()

# Get stats
total_items = session.query(CrawledItem).count()
analyzed_items = session.query(AnalysisResultDB).count()
entities_found = session.query(ExtractedEntityDB).count()

# Sentiment distribution
from sqlalchemy import func
sentiment_counts = session.query(
    AnalysisResultDB.sentiment_label,
    func.count(AnalysisResultDB.id)
).group_by(AnalysisResultDB.sentiment_label).all()

with col1:
    st.metric("Total Crawled", total_items)
with col2:
    st.metric("Analyzed", analyzed_items)
with col3:
    st.metric("Entities Found", entities_found)
with col4:
    positive = sum(c for l, c in sentiment_counts if l == 'positive')
    st.metric("Positive Sentiment", f"{positive/analyzed_items*100:.1f}%" if analyzed_items else "0%")

# Intelligence Report
st.header("🧠 Intelligence Report")

if st.button("Generate Intelligence Report"):
    with st.spinner("Analyzing data..."):
        engine = IntelligenceEngine()
        report = engine.generate_report(timeframe_hours=timeframe)
        
        st.success(f"Report generated from {report.total_documents_analyzed} documents")
        
        # Key findings
        st.subheader("🔑 Key Findings")
        for finding in report.key_findings:
            st.write(f"• {finding}")
        
        # Entity correlations
        if report.entity_correlations:
            st.subheader("🔗 Top Entity Correlations")
            df = pd.DataFrame([{
                'Entity': c.entity_name,
                'Sources': len(c.sources),
                'Confidence': f"{c.confidence:.1%}",
                'Occurrences': c.occurrence_count
            } for c in report.entity_correlations[:10]])
            st.dataframe(df, use_container_width=True)
        
        # Sentiment trends
        if report.sentiment_trends:
            st.subheader("📈 Sentiment Trends")
            trend_data = []
            for t in report.sentiment_trends[:5]:
                trend_data.append({
                    'Entity': t.entity_or_topic,
                    'Direction': t.trend_direction,
                    'Change': t.change_magnitude,
                    'Confidence': f"{t.confidence:.1%}"
                })
            st.dataframe(pd.DataFrame(trend_data), use_container_width=True)

# Recent crawls
st.header("📋 Recent Crawls")
recent = session.query(CrawledItem).order_by(
    CrawledItem.crawled_at.desc()
).limit(10).all()

if recent:
    df = pd.DataFrame([{
        'URL': r.url[:50] + '...',
        'Title': r.title[:40] + '...',
        'Domain': r.domain,
        'Crawled': r.crawled_at.strftime('%Y-%m-%d %H:%M')
    } for r in recent])
    st.dataframe(df, use_container_width=True)

session.close()

# Auto-refresh
if refresh:
    st.rerun()