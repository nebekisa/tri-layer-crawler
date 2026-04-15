"""
Streamlit dashboard for crawler metrics.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path
import sys

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.repositories.db_repository import DatabaseRepository
from src.core.config_loader import get_settings

st.set_page_config(
    page_title="Tri-Layer Crawler Dashboard",
    page_icon="🕷️",
    layout="wide",
)

st.title("🕷️ Tri-Layer Intelligence Crawler Dashboard")
st.markdown("Real-time monitoring and analytics for web crawling operations")

# Sidebar
st.sidebar.header("Filters")
refresh = st.sidebar.button("🔄 Refresh Data")

# Load data
@st.cache_data(ttl=60)
def load_data():
    repo = DatabaseRepository()
    try:
        items = repo.get_all(limit=1000)
        df = pd.DataFrame([item.to_dict() for item in items])
        repo.close()
        return df
    except Exception as e:
        st.error(f"Failed to load data: {e}")
        return pd.DataFrame()

df = load_data()

if df.empty:
    st.warning("No data available. Run the crawler first!")
    st.stop()

# Metrics row
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Total URLs Crawled", len(df))

with col2:
    unique_domains = df['domain'].nunique() if 'domain' in df else 0
    st.metric("Unique Domains", unique_domains)

with col3:
    avg_content_length = int(df['content_length'].mean()) if 'content_length' in df else 0
    st.metric("Avg Content Length", f"{avg_content_length:,} chars")

with col4:
    success_rate = (df['status_code'] == 200).mean() * 100 if 'status_code' in df else 0
    st.metric("Success Rate", f"{success_rate:.1f}%")

# Charts
st.header("📊 Analytics")

col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Crawls by Domain")
    if 'domain' in df.columns and not df.empty:
        domain_counts = df['domain'].value_counts().reset_index()
        domain_counts.columns = ['Domain', 'Count']
        fig = px.pie(domain_counts, values='Count', names='Domain', hole=0.4)
        st.plotly_chart(fig, width='stretch')

with col_right:
    st.subheader("Content Length Distribution")
    if 'content_length' in df.columns and not df.empty:
        fig = px.histogram(df, x='content_length', nbins=20, title=None)
        fig.update_layout(xaxis_title="Content Length (chars)", yaxis_title="Frequency")
        st.plotly_chart(fig, width='stretch')
# Recent crawls table
st.header("📋 Recent Crawls")
if 'crawled_at' in df.columns and not df.empty:
    # Fix timestamp format - remove duplicate timezone
    df['crawled_at'] = df['crawled_at'].str.replace(r'-\d{2}:\d{2}Z$', 'Z', regex=True)
    df['crawled_at'] = pd.to_datetime(df['crawled_at'], format='ISO8601', errors='coerce')
    
    display_df = df[['url', 'title', 'domain', 'status_code', 'content_length', 'crawled_at']].copy()
    display_df = display_df.sort_values('crawled_at', ascending=False).head(20)
    
    # Format datetime for display
    display_df['crawled_at'] = display_df['crawled_at'].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    st.dataframe(display_df, width='stretch', hide_index=True)
else:
    st.info("No crawl data available yet. Run the crawler first!")

# Raw data export
#use_container_width
st.header("📥 Export Data")
csv = df.to_csv(index=False).encode('utf-8')
st.download_button(
    label="Download CSV",
    data=csv,
    file_name=f"crawler_export_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
    mime="text/csv",
)

# Footer
st.sidebar.markdown("---")
st.sidebar.markdown(f"**Last Updated:** {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
st.sidebar.markdown(f"**Database:** {get_settings().database.type}")