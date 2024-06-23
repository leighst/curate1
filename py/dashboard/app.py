import os
import sqlite3

import pandas as pd
import plotly.express as px
import streamlit as st


def load_doc_attribs():
  db_path = os.getenv('SQLITE_DATABASE_PATH')
  if db_path is None:
    raise ValueError("SQLITE_DATABASE_PATH environment variable is not set.")
  conn = sqlite3.connect(db_path)
  query = '''
    SELECT *
    FROM document_attribute
  '''
  df = pd.read_sql_query(query, conn)
  conn.close()
  return df

st.title('Annotations')
df = load_doc_attribs()
df['hourly_date'] = pd.to_datetime(df['created_at'], unit='s').dt.floor('H')
#st.dataframe(grouped_df)

chart_data = df.groupby(['label', 'hourly_date']).size().unstack(fill_value=0).T
st.line_chart(chart_data)

# Load data from the documents table
def load_documents():
  db_path = os.getenv('SQLITE_DATABASE_PATH')
  if db_path is None:
    raise ValueError("SQLITE_DATABASE_PATH environment variable is not set.")
  conn = sqlite3.connect(db_path)
  query = '''
    SELECT created_at, content
    FROM document
  '''
  df = pd.read_sql_query(query, conn)
  conn.close()
  return df

# Process and display the data
documents_df = load_documents()
documents_df['hourly_date'] = pd.to_datetime(documents_df['created_at'], unit='s').dt.floor('H')
doc_count_over_time = documents_df.groupby('hourly_date').size()

st.title('Documents')
st.line_chart(doc_count_over_time)

document_lengths = documents_df['content'].str.len()
st.title('Document Size Distribution')
fig = px.histogram(document_lengths, nbins=50, title='Document Size Distribution')
st.plotly_chart(fig)
