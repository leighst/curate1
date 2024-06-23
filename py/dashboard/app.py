import os
import sqlite3

import pandas as pd
import streamlit as st


def load_data():
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
df = load_data()
df['hourly_date'] = pd.to_datetime(df['created_at'], unit='s').dt.floor('H')
grouped_df = df.groupby(['label', 'hourly_date']).size().reset_index(name='count')
#st.dataframe(grouped_df)

chart_data = df.groupby(['label', 'hourly_date']).size().unstack(fill_value=0).T
st.line_chart(chart_data)
