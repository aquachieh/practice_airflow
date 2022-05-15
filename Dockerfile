FROM apache/airflow:2.3.0

USER airflow
RUN mkdir /uniqlo-sqlite
RUN pip install streamlit pandas