import sqlite3
from datetime import datetime, timedelta
from functools import partial

import pandas as pd
import streamlit as st


conn = sqlite3.connect("sqlite_default.db")

cursor = conn.cursor()
start = datetime.today() - timedelta(days=30)

start = str(st.sidebar.date_input("earliest product appear date", start))

cursor.execute(f"Select * from UniqloProducts Where appear_date >= '{start}'")
a = cursor.fetchall()


def edit_tracking(key):
    changed_status = st.session_state[key]

    update_query = f"""
    UPDATE UniqloProducts
    SET is_tracking = '{changed_status}'
    WHERE UniqloProducts.product_id_tw = '{key}'
    """
    cursor.execute(update_query)
    conn.commit()


def fetch_pricing(key):
    cursor.execute(f"Select * from UniqloProductPrice Where product_id_tw = '{key}'")
    a = cursor.fetchall()

    df = pd.DataFrame(a, columns=['product_id', 'date', 'price', 'region', 'currency'])
    return df[df['price'] != -1]


def fetch_ranking(key):
    cursor.execute(f"Select * from UniqloProductRanks Where product_id_tw = '{key}'")
    a = cursor.fetchall()

    df = pd.DataFrame(a, columns=['product_id', 'date', 'rank', 'region'])
    return df


keep_tracking_only = st.sidebar.radio("Keep is_tracking only?", [False, True])
title_filter = st.sidebar.text_input(
    'title_filter: filter title with given words' 
)

queue = st.columns(5)

for product_id, l1id, title, image_url, is_tracking, meta, appear_date in a:

    display_title = f"{title}, {l1id}, {appear_date}"
    if title_filter and not (title_filter not in display_title):
        continue

    if isinstance(is_tracking, str) and is_tracking == 'True':
        radio_index = 1
    elif isinstance(is_tracking, bool) and is_tracking:
        radio_index = 1
    else:
        if keep_tracking_only:
            continue
        radio_index = 0

    if not queue:
        queue.extend(st.columns(5))

    col = queue.pop()

    with col:
        st.text(display_title)
        st.image(image_url)
        is_clicked = st.button('get-detail', key=product_id)
        genre = st.radio(
            "is_tracking",
            (False, True),
            key=product_id,
            index=radio_index,
            on_change=partial(edit_tracking, product_id),
        )
    if is_clicked:
        df = fetch_pricing(product_id)
        st.dataframe(df)
        df = fetch_ranking(product_id)
        st.dataframe(df)
