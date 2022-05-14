import requests
import json
from datetime import datetime

from airflow import DAG
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.sqlite.hooks.sqlite import SqliteHook


headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.102 Safari/537.36"  # 使用者代理
}


def get_newly_released_products():
    # [TODO]check meaningful sections and stores it
    # List[(product_id, section)]

    query = "https://www.uniqlo.com/tw/data/config_1/zh_TW/women_new_arrival_51737.json?t=1630495061237"
    response = requests.get(query, headers=headers)
    res = json.loads(response.text)
    items_list = []
    for sct in res.values():  # sct = res["section22"]
        if sct["componentType"] == "productRecommed":
            sct_name = sct["name"]  # "UT (印花 T 恤)"
            sct_items = sct["props"][0]["props"]
            # print(sct_name, len(sct_items))
            for item in sct_items:
                product_id = item["productCode"]
                items_list.append((product_id, sct_name))
    return items_list


def get_top_ten_products():
    # [TODO] check meaningful sections
    # [(product_id, rank, section)]
    top_10_query = (
        "https://www.uniqlo.com/tw/data/config_1/zh_TW/top_10_51605.json?t=1630495061237"
    )
    response = requests.get(top_10_query, headers=headers)
    res = json.loads(response.text)
    top10_list = []
    for sct in res.values():  # sct = res["section22"]
        if sct["componentType"] == "productRecommed":
            sct_name = sct["name"]  # "男裝"
            sct_items = sct["props"][0]["props"]
            # print(sct_name, len(sct_items))
            for idx,item in enumerate(sct_items):
                product_id = item["productCode"]
                top10_list.append((product_id,idx+1,sct_name))
    return top10_list


def fetch_product_info(product_id):
    # return title, product_id, product_id_jp, price, image_url
    url = f'https://www.uniqlo.com/tw/data/products/prodInfo/zh_TW/{product_id}.json'
    r = requests.get(url, headers=headers)
    res = r.json()
    name = res["name"]
    l1Id = res["code"]
    price = res["minPrice"]  # minprice
    mainpic = res.get("mainPic")
    if mainpic:
        image_url = f"https://www.uniqlo.com/tw{mainpic}"
    else:
        image_url = f"https://www.uniqlo.com/tw/hmall/test/{product_id}/main/first/561/1.jpg"
    # print (name, l1Id, price, image_url)
    return name, l1Id, price, image_url


class DumpLatestProdsOperator(BaseOperator):
    @apply_defaults
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

    def execute(self, context):
        new_prods = get_newly_released_products()
        top_ten_prods = get_top_ten_products()
        sqlite_hook = SqliteHook()
        # prods that already fetched profiles, needn't fetch again
        prods_fetched = sqlite_hook.get_pandas_df(sql='SELECT product_id_tw FROM UniqloProducts')

        prods_to_fetch = set(
            prod_id
            for prod_id, *_ in new_prods
            if prod_id not in set(prods_fetched['product_id_tw'])
        )
        prods_to_fetch |= set(
            prod_id
            for prod_id, *_ in top_ten_prods
            if prod_id not in set(prods_fetched['product_id_tw'])
        )

        # the date
        today_str = datetime.today().strftime('%Y-%m-%d')
        logs = []
        for prod in prods_to_fetch:
            try:
                res = fetch_product_info(prod)

                title, l1id, _, image_url = res
                logs.append((prod, l1id, title, image_url, False, '', today_str))
            except Exception as e:
                print(e)

        print(f"inserts {len(logs)} logs into db")

        sqlite_hook.insert_rows(table='UniqloProducts', rows=logs)
        return


with DAG(
    dag_id='fetch_latest_product_info',
    start_date=datetime(2021, 1, 1),
    schedule_interval='@daily',
    tags=['uniqlo'],
    catchup=False,
) as dag:
    DumpLatestProdsOperator(
        task_id='fetch-latest-product-info',
        dag=dag,
    )