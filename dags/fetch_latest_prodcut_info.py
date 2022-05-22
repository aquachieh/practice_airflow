import requests
import json
from datetime import datetime
import time

from airflow import DAG
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.sqlite.hooks.sqlite import SqliteHook


headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.102 Safari/537.36"  # noqa
}


def get_latest_prop_json_name(prop):
    query = "https://www.uniqlo.com/tw/data/config_1/zh_TW/cms-mobile-config.json"
    response = requests.get(query, headers=headers)
    res = json.loads(response.text)
    props = res['pages'][prop]['props'][-1]
    filename = f"{props['componentsPath']}?t={props['startDate']}"
    return filename


def get_newly_released_products():
    # [TODO]check meaningful sections and stores it
    # List[(product_id, section)]
    filename = get_latest_prop_json_name('/home/women_new_arrival')
    query = f"https://www.uniqlo.com/tw/data/config_1/zh_TW/{filename}"  # noqa
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
    filename = get_latest_prop_json_name('/home/top_10')
    top_10_query = f"https://www.uniqlo.com/tw/data/config_1/zh_TW/{filename}"
    response = requests.get(top_10_query, headers=headers)
    res = json.loads(response.text)
    top10_list = []
    for sct in res.values():  # sct = res["section22"]
        if sct["componentType"] == "productRecommed":
            sct_name = sct["name"]  # "男裝"
            sct_items = sct["props"][0]["props"]
            # print(sct_name, len(sct_items))
            for idx, item in enumerate(sct_items):
                product_id = item["productCode"]
                top10_list.append((product_id, idx + 1, sct_name))
    return top10_list


def fetch_product_info(product_id):
    # return title, product_id, product_id_jp, price, image_url
    url = f'https://www.uniqlo.com/tw/data/products/prodInfo/zh_TW/{product_id}.json'
    r = requests.get(url, headers=headers)
    res = r.json()
    name = res["name"]
    l1Id = res["code"]
    price_tw = res["minPrice"]  # minprice
    mainpic = res.get("mainPic")
    if mainpic:
        image_url = f"https://www.uniqlo.com/tw{mainpic}"
    else:
        image_url = (
            f"https://www.uniqlo.com/tw/hmall/test/{product_id}/main/first/561/1.jpg"  # noqa
        )
    # print (name, l1Id, price, image_url)
    return name, l1Id, price_tw, image_url


def get_jp_price(pdid):
    query = f"https://www.uniqlo.com/jp/api/commerce/v5/ja/products?q={pdid}"
    response = requests.get(query, headers=headers)
    res = json.loads(response.text)
    items_list = res["result"]["items"]
    for item in items_list:  # return first price
        l1Id = item["l1Id"]
        price_jp = item["prices"]["base"]["value"]
        if pdid == l1Id:  # check id
            return price_jp
    return -1


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
        product_logs = []
        price_logs = []
        rank_logs = []

        for prod_id_tw in prods_to_fetch:
            time.sleep(0.05)
            try:
                res = fetch_product_info(prod_id_tw)

                title, l1id, price_tw, image_url = res
                product_logs.append((prod_id_tw, l1id, title, image_url, False, '', today_str))

                price_jp = get_jp_price(l1id)
                price_logs.append((prod_id_tw, today_str, price_tw, "TW", "TWD"))  # TW
                price_logs.append((prod_id_tw, today_str, price_jp, "JP", "JPY"))  # JP
            except Exception as e:
                print(e)

        print(f"inserts {len(product_logs)} product_logs into db")
        print(f"inserts {len(price_logs)} price_logs into db")

        sqlite_hook.insert_rows(table='UniqloProducts', rows=product_logs)
        sqlite_hook.insert_rows(table='UniqloProductPrice', rows=price_logs)

        # insert UniqloProductRanks
        for item in top_ten_prods:
            prod_id_tw, rank, section = item
            rank_logs.append((prod_id_tw, today_str, rank, "TW"))

        sqlite_hook.insert_rows(table='UniqloProductRanks', rows=rank_logs)
        print(f"inserts {len(rank_logs)} rank_logs into db")
        return


with DAG(
    dag_id='fetch-latest-product-info',
    start_date=datetime(2021, 1, 1),
    schedule_interval='@daily',
    tags=['uniqlo'],
    catchup=False,
) as dag:
    DumpLatestProdsOperator(
        task_id='fetch-latest-product-info',
        dag=dag,
    )