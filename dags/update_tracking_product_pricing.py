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
        image_url = f"https://www.uniqlo.com/tw/hmall/test/{product_id}/main/first/561/1.jpg"
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


class TrackingProdsPriceOperator(BaseOperator):
    @apply_defaults
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

    def execute(self, context):
        sqlite_hook = SqliteHook()

        tracking_prods = sqlite_hook.get_pandas_df(
            sql="SELECT product_id_tw FROM UniqloProducts Where is_tracking = 'True'"
        )
        print(f"update {len(tracking_prods)} prods")
        # the date
        today_str = datetime.today().strftime('%Y-%m-%d')
        price_logs = []

        for prod in tracking_prods['product_id_tw']:
            time.sleep(0.05)
            try:
                res = fetch_product_info(prod)

                _, l1id, price_tw, _ = res

                price_jp = get_jp_price(l1id)
                price_logs.append((prod, today_str, price_tw, "TW", "TWD"))  # TW
                price_logs.append((prod, today_str, price_jp, "JP", "JPY"))  # JP
            except Exception as e:
                print(e)

        print(f"inserts {len(price_logs)} price_logs into db")

        sqlite_hook.insert_rows(table='UniqloProductPrice', rows=price_logs)
        return


with DAG(
    dag_id='update-tracking-product-price',
    start_date=datetime(2021, 1, 1),
    schedule_interval='@daily',
    tags=['uniqlo'],
    catchup=False,
) as dag:
    TrackingProdsPriceOperator(
        task_id='tracking-product-price',
        dag=dag,
    )