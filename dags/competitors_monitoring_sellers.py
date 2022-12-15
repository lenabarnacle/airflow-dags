from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.email import send_email

from dwh_resources import get_postgres_engine, get_clickhouse_connection

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from sqlalchemy import Text, Integer, Float


dag = DAG(
    "competitors_monitoring_sellers",
    description="Сбор данных и построение отчета Competitors monitoring (по продавцам)",
    schedule_interval="30 9 1 * *",
    start_date=datetime(2022, 12, 1),
    tags=["clickhouse", "powerbi", "reports"],
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "Pindiurina.EP",
        "sla": timedelta(minutes=15),
        "email": ["elena.p@carely.group"],
        "email_on_retry": False,
        "email_on_failure": True,
        "max_active_runs": 1,
    },
)


class ReplaceDoubleQuotes(str):
    def __repr__(self):
        return "".join(("'", super().__repr__()[1:-1], "'"))


def get_border_dates():

    """
    Определяет границы периода выбора данных для анализа

    Вход:
    --

    Выход:
    Первая дата текущего месяца,
    Левая граница периода выбора данных для анализа,
    Правая граница периода выбора данных для анализа
    """

    first_date_of_current_month = datetime.today().date().replace(day=1)
    start_date = first_date_of_current_month - relativedelta(months=13)
    end_date = first_date_of_current_month - timedelta(1)

    return first_date_of_current_month, start_date, end_date


def get_periods_list(first_date_of_current_month, start_date) -> list:

    """
    Генерирует список из 12 периодов, соответствующих последним 12 календарным месяцам (до 1 числа текущего месяца).
    Список не является необходимостью, но обеспечивает определенное удобство при дальнейшей работе с датами.

    Вход:
    Первая дата текущего месяца,
    Левая граница периода выбора данных для анализа

    Выход:
    Список периодов, соответствующих последним 12 календарным месяцам (далее "Список периодов").
    Например, в декабре 2022 г. список периодов выглядит следующим образом:
    ['2021-11',
     '2021-12',
     '2022-01',
     '2022-02',
     '2022-03',
     '2022-04',
     '2022-05',
     '2022-06',
     '2022-07',
     '2022-08',
     '2022-09',
     '2022-10',
     '2022-11']
    """

    periods_list = list()
    for i in range(
        (first_date_of_current_month.year - start_date.year) * 12
        + first_date_of_current_month.month
        - start_date.month
    ):
        periods_list.append(str(start_date + relativedelta(months=i))[:7])

    return periods_list


def get_clickhouse_data(
    client, start_date, end_date, subcategories: list
) -> pd.DataFrame:

    """ "
    Получает агрегированные по периодам данные по продажам в выбранных подкатегориях
    категории "Красота - Уход за кожей" на Wildberries из базы данных ClickHouse

    Вход:
    Клиент для подключения к базе данных ClickHouse,
    Левая граница периода выбора данных для анализа,
    Правая граница периода выбора данных для анализа,
    Список релевантных подкатегорий из категории "Красота - Уход за кожей" на Wildberries,

    Выход:
    Датафрейм с исходными данными
    """

    query = f"""
        with mpstats_cut as (
            select distinct
                mpstats.mpstats_id,
                mpstats.brand,
                mpstats.seller,
                mpstats.comments,
                mpstats.rating,
                mpstats.final_price,
                mpstats.sales,
                mpstats.revenue,
                mpstats."date",
                concat(
                    splitByChar('-',  coalesce(mpstats."date",'-'))[1], '-',
                    splitByChar('-',  coalesce(mpstats."date",'-'))[2]) as period
            from npddb.mps_wb_products mpstats
            where 1 = 1
                and mpstats."date" between '{str(start_date)}' and '{str(end_date)}'
                and splitByChar('/',  coalesce(mpstats.category,'-'))[1] = 'Красота'
                and splitByChar('/',  coalesce(mpstats.category,'-'))[2] = 'Уход за кожей'
                and splitByChar('/',  coalesce(mpstats.category,'-'))[3] in ({subcategories})
                and mpstats.brand <> ''
            )
        select mpstats_cut.brand,
            mpstats_cut.seller,
            mpstats_cut.mpstats_id,
            mpstats_cut.period,
            sum(mpstats_cut.revenue) as revenue
        from mpstats_cut
        group by mpstats_cut.brand,
            mpstats_cut.seller,
            mpstats_cut.mpstats_id,
            mpstats_cut.period
        having sum(mpstats_cut.revenue) <> 0
            """

    result, columns = client.execute(query, with_column_types=True)
    df = pd.DataFrame(result, columns=[tuple[0] for tuple in columns])
    df["seller"] = [x.replace("\\", "") for x in df["seller"]]
    df["brand"] = [x.replace("\\", "") for x in df["brand"]]

    return df


def get_sellers_mapping(df: pd.DataFrame) -> dict:

    """ "
    Создает словарь имен продавцов, в котором в качестве ключей используются все имеющиеся имена продавцов,
    а в качестве значений - актуальные имена продавцов. По своей сути это мэппинг имен продавцов. Мэппинг необходим потому,
    что имена продавцов часто изменяются, в то время как для корректной агрегации данных по продавцам (в том числе для отбора
    продавцов по объему выручки) имена продавцов должны быть уникальными.

    Вход:
    Датафрейм с исходными данными

    Выход:
    Мэппинг имен продавцов. Пример:
        {
        'ООО Эстилаб Рус': 'ООО Эстилаб РУС',
        'ООО ЭЛР': 'ООО Эстилаб РУС'
        }
    """

    diff_sellers = (
        df[["mpstats_id", "seller"]].groupby("mpstats_id").nunique().reset_index()
    )
    diff_sellers = diff_sellers[diff_sellers["seller"] > 1].reset_index(drop=True)
    diff_sellers = diff_sellers["mpstats_id"].to_list()

    sellers_mapping = dict()
    for i in diff_sellers:
        df_i = df[df["mpstats_id"] == i].reset_index(drop=True)
        df_i = df_i.sort_values(by="period").drop_duplicates(
            subset="seller", keep="last"
        )
        sellers = df_i["seller"].to_list()
        for i in range(len(sellers) - 1):
            sellers_mapping[sellers[i]] = sellers[-1]

    for k, v in sellers_mapping.items():
        if v in list(sellers_mapping.keys()):
            df_v = df[df["seller"].isin([v, sellers_mapping[v]])][
                ["seller", "period"]
            ].drop_duplicates()
            df_v = df_v.sort_values(by="period").reset_index(drop=True)
            sellers_mapping[k] = df_v["seller"][len(df_v) - 1]
            sellers_mapping[v] = df_v["seller"][len(df_v) - 1]

    sellers_mapping = {
        k: v
        for (k, v) in sellers_mapping.items()
        if k != v and "вайлдберриз" not in k.lower()
    }

    return sellers_mapping


def get_brands_mapping(df: pd.DataFrame) -> dict:

    """ "
    Создает словарь названий брендов, в котором в качестве ключей используются все имеющиеся названия брендов,
    а в качестве значений - актуальные названия брендов. По своей сути это мэппинг названий брендов. Мэппинг необходим потому,
    что названия брендов часто изменяются, в то время как для корректного подсчета количества брендов у каждого продавца
    названия брендов должны быть уникальными.

    Вход:
    Датафрейм с исходными данными

    Выход:
    Мэппинг названий брендов. Пример:
        {
            'Pixel Tap': 'PixelTap ic',
            'Pixel Tap ic': 'PixelTap ic'
        }
    """

    diff_brands = (
        df[["mpstats_id", "brand"]].groupby("mpstats_id").nunique().reset_index()
    )
    diff_brands = diff_brands[diff_brands["brand"] > 1].reset_index(drop=True)
    diff_brands = diff_brands["mpstats_id"].to_list()

    brands_mapping = dict()
    for i in diff_brands:
        df_i = df[df["mpstats_id"] == i].reset_index(drop=True)
        df_i = df_i.sort_values(by="date").drop_duplicates(subset="brand", keep="last")
        brands = df_i["brand"].to_list()
        for i in range(len(brands) - 1):
            brands_mapping[brands[i]] = brands[-1]

    for k, v in brands_mapping.items():
        if v in list(brands_mapping.keys()):
            df_v = df[df["brand"].isin([v, brands_mapping[v]])][
                ["brand", "date"]
            ].drop_duplicates()
            df_v = df_v.sort_values(by="date").reset_index(drop=True)
            brands_mapping[k] = df_v["brand"][len(df_v) - 1]
            brands_mapping[v] = df_v["brand"][len(df_v) - 1]

    brands_mapping = {k: v for (k, v) in brands_mapping.items() if k != v}

    return brands_mapping


def select_sellers(df: pd.DataFrame, sellers_rename: dict, periods_list: list) -> list:

    """ "
    Создает список продавцов (их актуальных имен), у которых выручка в последнем месяце превысила 2 млн руб.

    Вход:
    Датафрейм с исходными данными,
    Мэппинг имен продавцов,
    Список периодов

    Выход:
    Список имен продавцов, соответствующих условию по выручке
    """

    for k in sellers_rename.keys():
        df["seller"] = df["seller"].replace(k, sellers_rename[k])

    df = (
        df[["seller", "period", "revenue"]]
        .groupby(["seller", "period"])
        .sum()
        .reset_index()
    )

    sellers = list(
        df[(df["period"] == periods_list[-1]) & (df["revenue"] > 2000000)][
            "seller"
        ].unique()
    )
    for k, v in sellers_rename.items():
        if v in sellers:
            sellers.append(k)
    sellers = list(set(sellers))
    sellers = [x.replace("'", "''") for x in sellers]
    sellers = [ReplaceDoubleQuotes(x) for x in sellers]

    return sellers


def get_clickhouse_data_on_selected_sellers(
    client, start_date, end_date, subcategories: list, sellers: list
) -> pd.DataFrame:

    """
    Получает агрегированные по периодам данные по продажам в выбранных подкатегориях
    категории "Красота - Уход за кожей" на Wildberries из базы данных ClickHouse по выбранным продавцам

    Вход:
    Клиент для подключения к базе данных ClickHouse,
    Левая граница периода выбора данных для анализа,
    Правая граница периода выбора данных для анализа,
    Список релевантных подкатегорий из категории "Красота - Уход за кожей" на Wildberries,
    Список имен продавцов, соответствующих условию по выручке

    Выход:
    Датафрейм с данными
    """

    query = f"""
    select distinct
        mpstats.mpstats_id,
        mpstats.brand,
        mpstats.seller,
        mpstats.comments,
        mpstats.rating,
        mpstats.final_price,
        mpstats.sales,
        mpstats.revenue,
        mpstats."date",
        concat(
            splitByChar('-',  coalesce(mpstats."date",'-'))[1], '-',
            splitByChar('-',  coalesce(mpstats."date",'-'))[2]) as period
    from npddb.mps_wb_products mpstats
    where 1 = 1
        and (mpstats."date" between '{str(start_date)}' and '{str(end_date)}')
        and splitByChar('/',  coalesce(mpstats.category,'-'))[1] = 'Красота'
        and splitByChar('/',  coalesce(mpstats.category,'-'))[2] = 'Уход за кожей'
        and splitByChar('/',  coalesce(mpstats.category,'-'))[3] in ({subcategories})
        and mpstats.seller in ({sellers})
            """

    result, columns = client.execute(query, with_column_types=True)
    df = pd.DataFrame(result, columns=[tuple[0] for tuple in columns])

    df["seller"] = [x.replace("\\", "") for x in df["seller"]]
    df["brand"] = [x.replace("\\", "") for x in df["brand"]]

    return df


def prepare_df(
    df: pd.DataFrame, sellers_rename: dict, brands_rename: dict
) -> pd.DataFrame:
    # Подготавливает датафрейм с данными к расчетным манипуляциям

    df["mpstats_id"] = df["mpstats_id"].astype(str)
    df["date"] = [str(x)[:10] for x in df["date"]]
    df["date"] = pd.to_datetime(df["date"])

    sellers_rename = pd.DataFrame.from_dict(
        sellers_rename, orient="index"
    ).reset_index()
    sellers_rename = sellers_rename.rename(
        columns={"index": "seller_from", 0: "seller_to"}
    )

    df = df.merge(sellers_rename, how="left", left_on="seller", right_on="seller_from")
    df["seller"] = np.where(df["seller_from"].isna(), df["seller"], df["seller_to"])
    df = df.drop(["seller_from", "seller_to"], axis=1)

    brand_rename = pd.DataFrame.from_dict(brands_rename, orient="index").reset_index()
    brand_rename = brand_rename.rename(columns={"index": "brand_from", 0: "brand_to"})

    df = df.merge(brand_rename, how="left", left_on="brand", right_on="brand_from")
    df["brand"] = np.where(df["brand_from"].isna(), df["brand"], df["brand_to"])
    df = df.drop(["brand_from", "brand_to"], axis=1)

    return df


def get_sku_with_sales(df: pd.DataFrame, periods_list: list) -> pd.DataFrame:
    # Считает число sku с продажами для каждого продавца (последний месяц)

    sku_with_sales = (
        df[(df["period"] == periods_list[-1])]
        .groupby(["seller", "mpstats_id"])
        .agg(sales=pd.NamedAgg("sales", "sum"))
        .reset_index()
    )
    sku_with_sales = (
        sku_with_sales[sku_with_sales["sales"] != 0][["seller", "mpstats_id"]]
        .groupby(["seller"])
        .count()
    )
    sku_with_sales = sku_with_sales.rename(
        columns={"mpstats_id": "sku_with_sales_num"}
    ).reset_index()

    return sku_with_sales


def get_sku_num_prev(df: pd.DataFrame, periods_list: list) -> pd.DataFrame:
    # Считает число sku в предыдущем (относительно последнего месяца) месяце для каждого продавца
    # Это необходимо для последующего расчета числа новых sku у каждого продавца за последний месяц

    sku_num_prev = (
        df[df["period"] == periods_list[-2]]
        .groupby(["seller"])
        .agg(sku_num_prev=pd.NamedAgg("mpstats_id", "nunique"))
        .reset_index()
    )

    return sku_num_prev


def get_products_rating(df: pd.DataFrame) -> pd.DataFrame:
    # Определяет рейтинг продуктов на последнюю в заданном периоде (последний месяц) дату

    products_rating = df[
        ["seller", "mpstats_id", "period", "date", "comments", "rating"]
    ]
    products_rating = products_rating.sort_values(["seller", "mpstats_id", "date"])
    products_rating = products_rating.drop_duplicates(
        ["seller", "mpstats_id", "period"], keep="last"
    )
    products_rating = products_rating.drop(["date"], axis=1).reset_index(drop=True)

    return products_rating


def get_sellers_rating(
    products_rating: pd.DataFrame, periods_list: list
) -> pd.DataFrame:
    # Считает средний рейтинг продавца (последний месяц) и количество новых отзывов (последний месяц)

    new_comments = products_rating[products_rating["period"] == periods_list[-1]][
        ["seller", "mpstats_id", "comments"]
    ].merge(
        products_rating[products_rating["period"] == periods_list[-2]][
            ["seller", "mpstats_id", "comments"]
        ],
        how="inner",
        on=["seller", "mpstats_id"],
    )

    new_comments = (
        new_comments.groupby(["seller"])
        .agg(
            comments_x=pd.NamedAgg("comments_x", "sum"),
            comments_y=pd.NamedAgg("comments_y", "sum"),
        )
        .reset_index()
    )
    new_comments["new_comments"] = (
        new_comments["comments_x"] - new_comments["comments_y"]
    )

    sellers_rating = (
        products_rating[products_rating["period"] == periods_list[-1]]
        .groupby(["seller"])
        .agg(avg_rating=pd.NamedAgg("rating", "mean"))
        .reset_index()
    )
    sellers_rating = sellers_rating.merge(
        new_comments[["seller", "new_comments"]], how="left", on="seller"
    )

    return sellers_rating


def calculate_dynamic_metrics(
    df: pd.DataFrame, products_rating: pd.DataFrame, periods_list: list
) -> pd.DataFrame:
    """
    Рассчитывает различные показатели по состоянию на 3, 6, 12 мес. назад:
        - выручка,
        - средняя цена,
        - общее число sku,
        - выручка на sku,
        - число sku с продажами,
        - средний рейтинг продавца

    Это необходимо для анализа динамики указанных показателей.
    """

    dynamics = pd.DataFrame(
        {
            "seller": [],
            "revenue_period": [],
            "avg_price_period": [],
            "sku_num_period": [],
            "sku_sales_period": [],
            "sku_rev_period": [],
            "avg_rating_period": [],
            "period": [],
        }
    )

    for period in [0, 6, 9]:

        df_period = (
            df[df["period"] == periods_list[period]]
            .groupby(["seller"])
            .agg(
                revenue_period=pd.NamedAgg("revenue", "sum"),  # выручка
                avg_price_period=pd.NamedAgg("final_price", "mean"),  # средняя цена
                sku_num_period=pd.NamedAgg("mpstats_id", "nunique"),
            )
            .reset_index()
        )  # общее число sku
        df_period["sku_rev_period"] = (
            df_period["revenue_period"] / df_period["sku_num_period"]
        )  # выручка на sku

        # число sku с продажами
        sku_with_sales = (
            df[(df["period"] == periods_list[period])]
            .groupby(["seller", "mpstats_id"])
            .agg(sales=pd.NamedAgg("sales", "sum"))
            .reset_index()
        )
        sku_with_sales = (
            sku_with_sales[sku_with_sales["sales"] != 0][["seller", "mpstats_id"]]
            .groupby(["seller"])
            .count()
        )
        sku_with_sales = sku_with_sales.rename(
            columns={"mpstats_id": "sku_sales_period"}
        ).reset_index()
        df_period = df_period.merge(sku_with_sales, how="left", on="seller").fillna(0)

        # средний рейтинг продавца
        sellers_rating = (
            products_rating[products_rating["period"] == periods_list[period]]
            .groupby(["seller"])
            .agg(avg_rating_period=pd.NamedAgg("rating", "mean"))
            .reset_index()
        )
        df_period = df_period.merge(sellers_rating, how="left", on="seller").fillna(0)

        df_period["period"] = 12 - period

        dynamics = dynamics.append(df_period, ignore_index=True)

        return dynamics


def add_new_sellers(main_table: pd.DataFrame, dynamics: pd.DataFrame) -> pd.DataFrame:
    # Добавляет в датафрейм dynamics продавцов, появившихся на рынке менее 3 мес. назад

    new_sellers = main_table[~main_table["seller"].isin(dynamics["seller"])][
        ["seller"]
    ].reset_index(drop=True)
    new_sellers = new_sellers.merge(
        pd.DataFrame(dynamics["period"].unique()), how="cross"
    )
    new_sellers = new_sellers.rename(columns={0: "period"})
    dynamics = dynamics.append(new_sellers, ignore_index=True)

    return dynamics


def calculate_growth(main_table: pd.DataFrame) -> pd.DataFrame:
    # Рассчитывает прирост различных показателей за 3, 6, 12 мес.

    main_table["rev_g"] = (
        main_table["revenue"] / main_table["revenue_period"] - 1
    )  # выручка
    main_table["sku_g"] = (
        main_table["sku_num"] - main_table["sku_num_period"]
    )  # общее число sku
    main_table["sku_s_g"] = (
        main_table["sku_with_sales_num"] - main_table["sku_sales_period"]
    )  # число sku с продажами
    main_table["sku_rev_g"] = (
        main_table["sku_rev"] / main_table["sku_rev_period"] - 1
    )  # выручка на sku
    main_table["price_g"] = (
        main_table["avg_price"] / main_table["avg_price_period"] - 1
    )  # средняя цена
    main_table["rating_g"] = (
        main_table["avg_rating"] - main_table["avg_rating_period"]
    )  # средний рейтинг продавца

    return main_table


def update_database_data(main_table: pd.DataFrame):

    engine = get_postgres_engine("DWHPostgreSQL")
    main_table.to_sql(
        name="market_by_sellers_tmp",
        con=engine,
        if_exists="replace",
        schema="sa",
        chunksize=10024,
        index=False,
        method="multi",
        dtype={
            "seller": Text(),
            "revenue": Float(),
            "avg_price": Float(),
            "sku_num": Integer(),
            "brands": Integer(),
            "sku_rev": Float(),
            "sku_with_sales": Float(),
            "sku_new": Integer(),
            "avg_rating": Float(),
            "new_comments": Integer(),
            "period": Integer(),
            "rev_g": Float(),
            "sku_g": Integer(),
            "sku_s_g": Integer(),
            "sku_rev_g": Float(),
            "price_g": Float(),
            "rating_g": Float(),
        },
    )

    return


def __main__():

    # подкатегории отобраны продакт-менеджерами для бренда Art&Fact
    subcategories = [
        "Борьба с несовершенствами",
        "Уход для глаз и бровей",
        "Уход для губ",
        "Уход за лицом",
        "Уход за руками",
        "Уход за телом",
    ]

    first_date_of_current_month, start_date, end_date = get_border_dates()
    periods_list = get_periods_list(first_date_of_current_month, start_date)

    client = get_clickhouse_connection("ClickHouse")
    df = get_clickhouse_data(client, start_date, end_date, subcategories)

    sellers_mapping = get_sellers_mapping(df)
    sellers = select_sellers(df, sellers_mapping, periods_list)
    df = get_clickhouse_data_on_selected_sellers(
        client, start_date, end_date, subcategories, sellers
    )

    brands_mapping = get_brands_mapping(df)
    df = prepare_df(df, sellers_mapping, brands_mapping)

    main_table = (
        df[df["period"] == periods_list[-1]]
        .groupby(["seller"])
        .agg(
            revenue=pd.NamedAgg("revenue", "sum"),  # выручка (последний месяц)
            avg_price=pd.NamedAgg(
                "final_price", "mean"
            ),  # средняя цена (последний месяц)
            sku_num=pd.NamedAgg(
                "mpstats_id", "nunique"
            ),  # общее число sku (последний месяц)
            brands=pd.NamedAgg(
                "brand", "nunique"
            ),  # количество продавцов (последний месяц)
        )
        .reset_index()
    )

    main_table["sku_rev"] = (
        main_table["revenue"] / main_table["sku_num"]
    )  # выручка на sku (последний месяц)
    main_table = main_table[main_table["revenue"] > 2000000].reset_index(
        drop=True
    )  # страховка

    sku_with_sales = get_sku_with_sales(df, periods_list)
    main_table = main_table.merge(sku_with_sales, how="left", on="seller")
    main_table["sku_with_sales"] = (
        main_table["sku_with_sales_num"] / main_table["sku_num"]
    )  # % sku с продажами

    sku_num_prev = get_sku_num_prev(df, periods_list)
    main_table = main_table.merge(sku_num_prev, how="left", on="seller").fillna(0)
    main_table["sku_new"] = (
        main_table["sku_num"] - main_table["sku_num_prev"]
    )  # число новых sku (последний месяц)
    main_table = main_table.drop(["sku_num_prev"], axis=1)

    products_rating = get_products_rating(df)
    sellers_rating = get_sellers_rating(products_rating, periods_list)
    main_table = main_table.merge(sellers_rating, how="inner", on="seller")

    dynamics = calculate_dynamic_metrics(df, products_rating, periods_list)
    dynamics = add_new_sellers(main_table, dynamics)
    main_table = main_table.merge(dynamics, how="left", on="seller").fillna(0)
    main_table = calculate_growth(main_table)
    main_table = main_table.drop(
        [
            "revenue_period",
            "sku_num_period",
            "sku_sales_period",
            "sku_rev_period",
            "avg_price_period",
            "avg_rating_period",
        ],
        axis=1,
    )
    main_table = main_table.replace([np.inf, -np.inf], np.nan).fillna(0)

    update_database_data(main_table)

    send_email(
        to=["elena.p@carely.group"],
        subject="Competitors monitoring [sellers] has been updated",
        html_content="Competitors monitoring [sellers] has been updated",
        cc=None,
        bcc=None,
    )


main = PythonOperator(task_id="branch_task", python_callable=__main__, dag=dag)

main
