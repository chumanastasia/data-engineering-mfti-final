import logging
import os
from datetime import date, timedelta
from functools import reduce
from pathlib import Path
from typing import TypeAlias

import pandas as pd
import pendulum
from airflow.decorators import dag, task

now = pendulum.now()

ProfitTable: TypeAlias = pd.DataFrame


@task(task_id="extract")
def extract_profit_table(csv_source: Path = Path("data/profit_table.csv")) -> ProfitTable:
    """Задача на извлечение данных из CSV-файла."""
    return pd.read_csv(csv_source)


@task(task_id="prepare")
def prepare_profit_table(profit_table: ProfitTable, current_date: date) -> ProfitTable:
    """Задача на сбор таблицы флагов активности по продуктам."""
    current_date = pd.to_datetime(current_date)
    start_date = current_date - pd.DateOffset(months=2)
    end_date = current_date + pd.DateOffset(months=1)
    date_list = pd.date_range(start=start_date, end=end_date, freq="M").strftime("%Y-%m-01")

    return (
        profit_table[profit_table["date"].isin(date_list)]
        .drop("date", axis=1)
        .groupby("id")
        .sum()
    )


@task(task_id="transform", retries=5)
def transform_profit_table(profit_table: ProfitTable, product_name: str) -> ProfitTable:
    """Задача на преобразование таблицы прибыли в таблицу флагов активности."""
    profit_table = profit_table.copy()

    product_filed = f"flag_{product_name}"
    profit_table[product_filed] = profit_table.apply(
        func=lambda row: row[f"sum_{product_name}"] != 0 and row[f"count_{product_name}"] != 0,
        axis=1,
    ).astype(int)

    return profit_table.filter(regex='flag').reset_index()


@task(task_id="load")
def load_activity_table(
        profit_tables: tuple[ProfitTable, ...],
        csv_target: Path = Path("data/activity_table.csv")
) -> None:
    """Задача на сохранение таблицы флагов активности в CSV-файл."""
    df_merged = reduce(
        lambda left, right: pd.merge(left, right, on="id", how="outer"),
        profit_tables
    )

    if os.stat(csv_target).st_size == 0:
        df_merged.to_csv(csv_target, index=False)
    else:
        df_merged.to_csv(csv_target, mode='a', header=False, index=False)


@dag(dag_display_name="additional Чумакова Анастасия", catchup=False, start_date=now, schedule="0 0 5 * *",
     tags=["mfti"])
def etl():
    profit_table = extract_profit_table()
    profit_table_tmp = prepare_profit_table(profit_table, date.today())

    transform_tasks = tuple(
        transform_profit_table.override(task_id=f"transform_product_{product}")(
            profit_table=profit_table_tmp,
            product_name=product,
        )
        for product in ('a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j')
    )

    profit_table >> profit_table_tmp >> transform_tasks >> load_activity_table(profit_tables=transform_tasks)


dag_etl = etl()
