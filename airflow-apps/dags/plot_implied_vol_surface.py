from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.models.variable import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from common import is_datetime, get_dag_name
from datetime import datetime, timedelta
from dateutil.parser import parse as dtparse
from mpl_toolkits import mplot3d
from datetime import datetime
from itertools import chain
import logging
from matplotlib import cm
import matplotlib.pyplot as plt
import numpy as np
import os
import sys

##################
# Operators:
##################
def generate_iv_surface(**context):
    """
    * Generate (average) implied volatility surface for individual day for individual
    ticker, output as png to volatility_surfaces_dir.
    """
    log = context["log"]
    log.info("Starting generate_iv_surface().")
    strike_pm = context["strike_pm"]
    option_type = context["option_type"]
    ticker = context["ticker"]
    vsd = Variable.get("volatility_surfaces_dir")
    vsd = vsd.format_map({"<ticker>" : ticker.lower()})
    if not os.path.exists(vsd):
        log.info(f"Making implied volatility surfaces directory at {vsd}.")
        os.mkdir(vsd)
    ocd = dtparse(context["option_chain_date"])
    log.info(f"Generating surface for ticker {ticker} and option chain valuation date {ocd}.")
    tickers = Variable.get("option_chains_tables", {})
    option_chain_table = tickers[ticker]
    pg_hook = PostgresHook(conn_id = context["conn_id"])
    log.info("Getting data.")
    results = None
    with pg_hook.get_conn() as conn:
        cursor = conn.cursor()
        query = ["SELECT DATE_PART('day', expirationdate - timestamp) AS days_til_expiry, strike, AVG(impliedvolatility) as impliedvolatility"]
        query.append(f"FROM {option_chain_table} WHERE iscall = {True if option_type.lower() == 'call' else False} ")
        query.append(" AND percentatm BETWEEN -{strike_pm} AND {strike_pm}")
        query.append(f" AND MONTH(upload_timestamp) = {ocd.month} AND DAY(upload_timestamp) = {ocd.day} AND YEAR(upload_timestamp) = {ocd.year}")
        query.append(" GROUP BY MONTH(upload_timestamp), DAY(upload_timestamp), YEAR(upload_timestamp)")
        cursor.execute(" ".join(query))
        results = cursor.fetchall()
    if not results:
        log.info("No data for {ocd}. Skipping surface generation.")
        return
    days_til_expiry = []
    days_til_expiry_extended = []
    strikes = []
    implied_vols = []
    for result in results:
        days_til_expiry.append(result[0])
        strikes.append(result[1])
        implied_vols.append(result[2])
    # Repeat missing data:
    for idx in range(len(strikes)):
        # repeat DTE so the list has same length as the other lists
        days_til_expiry_extended.append(np.repeat(days_til_expiry[idx], len(days_til_expiry[idx])))
        # append implied volatilities to list
        implied_vols.append(implied_vols[idx])
    strikes = list(chain(*strikes))
    days_til_expiry_extended = list(chain(*days_til_expiry_extended))
    implied_vols = list(chain(*implied_vols))
    # Generate surface:
    fig = plt.figure(figsize=(7,7))
    axs = plt.axes(projection="3d")
    # use plot_trisurf from mplot3d to plot surface and cm for color scheme
    axs.plot_trisurf(strikes, days_til_expiry, implied_vols, cmap=cm.jet)
    # change angle
    axs.view_init(30, 65)
    # add labels
    plt.xlabel("Strike")
    plt.ylabel("Days Til Expiration")
    plt.title(f"{ticker.lower()} IV Surface on {ocd.strftime('%m/%d/%y')}")
    outpath = os.path.join(vsd, f"{ticker.lower()}_{ocd.strftime('%m_%d_%y')}.png")
    log.info(f"Output figure to {outpath}.")
    plt.savefig(outpath)
    log.info("Ending generate_iv_surface().")

with DAG(
    dag_id=get_dag_name(__file__),
    catchup=False,
    start_date=days_ago(1),
    schedule="@once",
    max_active_tasks=30,
    params = {"ticker" : Param("ticker", type="string", description="Ticker to pull implied volatility for."),
              "strike_pm" : Param("strike_param", type="float", default = "", description="Strike % to generate surface. If skipped then does for all strikes."),
              "option_chain_date" : Param("option_chain_date", type="string", description="Date string from which to generate surface.")}
) as dag:
    
    log = logging.getLogger()
    log.setLevel(logging.INFO)
    
    start = EmptyOperator(task_id="start")
    
    generate_iv_surface_task = PythonOperator(task_id="generate_iv_surface",
                                              python_callable=generate_iv_surface,
                                              op_kwargs={"log" : log, 
                                                         "ticker" : dag.params["ticker"],
                                                         "option_chain_date" : dag.params["option_chain_date"],
                                                         "strike_pm" : dag.params["strike_pm"],
                                                         "conn_id" : "postgres_default"})   

    start >> generate_iv_surface_task
    