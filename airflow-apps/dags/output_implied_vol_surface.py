from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.models.variable import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from common import is_datetime, is_float, get_dag_name
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
import pandas as pd
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
    strike_pm = float(context["strike_pm"]) if context["strike_pm"] else ""
    option_type = context["option_type"]
    ticker = context["ticker"].upper()
    tickers = Variable.get("option_chains_tables", {})
    ocd = dtparse(context["option_chain_date"])
    vsd = Variable.get("volatility_surfaces_dir")
    if not os.path.exists(vsd):
        log.info(f"Making implied volatility surfaces directory at {vsd}.")
        os.mkdir(vsd)
    vsd = os.path.join(vsd, ticker)
    if not os.path.exists(vsd):
        log.info(f"Making implied volatility surfaces directory for ticker at {vsd}.")
        os.mkdir(vsd)
    
    log.info(f"Generating surface for ticker {ticker} and option chain valuation date {ocd}.")
    option_chain_table = tickers[ticker]
    pg_hook = PostgresHook(conn_id = context["conn_id"])
    log.info("Getting data.")
    results = pd.DataFrame()
    with pg_hook.get_conn() as conn:
        query = ["SELECT DATE_PART('day', expirationdate - timestamp) AS days_til_expiry, strike, AVG(impliedvolatility) as impliedvolatility"]
        query.append(f"FROM {option_chain_table} WHERE iscall = {True if option_type.lower() == 'calls' else False} ")
        if strike_pm:
            query.append("AND moneyness BETWEEN -{strike_pm} AND {strike_pm}")
        query.append(f"AND MONTH(upload_timestamp) = {ocd.month} AND DAY(upload_timestamp) = {ocd.day} AND YEAR(upload_timestamp) = {ocd.year}")
        query.append("GROUP BY MONTH(upload_timestamp), DAY(upload_timestamp), YEAR(upload_timestamp)")
        query = "\n".join(query)
        log.info("Full query: ")
        log.info(query)
        results = pd.read_sql(sql=query, con=conn)
    if len(results) == 0:
        log.info(f"No data for {ocd}. Skipping surface generation.")
        return
    days_til_expiry = []
    days_til_expiry_extended = []
    strikes = []
    implied_vols = []
    log.info("Skipping interpolation, making all unknowns constant.")
    days_til_expiry = list(results["days_til_expiry"])
    # Repeat missing data:
    for row in range(len(results)):
        # repeat DTE so the list has same length as the other lists
        days_til_expiry_extended.append(np.repeat(days_til_expiry[row], len(results)))
        strikes.append(results.iloc[row, 1])
        implied_vols.append(results.iloc[row, 2])
    # Unlist list of lists:
    strikes = list(chain(*strikes))
    days_til_expiry_extended = list(chain(*days_til_expiry_extended))
    implied_vols = list(chain(*implied_vols))
    # Generate surface:
    plt.clf()
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
    outpath = os.path.join(vsd, f"{ticker.lower()}_iv_surface_{ocd.strftime('%m_%d_%y')}.png")
    log.info(f"Output figure to {outpath}.")
    plt.savefig(outpath)
    log.info("Ending generate_iv_surface().")

with DAG(
    dag_id=get_dag_name(__file__),
    catchup=False,
    start_date=datetime.now(),
    schedule=None,
    max_active_tasks=30,
    render_template_as_native_obj=True,
    params = {"ticker" : Param("", type="string", description="Single ticker to pull implied volatility for."),
              "option_chains_date" : Param("", type="string", description="Market price date from which to generate surface."),
              "option_type" : Param("calls", type="string", description="Put calls if want calls, puts if puts."),
              # Optional
              "strike_pm" : Param("", type="string", description="Strike % +/- from ATM to generate surface. If skipped then does for all strikes. Must be positive floating point.")}
    ) as dag:
    
    log = logging.getLogger()
    log.setLevel(logging.INFO)
    op_kwargs = {"log" : log}
    op_kwargs["ticker"] = "{{ params.ticker }}"
    op_kwargs["strike_pm"] = "{{ params.strike_pm }}"
    op_kwargs["option_chains_date"] = "{{ params.option_chains_date }}"
    op_kwargs["conn_id"] ="postgres_default"
    op_kwargs["log"] = log
    op_kwargs["required"] = {"ticker" : (str, lambda ticker : ticker.upper() in Variable.get("option_chains_tables")), "option_chains_date" : (str, is_datetime), "option_type" : (str, lambda x : x in ["calls", "puts"])}
    op_kwargs["optional"] = {"strike_pm" : (str, lambda x : is_float(x) and x > 0)}
    start = EmptyOperator(task_id="start")
    
    generate_iv_surface_task = PythonOperator(task_id="generate_iv_surface",
                                              python_callable=generate_iv_surface,
                                              op_kwargs=op_kwargs)   

    start >> generate_iv_surface_task
    