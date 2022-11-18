from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.models.variable import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.db import create_session
from common import check_params, cleanup_files, get_dag_name, is_regex
from datetime import datetime
import json
import logging
import os
import pandas as pd

##################
# Operators:
##################
def get_all_variable_filepaths(**context):
    """
    * Get target variables.
    """
    log = context["log"]
    log.info("Starting get_all_variable_filepaths().")
    # By calling .query() with Variable, we are asking the airflow db 
    # session to return all variables (select * from variables).
    # The result of this is an iterable item similar to a dict but with a 
    # slightly different signature (object.key, object.val).
    filepaths = []
    with create_session() as session:
        for var in session.query(Variable):
            if var.key.lower().endswith('_dir'):
                value = Variable.get(var.key)
                if os.path.isdir(value):    
                    entries = os.listdir(value)
                    for entry in entries:
                        path = os.path.join(value, entry)
                        if os.path.isfile(path):
                            filepaths.append(path)
    log.info("Ending get_all_variable_filepaths().")
    log.info(f"Got {len(filepaths)} filepaths.")
    context["ti"].xcom_push(key="filepaths", value=filepaths)
    
###################
# Dag :
###################
with DAG(
    dag_id=get_dag_name(__file__),
    start_date=datetime.today(),
    catchup=False,
    schedule_interval=None,
    render_template_as_native_obj=True,
    params = {"fileregex" : Param(type="string", default="")}
    ) as dag:
    
    #dag.trigger_arguments = dag.params
    log = logging.getLogger()
    log.setLevel(logging.INFO)
    
    op_kwargs = {}
    op_kwargs["log"] = log
    op_kwargs["fileregex"] = "{{ params.fileregex }}"
    op_kwargs["optional"] = {"fileregex" : (str, is_regex) }
    
    start = EmptyOperator(task_id="start")
    
    check_params_task = PythonOperator(task_id="check_params",
                                       op_kwargs=op_kwargs,
                                       python_callable=check_params)
    
    get_all_variable_filepaths_task = PythonOperator(task_id="get_all_variable_filepaths",
                                                      op_kwargs=op_kwargs,
                                                      python_callable=get_all_variable_filepaths)
    try:
        filepaths = json.loads("{{ ti.xcom_pull(task_ids='get_all_variable_filepaths', key='filepaths') }}")
    except:
        filepaths = ""
        
    cleanup_files_task = PythonOperator(task_id="cleanup_files",
                                        op_kwargs={"log" : log,
                                                   "filepaths" : filepaths},
                                        python_callable=cleanup_files)
        
    start >> get_all_variable_filepaths_task >> cleanup_files_task