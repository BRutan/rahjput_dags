from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.models.connection import Connection
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import json
import os
import pandas as pd
import re
import sys
import traceback
