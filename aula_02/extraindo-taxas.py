from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime

with DAG(
  'Executando-notebook-etl',
  start_date=datetime(2023, 6, 1), # Lembre de colocar para sua data, para não ultrapassar as 250 requisições
  schedule_interval="0 9 * * *",  # Todos os dias as 9 da manhã
  ) as dag_executando_notebook_extracao:
    
    extraindo_dados = DatabricksRunNowOperator(
    task_id = 'Extraindo-conversoes',
    databricks_conn_id = 'databricks_default',
    job_id = "seu_job_id",
    notebook_params={"data_execucao": '{{data_interval_end.strftime("%Y-%m-%d")}}'}
  )
    extraindo_dados
