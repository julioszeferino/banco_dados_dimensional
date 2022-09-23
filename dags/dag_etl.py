from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount # Mounts the data folder to the container
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.python_operator import PythonOperator
from utils.helpers import _load_s3, _create_tables_redshift, _load_redshift


with DAG(
    dag_id = "pipeline_dados_seguros",
    description = "Pipeline de dados completo com extracao de dados, armazenamento em bucket s3 e input de dados no  Redshift",
    start_date = datetime(2022, 9, 20),
    schedule_interval = None,
    dagrun_timeout=timedelta(minutes=60),
    catchup = False,
    tags=['projeto_seguros', 'xp_educacao'], 
) as dag:

    etl_seguros = DockerOperator(
        api_version = "auto",
        docker_url= "tcp://docker-socket-proxy:2375",
        auto_remove= False, 
        image="julioszeferino/docker-operator-etl:latest",
        task_id= 'docker-operator-etl',
        command= "src/app.py",
        mounts=[Mount(
            "/app/data",
            "C:\\Users\julio\\Desktop\\PROJETO_IGTI_DATA_WAREHOUSE\\data",
            type="bind"
        )]
    )

    envia_arquivos_s3 = PythonOperator(
        task_id='envia-arquivos-s3',
        python_callable=_load_s3,
        op_args=[
            "{{ var.json.aws_default_secret.aws_access_key_id }}",
            "{{ var.json.aws_default_secret.aws_secret_access_key }}"]
    )

    cria_tabelas_redshift = PythonOperator(
        task_id='cria-tabelas-redshift',
        python_callable=_create_tables_redshift,
        op_args=[
            "{{ var.json.aws_redshift_seguros_secret.user }}",
            "{{ var.json.aws_redshift_seguros_secret.password }}"]
    )

    carrega_dados_redshift = PythonOperator(
        task_id='carrega-dados-redshift',
        python_callable=_load_redshift,
        op_args=[
            "{{ var.json.aws_redshift_seguros_secret.user }}",
            "{{ var.json.aws_redshift_seguros_secret.password }}"]
    )

    etl_seguros >> envia_arquivos_s3 >> cria_tabelas_redshift >> carrega_dados_redshift


if __name__ == "__main__":
    dag.cli()
        
