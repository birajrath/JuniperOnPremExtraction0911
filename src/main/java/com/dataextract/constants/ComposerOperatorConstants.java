package com.dataextract.constants;

public class ComposerOperatorConstants {

	
	
	public static final String IMPORTS="import argparse\n" + 
			"from airflow import DAG\n" + 
			"from airflow.operators.bash_operator import BashOperator\n" + 
			"from datetime import datetime, timedelta\n" + 
			"from airflow.operators.python_operator import PythonOperator\n" + 
			"from airflow.contrib.operators.bigquery_operator import BigQueryOperator";
	
	
	public static final String DEFAULT_ARGS="default_args = {\n" + 
			"    'owner': 'airflow',\n" + 
			"    'depends_on_past': False,\n" + 
			"    'start_date': datetime(${date}),\n" + 
			"    'email': ['abc@hsbc.com'],\n" + 
			"    'email_on_failure': False,\n" + 
			"    'email_on_retry': False,\n" + 
			"    'retries': 1,\n" + 
			"    'retry_delay': timedelta(minutes=5),\n" + 
			"    #'schedule_interval': '${schedule}',\n" +
			"    # 'queue': 'bash_queue',\n" + 
			"    # 'pool': 'backfill',\n" + 
			"    # 'priority_weight': 10,\n" + 
			"    # 'end_date': datetime(2018, 1, 11),\n" +
			"    # 'schedule_interval': '@hourly',\n" +
			"}";
	
	public static String DAG="dag = DAG('${dag_name}', default_args=default_args,schedule_interval='{$cron}')";
	
	public static String BIG_QUERY_OPERATOR="${task_number} = BigQueryOperator(\n" + 
			"        task_id = '${task_name}',\n" + 
			"        bql = '${path}',\n" + 
			"        destination_dataset_table = '${destination}',\n" + 
			"        write_disposition = 'WRITE_TRUNCATE',\n" + 
			"        bigquery_conn_id = 'AIrflow_Infosys_Ingestion',\n" + 
			"        use_legacy_sql = False,\n" + 
			"        dag=dag\n" + 
			"    )";
	
	public static String BASH_OPERATOR="${task_number} = BashOperator(task_id='${task_name}',bash_command='/usr/bin/java -cp ${script_path}', dag=dag)";
	
	/*public static String PYTHON_OPERATOR="${task_number} = PythonOperator(\n" + 
			"        task_id='${task_name}',\n" + 
			"        python_callable=${script_path},\n" + 
			"        op_kwargs={'batch_pipeline': '${batch_pipeline}'},\n" + 
			"        dag=dag)";*/
	
	//public static String NIFI_JAR_LOCATION="/home/birajrath2008/";
	//public static String PROJECT="dazzling-byway-184414";
	
}