from pathlib import Path
from typing import Dict

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.models.xcom_arg import XComArg
from airflow.operators.python import PythonOperator
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator

from utilities import parameters

_SUPPORTED_BEAM_RUNNERS = ['DirectRunner', 'FlinkRunner', 'SparkRunner']
_HARNESS_ENVIRONMENTS = ['DOCKER', 'PROCESS', 'EXTERNAL', 'LOOPBACK']


def _get_dag_dataset_params() -> Dict[str, Param]:
    return {
        "dataset_names": Param(
            title="Dataset Names",
            description="Names of the datasets to canonicalize.",
            section="Dataset",
            type="array",
            default=['maestro-v3', 'jsb-chorales-v1']
        ),
        "dataset_modes": Param(
            title="Dataset Modes",
            description="Mode of the datasets to canonicalize. Available modes depends on the chosen dataset.",
            section="Dataset",
            type="array",
            default=['midi', 'full']
        ),
        'dataset_file_types': Param(
            title="File Types",
            description="Types of the file to consider from the dataset for the canonicalization.",
            section="Dataset",
            type="array",
            default=['midi', 'mxml']
        ),
        'force_overwrite': Param(
            title="Force Overwrite",
            description="If true, overwrite the dataset if it already exists.",
            section="Dataset",
            type="boolean",
            default=True
        )
    }


def _get_arguments(**context):
    from resolv_data import get_dataset_root_dir_name
    current_dag = context["dag_run"]
    dataset_index_paths = []
    dataset_output_paths = []
    dataset_file_types = current_dag.conf["dataset_file_types"]
    input_path = f's3://{current_dag.conf["s3_bucket_id"]}/raw'
    output_path = f's3://{current_dag.conf["s3_bucket_id"]}/canonical'
    for dataset_name, dataset_mode, dataset_file_type in zip(current_dag.conf["dataset_names"],
                                                             current_dag.conf['dataset_modes'],
                                                             dataset_file_types):
        dataset_root_dir = get_dataset_root_dir_name(dataset_name, dataset_mode)
        dataset_index_path = f'{input_path}/{dataset_root_dir}/index.json'
        dataset_index_paths.append(dataset_index_path)
        dataset_output_path = f'{output_path}/{dataset_root_dir}/{dataset_file_type}'
        dataset_output_paths.append(dataset_output_path)
    context['ti'].xcom_push(key='runner', value=current_dag.conf["runner"])
    context['ti'].xcom_push(key='dataset_output_paths', value=','.join(dataset_output_paths))
    context['ti'].xcom_push(key='dataset_index_file_paths', value=','.join(dataset_index_paths))
    context['ti'].xcom_push(key='dataset_file_types', value=','.join(dataset_file_types))
    context['ti'].xcom_push(key='logging_level', value=current_dag.conf["logging_level"])


def _get_beam_pipeline_options(**context) -> Dict:
    from utilities.minio import MinIOConnectionManager
    current_dag = context["dag_run"]
    minio = MinIOConnectionManager(connection_id=current_dag.conf["s3_connection_id"])
    options = {
        **parameters.get_runner_options_for_beam_pipeline(current_dag),
        **parameters.get_s3_options_for_beam_pipeline(minio)
    }
    if current_dag.conf["force_overwrite"]:
        options['force_overwrite'] = True
    if current_dag.conf["debug"]:
        options['debug'] = True
        options['debug_file_pattern'] = current_dag.conf["debug_file_pattern"]
    return options


dag_parameters = {
    **_get_dag_dataset_params(),
    **parameters.get_dag_s3_params(defaults={"s3_bucket_prefix": "canonical/datasets"}),
    **parameters.get_dag_runner_params(),
    **parameters.get_dag_direct_runner_params(),
    **parameters.get_dag_flink_runner_params(),
    **parameters.get_dag_spark_runner_params(),
    **parameters.get_dag_others_params()
}

with DAG(dag_id='canonicalize_dataset',
         schedule=None,
         description='A DAG with tasks for canonicalize a dataset in MinIO',
         default_args=parameters.get_dag_default_args(),
         catchup=False,
         tags=['ingestion'],
         render_template_as_native_obj=True,
         params=dag_parameters) as dag:
    @task
    def init(**context) -> bool:
        from utilities.minio import MinIOConnectionManager
        current_dag = context["dag_run"]
        minio = MinIOConnectionManager(connection_id=current_dag.conf["s3_connection_id"])
        bucket_name = current_dag.conf["s3_bucket_id"]
        minio.create_bucket(bucket_name)


    get_args = PythonOperator(
        task_id='get_args',
        python_callable=_get_arguments
    )

    get_beam_pipeline_options = PythonOperator(
        task_id='get_beam_pipeline_options',
        python_callable=_get_beam_pipeline_options
    )

    canonicalize_dataset = BeamRunPythonPipelineOperator(
        runner=str(XComArg(get_args, key="runner")),
        task_id='canonicalize_dataset',
        py_file=str(Path('./beam/pipelines/canonicalize_dataset_pipeline.py').resolve()),
        default_pipeline_options=XComArg(get_beam_pipeline_options),
        pipeline_options={
            "dataset_output_paths": XComArg(get_args, key="dataset_output_paths"),
            "dataset_index_file_paths": XComArg(get_args, key="dataset_index_file_paths"),
            "dataset_file_types": XComArg(get_args, key="dataset_file_types"),
            "logging_level": XComArg(get_args, key="logging_level")
        }
    )

    init() >> [get_args, get_beam_pipeline_options] >> canonicalize_dataset
