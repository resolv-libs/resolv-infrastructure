import json
from pathlib import Path
from typing import Dict, Any

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.models.xcom_arg import XComArg
from airflow.operators.python import PythonOperator
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator

from beam.dofn.base import DebugOutputTypeEnum
from beam.dofn.note_sequence import NS_DO_FN_MAP
from utilities import parameters


def _get_dag_processors_params() -> Dict[str, Any]:
    return {
        do_fn_id.replace("_ps_do_fn", ""): Param(
            title=do_fn.name(),
            description=f"Configuration for the {do_fn.name()} processor.",
            section="Processors",
            default={
                'enabled': False,
                'order': 1,
                **do_fn.default_config()
            },
            type=["object", "null"]
        ) for do_fn_id, do_fn in NS_DO_FN_MAP.items()
    }


def _get_dag_dataset_params() -> Dict[str, Param]:
    return {
        "output_dataset_name": Param(
            title="Dataset Name",
            description="The name of the dataset to generate.",
            section="Dataset",
            type="string"
        ),
        "source_datasets_names": Param(
            title="Source Datasets",
            description="Name of the datasets to use a sources for the dataset to generate.",
            section="Dataset",
            type="array",
            default=['maestro-v3', 'jsb-chorales-v1']
        ),
        "source_datasets_modes": Param(
            title="Source Datasets Modes",
            description="The modes of the source datasets. i-th element of this array will be the mode of the i-th "
                        "source dataset.",
            section="Dataset",
            type="array",
            default=['midi', 'full']
        ),
        'source_datasets_file_types': Param(
            title="Source Datasets File Types",
            description="The type of file to consider in the source datasets. i-th element of this array will be the "
                        "mode of the i-th source dataset.",
            type="array",
            section="Dataset",
            default=['midi', 'mxml']
        ),
        'distinct': Param(
            title="Distinct",
            description="If true, output dataset will contain only distinct sequences.",
            section="Dataset",
            type="boolean",
            default=False
        ),
        'force_overwrite': Param(
            title="Force Overwrite",
            description="If true, overwrite the dataset if it already exists.",
            section="Dataset",
            type="boolean",
            default=True
        )
    }


def _get_dag_others_params() -> Dict[str, Param]:
    default_params = parameters.get_dag_others_params()
    default_params['debug_output_type'] = Param(
        title="Debug Output Type",
        description="The output type for the debug artifacts.",
        section="Others",
        enum=[e.value for e in DebugOutputTypeEnum],
        default=DebugOutputTypeEnum.SOURCE.value
    )
    return default_params


def _get_arguments(**context):
    from resolv_data import get_dataset_root_dir_name
    current_dag = context["dag_run"]
    bucket_name = current_dag.conf["s3_bucket_id"]
    source_datasets_dir_names = [
        f'{get_dataset_root_dir_name(dataset_name, dataset_mode)}/{dataset_file_type}'
        for (dataset_name, dataset_mode, dataset_file_type) in zip(current_dag.conf['source_datasets_names'],
                                                                   current_dag.conf['source_datasets_modes'],
                                                                   current_dag.conf['source_datasets_file_types'])
    ]
    input_path = f's3://{bucket_name}/canonical'
    source_dataset_paths = [f'{input_path}/{source_data_dir}/*.tfrecord' for source_data_dir in
                            source_datasets_dir_names]
    output_path = f's3://{bucket_name}/{current_dag.conf["s3_bucket_prefix"]}'
    output_dataset_paths = [f'{output_path}/{current_dag.conf["output_dataset_name"]}/{source_data_dir}'
                            for source_data_dir in source_datasets_dir_names]
    context['ti'].xcom_push(key='source_dataset_paths', value=','.join(source_dataset_paths))
    context['ti'].xcom_push(key='output_dataset_paths', value=','.join(output_dataset_paths))
    context['ti'].xcom_push(key='logging_level', value=current_dag.conf["logging_level"])
    for processors_id in [do_fn_id.replace("_ps_do_fn", "") for do_fn_id in NS_DO_FN_MAP.keys()]:
        context['ti'].xcom_push(key=processors_id, value=json.dumps(current_dag.conf[processors_id]))


def _get_beam_pipeline_options(**context) -> Dict:
    from utilities.minio import MinIOConnectionManager
    current_dag = context["dag_run"]
    minio = MinIOConnectionManager(connection_id=current_dag.conf["s3_connection_id"])
    options = {
        **parameters.get_runner_options_for_beam_pipeline(current_dag),
        **parameters.get_s3_options_for_beam_pipeline(minio)
    }
    if current_dag.conf["distinct"]:
        options['distinct'] = True
    if current_dag.conf["force_overwrite"]:
        options['force_overwrite'] = True
    if current_dag.conf["debug"]:
        options['debug'] = True
        options['debug_file_pattern'] = current_dag.conf["debug_file_pattern"]
        options['debug_output_type'] = current_dag.conf["debug_output_type"]
    return options


dag_parameters = {
    **_get_dag_dataset_params(),
    **_get_dag_processors_params(),
    **parameters.get_dag_s3_params(defaults={"s3_bucket_prefix": "generated"}),
    **parameters.get_dag_runner_params(),
    **parameters.get_dag_direct_runner_params(),
    **parameters.get_dag_flink_runner_params(),
    **parameters.get_dag_spark_runner_params(),
    **_get_dag_others_params()
}


with DAG(dag_id='generate_dataset',
         schedule=None,
         description='A DAG with tasks for generating a dataset.',
         default_args=parameters.get_dag_default_args(),
         catchup=False,
         tags=['transform'],
         render_template_as_native_obj=True,
         params=dag_parameters) as dag:
    @task
    def init(**context) -> bool:
        from resolv_data import DATASET_TYPE_MAP
        from utilities.minio import MinIOConnectionManager
        current_dag = context["dag_run"]
        not_supported_datasets = [element for element in current_dag.conf['source_datasets_names'] if
                                  element not in list(DATASET_TYPE_MAP.keys())]
        if not_supported_datasets:
            raise ValueError(f'Datasets {not_supported_datasets} are not supported.')
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

    generate_melodies_dataset = BeamRunPythonPipelineOperator(
        task_id='generate_melodies_dataset',
        py_file=str(Path('./beam/pipelines/generate_dataset_pipeline.py').resolve()),
        default_pipeline_options=XComArg(get_beam_pipeline_options),
        pipeline_options={
            **{
                "source_dataset_paths": XComArg(get_args, key="source_dataset_paths"),
                "output_dataset_paths": XComArg(get_args, key="output_dataset_paths"),
                "logging_level": XComArg(get_args, key="logging_level")
            },
            **{
                processor_id: XComArg(get_args, key=processor_id)
                for processor_id in [do_fn_id.replace("_ps_do_fn", "") for do_fn_id in NS_DO_FN_MAP.keys()]
            }
        }
    )

    init() >> [get_args, get_beam_pipeline_options] >> generate_melodies_dataset
