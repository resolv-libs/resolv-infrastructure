from typing import Dict

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param

from resolv_data import DATASET_TYPE_MAP
from utilities import parameters


def _get_dag_dataset_params() -> Dict[str, Param]:
    return {
        "dataset_name": Param(
            title="Dataset Name",
            description="Name of the dataset to import.",
            section="Dataset",
            type="string",
            enum=list(DATASET_TYPE_MAP.keys()),
            default="maestro_v3"
        ),
        "dataset_mode": Param(
            title="Dataset Mode",
            description="Mode of the dataset to import. Available modes depends on the chosen dataset.",
            section="Dataset",
            type="string",
            default="midi"
        ),
        'force_overwrite': Param(
            title="Force Overwrite",
            description="If true, overwrite the dataset if it already exists.",
            section="Dataset",
            type="boolean",
            default=True
        ),
        'allow_invalid_checksum': Param(
            title="Allow Invalid Checksum",
            description="Allow dataset import even if its checksum it's invalid.",
            section="Dataset",
            type="boolean",
            default=False
        ),
        'cleanup': Param(
            title="Cleanup",
            description="Whether to cleanup the local downloaded directory after the import.",
            section="Dataset",
            type="boolean",
            default=True
        )
    }


dag_parameters = {
    **_get_dag_dataset_params(),
    **parameters.get_dag_s3_params(defaults={"s3_bucket_prefix": "raw"}),
}

with DAG(dag_id='import_dataset',
         schedule=None,
         description='A DAG with tasks for downloading, indexing and uploading datasets to S3',
         default_args=parameters.get_dag_default_args(),
         catchup=False,
         tags=['ingestion'],
         render_template_as_native_obj=True,
         params=dag_parameters) as dag:
    @task
    def init(**context) -> bool:
        import logging
        from resolv_data import get_dataset_root_dir_name
        from utilities.minio import MinIOConnectionManager
        current_dag = context["dag_run"]
        minio = MinIOConnectionManager(connection_id=current_dag.conf["s3_connection_id"])
        bucket_name = current_dag.conf["s3_bucket_id"]
        minio.create_bucket(bucket_name)
        if not current_dag.conf['force_overwrite']:
            dataset_root = get_dataset_root_dir_name(current_dag.conf["dataset_name"],
                                                     current_dag.conf['dataset_mode'])
            dataset_prefix = f'{current_dag.conf["s3_bucket_prefix"]}/{dataset_root}'
            if minio.check_for_object(bucket_name, dataset_prefix):
                logging.info(f"Dataset {dataset_prefix} already exists.")
                return False
        return True


    @task.short_circuit
    def dataset_exists(is_init) -> bool:
        return is_init


    @task
    def download_and_index(**context):
        current_dag = context["dag_run"]
        dataset_name = current_dag.conf['dataset_name']
        dataset = DATASET_TYPE_MAP[dataset_name](
            mode=current_dag.conf['dataset_mode'],
            overwrite=current_dag.conf['force_overwrite'],
            cleanup=current_dag.conf['cleanup'],
            allow_invalid_checksum=current_dag.conf['allow_invalid_checksum']
        )
        dataset_path = dataset.download(temp=True)
        index_prefix = 's3://{}/{}/{}'.format(current_dag.conf["s3_bucket_id"],
                                              current_dag.conf["s3_bucket_prefix"],
                                              dataset.root_dir_name)
        dataset.compute_index(path_prefix=index_prefix)
        context['ti'].xcom_push(key='dataset_path', value=str(dataset_path))
        context['ti'].xcom_push(key='dataset_root_dir_name', value=dataset.root_dir_name)


    @task
    def upload_dataset_to_minio(**context):
        from utilities.minio import MinIOConnectionManager
        current_dag = context["dag_run"]
        minio = MinIOConnectionManager(connection_id=current_dag.conf["s3_connection_id"])
        dir_to_upload = context['ti'].xcom_pull(task_ids='download_and_index', key='dataset_path')
        dataset_root_dir_name = context['ti'].xcom_pull(task_ids='download_and_index', key='dataset_root_dir_name')
        key_prefix = f"{current_dag.conf['s3_bucket_prefix']}/{dataset_root_dir_name}"
        minio.upload_directory(dir_to_upload, current_dag.conf["s3_bucket_id"], key_prefix,
                               replace=current_dag.conf['force_overwrite'], cleanup=current_dag.conf['cleanup'])


    dataset_exists(init()) >> download_and_index() >> upload_dataset_to_minio()
