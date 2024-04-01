import datetime
from typing import Dict, Any

from airflow.models.param import Param

from minio import MinIOConnectionManager

_SUPPORTED_BEAM_RUNNERS = ['DirectRunner', 'FlinkRunner', 'SparkRunner']
_HARNESS_ENVIRONMENTS = ['DOCKER', 'PROCESS', 'EXTERNAL', 'LOOPBACK']


# --------------------------------- DAG ARGUMENTS ---------------------------------

def get_dag_default_args(defaults: Dict[str, Any] = None) -> Dict[str, Any]:
    default_args = {
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,
        # 'retry_delay': datetime.timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        'execution_timeout': datetime.timedelta(hours=3),
        # 'on_failure_callback': some_function, # or list of functions
        # 'on_success_callback': some_other_function, # or list of functions
        # 'on_retry_callback': another_function, # or list of functions
        # 'sla_miss_callback': yet_another_function, # or list of functions
        # 'trigger_rule': 'all_success'
    }
    if defaults:
        default_args.update(defaults)
    return default_args


# --------------------------------- DAG PARAMETERS ---------------------------------


def _update_default_params(params: Dict[str, Param], defaults: Dict[str, Any]) -> Dict[str, Param]:
    if defaults:
        for param_id, param in params.items():
            param.value = defaults.get(param_id, param.value)
    return params


def get_dag_runner_params(defaults: Dict[str, Any] = None) -> Dict[str, Param]:
    params = {
        "runner": Param(
            title="Runner",
            description="Apache Beam runner for the pipeline. Set its options in the corresponding section.",
            section="Runner",
            type="string",
            enum=_SUPPORTED_BEAM_RUNNERS,
            default="DirectRunner"
        ),
        "environment_type": Param(
            title="SDK Harness Environment",
            description="Determines where user code will be executed for portable runners (eg. Flink or Spark).",
            section="Runner",
            type="string",
            enum=_HARNESS_ENVIRONMENTS,
            default="EXTERNAL"
        ),
        "environment_config": Param(
            title="SDK Harness Environment Config",
            description="Configures the environment depending on the value of environment_type. See the SDK Harness in "
                        "Apache Beam guide.",
            section="Runner",
            type="string",
            default="python-worker-harness:50000"
        ),
        'sdk_worker_parallelism': Param(
            title="SDK Workers",
            description="Sets the number of SDK workers that run on each worker node. The default is 1. If 0, the "
                        "value is automatically set by the runner by looking at different parameters, such as the "
                        "number of CPU cores on the worker machine",
            section="Runner",
            type="integer",
            default=1
        )
    }
    return _update_default_params(params, defaults)


def get_dag_s3_params(defaults: Dict[str, Any] = None) -> Dict[str, Param]:
    params = {
        "s3_connection_id": Param(
            title="S3 Connection ID",
            description="The identifier of the connection for the S3 storage.",
            section="S3",
            type="string",
            default="minio"
        ),
        "s3_bucket_id": Param(
            title="S3 Bucket",
            description="The identifier of the bucket where to import the dataset.",
            section="S3",
            type="string",
            default="datalake"
        ),
        "s3_bucket_prefix": Param(
            title="S3 Bucket Prefix",
            description="The path inside the bucket where the dataset will be saved.",
            section="S3",
            type="string",
            default=""
        )
    }
    return _update_default_params(params, defaults)


def get_dag_direct_runner_params(defaults: Dict[str, Any] = None) -> Dict[str, Param]:
    params = {
        'direct_running_mode': Param(
            title="Running mode",
            description="The Apache Beam job running mode.",
            section="Direct Runner",
            type="string",
            enum=["in_memory", "multi_processing", "multi_threading"],
            default="multi_processing"
        ),
        'direct_num_workers': Param(
            title="Workers",
            description="The number of workers that the Apache Beam job will use. Valid only if running in "
                        "multi_processing or multi_threading mode.",
            section="Direct Runner",
            type="integer",
            default=10
        ),
        'no_direct_runner_use_stacked_bundle': Param(
            title="Use stacked bundle",
            description="DirectRunner uses stacked WindowedValues within a Bundle for memory optimization. "
                        "Set --no_direct_runner_use_stacked_bundle to avoid it.",
            section="Direct Runner",
            type="boolean",
            default=False
        ),
        'direct_runner_bundle_repeat': Param(
            title="Bundle repeat",
            description="Replay every bundle this many extra times, for profiling and debugging.",
            section="Direct Runner",
            type="integer",
            default=0
        ),
        'direct_embed_docker_python': Param(
            title="Embed Docker Python",
            description="DirectRunner uses the embedded Python environment when the default Python docker environment "
                        "is specified.",
            section="Direct Runner",
            type="boolean",
            default=False
        )
    }
    return _update_default_params(params, defaults)


def get_dag_flink_runner_params(defaults: Dict[str, Any] = None) -> Dict[str, Param]:
    params = {
        'flink_master': Param(
            title="Master address",
            description="Flink master address (http://host:port). Use [local] to start a local cluster for the "
                        "execution. Use [auto] if you plan to either execute locally or let the Flink job server "
                        "infer the cluster address.",
            section="Flink Runner",
            type="string",
            default="http://host.docker.internal:8081"
        ),
        'flink_version': Param(
            title="Version",
            description="Flink master address (http://host:port). Use [local] to start a local cluster for the "
                        "execution. Use [auto] if you plan to either execute locally or let the Flink job server "
                        "infer the cluster address.",
            section="Flink Runner",
            type="string",
            enum=['1.12', '1.13', '1.14', '1.15', '1.16'],
            default='1.16'
        ),
        # 'flink_job_server_jar': '', TODO - Flink external job server support (only Uber mode at the moment)
        # 'flink_submit_uber_jar': True,
        'flink_parallelism': Param(
            title="Parallelism",
            description="The degree of parallelism to be used when distributing operations onto workers. If the "
                        "parallelism is not set, the configured Flink default is used, or 1 if none can be found.",
            section="Flink Runner",
            type="integer",
            default=-1
        ),
        'flink_max_parallelism': Param(
            title="Max parallelism",
            description="The pipeline wide maximum degree of parallelism to be used. The maximum parallelism specifies "
                        "the upper limit for dynamic scaling and the number of key groups used for partitioned state.",
            section="Flink Runner",
            type="integer",
            default=-1
        )
    }
    return _update_default_params(params, defaults)


def get_dag_spark_runner_params(defaults: Dict[str, Any] = None) -> Dict[str, Param]:
    params = {
        'spark_master_url': Param(
            title="Master address",
            description="Spark master URL (spark://HOST:PORT). Use local (single-threaded) or local[*] "
                        "(multi-threaded) to start a local cluster for the execution.",
            section="Spark Runner",
            type="string",
            default="local[4]"
        ),
        'spark_version': Param(
            title="Version",
            description="Spark major version to use.",
            section="Spark Runner",
            type="string",
            enum=['3'],
            default='3'
        ),
        # 'spark_job_server_jar': '', TODO - Spark external job server support (only Uber mode at the moment)
        # 'spark_submit_uber_jar': True,
        'spark_rest_url': Param(
            title="REST Url",
            description="URL for the Spark REST endpoint. Only required when using spark_submit_uber_jar. "
                        "For example, http://hostname:6066",
            section="Spark Runner",
            type="string",
            default='http://localhost:6066'
        )
    }
    return _update_default_params(params, defaults)


def get_dag_others_params(defaults: Dict[str, Any] = None) -> Dict[str, Param]:
    params = {
        'logging_level': Param(
            title="Logging Level",
            description="Set the logging level for the pipeline.",
            section="Others",
            type="string",
            enum=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
            default="INFO"
        ),
        'debug': Param(
            title="Debug",
            description="Run the pipeline in debug mode.",
            section="Others",
            type="boolean",
            default=False
        ),
        'debug_file_pattern': Param(
            title="Debug File Pattern",
            description="RegEx file pattern. Debug will be written only if the file id matches this pattern.",
            section="Others",
            type="string",
            default='.*'
        )
    }
    return _update_default_params(params, defaults)


# --------------------------------- BEAM OPERATOR OPTIONS PARAMETERS ---------------------------------

def get_runner_options_for_beam_pipeline(current_dag):
    dag_runner = current_dag.conf['runner']
    if dag_runner == 'DirectRunner':
        direct_runner_options = {
            'direct_running_mode': current_dag.conf["direct_running_mode"],
            'direct_num_workers': current_dag.conf["direct_num_workers"],
            'direct_runner_bundle_repeat': current_dag.conf['direct_runner_bundle_repeat']
        }
        if current_dag.conf["no_direct_runner_use_stacked_bundle"]:
            direct_runner_options['no_direct_runner_use_stacked_bundle'] = True
        if current_dag.conf["direct_embed_docker_python"]:
            direct_runner_options['direct_embed_docker_python'] = True
        return direct_runner_options
    elif dag_runner == 'FlinkRunner':
        return {
            'flink_master': current_dag.conf["flink_master"],
            'flink_version': current_dag.conf["flink_version"],
            'flink_submit_uber_jar': True,
            'parallelism': current_dag.conf["flink_parallelism"],
            'max_parallelism': current_dag.conf["flink_max_parallelism"],
            'environment_type': current_dag.conf["environment_type"],
            'environment_config': current_dag.conf["environment_config"],
            'sdk_worker_parallelism': current_dag.conf["sdk_worker_parallelism"],
        }
    elif dag_runner == 'SparkRunner':
        return {
            'spark_master_url': current_dag.conf["spark_master_url"],
            'spark_version': current_dag.conf["spark_version"],
            'spark_submit_uber_jar': True,
            'spark_rest_url': current_dag.conf["spark_rest_url"],
            'environment_type': current_dag.conf["environment_type"],
            'environment_config': current_dag.conf["environment_config"],
            'sdk_worker_parallelism': current_dag.conf["sdk_worker_parallelism"],
        }


def get_s3_options_for_beam_pipeline(minio: MinIOConnectionManager) -> Dict[str, Any]:
    aws_connection = minio.client.conn_config
    return {
        "s3_access_key_id": aws_connection.aws_access_key_id,
        "s3_secret_access_key": aws_connection.aws_secret_access_key,
        "s3_endpoint_url": aws_connection.endpoint_url,
        "s3_verify": False,
        "s3_disable_ssl": True
    }
