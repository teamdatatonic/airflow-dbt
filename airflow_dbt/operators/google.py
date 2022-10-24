from airflow.models import BaseOperatorLink, XCom
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults

from airflow_dbt.hooks.google import DbtCloudBuildHook
from airflow_dbt.operators.dbt_operator import DbtBaseOperator

XCOM_KEY_CLOUD_BUILD_LINK = 'cloud_build_link'
XCOM_KEY_DBT_ARTIFACT_LINK = 'dbt_artifact_link'
XCOM_KEY_BUILD_ID = 'build_id'


class CloudBuildConsoleLink(BaseOperatorLink):
    """Helper class for constructing BigQuery link."""

    name = 'CloudBuild'

    def get_link(self, operator, dttm):
        cloud_build_link = XCom.get_one(
            dag_id=operator.dag.dag_id,
            task_id=operator.task_id,
            execution_date=dttm,
            key=XCOM_KEY_CLOUD_BUILD_LINK,
        )
        return cloud_build_link

class DbtArtifactsConsoleLink(BaseOperatorLink):
    """Helper class for constructing BigQuery link."""

    name = 'DBT Artifacts'

    def get_link(self, operator, dttm):
        cloud_build_link = XCom.get_one(
            dag_id=operator.dag.dag_id,
            task_id=operator.task_id,
            execution_date=dttm,
            key=XCOM_KEY_DBT_ARTIFACT_LINK,
        )
        return cloud_build_link


class AirflowDbtCloudBuildLinksPlugin(AirflowPlugin):
    """Airflow plugin for CloudBuild links"""
    name = "cloud_build_link_plugin"
    operator_extra_links = [
        CloudBuildConsoleLink(),
        DbtArtifactsConsoleLink(),
    ]


class DbtCloudBuildOperator(DbtBaseOperator):
    """Uses the CloudBuild Hook to run the provided dbt config"""

    operator_extra_links = (CloudBuildConsoleLink(), DbtArtifactsConsoleLink())

    template_fields = DbtBaseOperator.template_fields + [
        'gcs_staging_location', 'project_id', 'dbt_version',
        'service_account', 'dbt_artifacts_dest'
    ]

    # noinspection PyDeprecation
    @apply_defaults
    def __init__(
        self,
        gcs_staging_location: str,
        project_id: str = None,
        gcp_conn_id: str = "google_cloud_default",
        dbt_version: str = '1.3.latest',
        dbt_image: str = 'ghcr.io/dbt-labs/dbt-bigquery',
        dbt_artifacts_dest: str = None,
        service_account: str = None,
        *args,
        **kwargs
    ):
        self.dbt_artifacts_dest = dbt_artifacts_dest
        self.gcs_staging_location = gcs_staging_location
        self.gcp_conn_id = gcp_conn_id
        self.project_id = project_id
        self.dbt_version = dbt_version
        self.dbt_image = dbt_image
        self.service_account = service_account

        super(DbtCloudBuildOperator, self).__init__(
            *args,
            **kwargs
        )

    def execute(self, context):
        # execute the super function normally
        build_id = super(DbtCloudBuildOperator, self).execute(context)
        # use the returned value to create several XCom values
        project_id = self.dbt_hook.cloud_build_hook.project_id
        artifacts_path = self.dbt_artifacts_dest.lstrip('gs://')
        artifacts_url = f'https://console.cloud.google.com/storage/browser/{artifacts_path}?project={project_id}'
        build_url = f'https://console.cloud.google.com/cloud-build/builds/{build_id}?project={project_id}'
        # push the values to the class context
        self.artifacts_url = artifacts_url
        self.build_url = build_url
        # push the xcoms
        self.xcom_push(context, key=XCOM_KEY_DBT_ARTIFACT_LINK, value=artifacts_url)
        self.xcom_push(context, key=XCOM_KEY_CLOUD_BUILD_LINK, value=build_url)
        self.xcom_push(context, key=XCOM_KEY_BUILD_ID, value=build_id)
        return build_id

    def instantiate_hook(self):
        """
        Instantiates a Cloud build dbt hook. This has to be done out of the
        constructor because by the time the constructor runs the params have
        not been yet interpolated.
        """
        self.dbt_hook = DbtCloudBuildHook(
            env=self.dbt_env,
            gcs_staging_location=self.gcs_staging_location,
            gcp_conn_id=self.gcp_conn_id,
            dbt_version=self.dbt_version,
            dbt_image=self.dbt_image,
            service_account=self.service_account,
            project_id=self.project_id,
            dbt_project_dir=self.dbt_config.get('project_dir'),
            dbt_artifacts_dest=self.dbt_artifacts_dest,
        )
