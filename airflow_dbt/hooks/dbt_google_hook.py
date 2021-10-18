import logging
import os
import tarfile
from tempfile import NamedTemporaryFile
from typing import Any, Dict, List
from uuid import uuid4

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.cloud_build import CloudBuildHook
from airflow.providers.google.cloud.hooks.gcs import (
    GCSHook, _parse_gcs_url, gcs_object_is_directory,
)

from hooks.dbt_hook import DbtBaseHook


class DbtCloudBuildHook(DbtBaseHook):
    """
    Runs the dbt command in a Cloud Build job in GCP

    :type dir: str
    :param dir: The directory containing the DBT files. The logic is, if this
        is a GCS blob compressed in tar.gz then it will be used in the build
        process without re-uploading. Otherwise we expect "dir" to point to a
        local path to be uploaded to the GCS prefix set in
        "gcs_staging_location".
    :type env: dict
    :param env: If set, passed to the dbt executor
    :param dbt_bin: The `dbt` CLI. Defaults to `dbt`, so assumes it's on your
        `PATH`
    :type dbt_bin: str

    :param project_id: GCP Project ID as stated in the console
    :type project_id: str
    :param timeout: Default is set in Cloud Build itself as ten minutes. A
        duration in seconds with up to nine fractional digits, terminated by
        's'. Example: "3.5s"
    :type timeout: str
    :param wait: Waits for the cloud build process to finish. That is waiting
        for the DBT command to finish running or run asynchronously
    :type wait: bool
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param gcs_staging_location: Where to store the sources to be fetch later
        by the cloud build job. It should be the GCS url for a folder. For
        example: `gs://my-bucket/stored. A sub-folder will be generated to
        avoid collision between possible different concurrent runs.
    :param gcs_staging_location: str
    :param dbt_version: the DBT version to be fetched from dockerhub. Defaults
        to '0.21.0'
    :type dbt_version: str
    """

    def __init__(
        self,
        project_id: str,
        dir: str = '.',
        gcs_staging_location: str = None,
        timeout: str = None,
        wait: bool = True,
        gcp_conn_id: str = "google_cloud_default",
        dbt_version: str = '0.21.0',
        env: Dict = None,
        dbt_bin='dbt',
    ):
        if dir is not None and gcs_staging_location is not None:
            logging.info(f'Files in "{dir}" will be uploaded to GCS with the '
                         f'prefix "{gcs_staging_location}"')
            # check the destination is a gcs directory and extract bucket and
            # folder
            if not gcs_object_is_directory(gcs_staging_location):
                raise ValueError(
                    f'The provided "gcs_sources_location": "'
                    f'{gcs_staging_location}"'
                    f' is not a valid gcs folder'
                )
            slb, slp = _parse_gcs_url(gcs_staging_location)
            # we have provided something similar to 'gs://<slb>/<slf>'
            self.gcs_staging_bucket = slb
            self.gcs_staging_blob = f'{slp}dbt_staging_{uuid4().hex}.tar.gz'
            self.upload = True

        elif dir is not None and gcs_staging_location is None:
            logging.info('Files in the "{dir}" blob will be used')
            staging_bucket, staging_blob = _parse_gcs_url(dir)
            if not staging_blob.endswith('.tar.gz'):
                raise AirflowException(
                    f'The provided blob "{dir}" to a compressed file does not '+
                    f'have the right extension ".tar.gz'
            )
            self.gcs_staging_bucket = staging_bucket
            self.gcs_staging_blob = staging_blob
            self.upload = False

        self.dbt_version = dbt_version
        self.cloud_build_hook = CloudBuildHook(gcp_conn_id=gcp_conn_id)
        self.gcp_conn_id = gcp_conn_id
        self.project_id = project_id
        self.timeout = timeout
        self.wait = wait

        super().__init__(dir=dir, env=env, dbt_bin=dbt_bin)

    def get_conn(self) -> Any:
        """Returns the cloud build connection, which is a gcp connection"""
        return self.cloud_build_hook.get_conn()

    def upload_dbt_sources(self) -> None:
        """Upload sources from local to a staging location"""
        gcs_hook = GCSHook(gcp_conn_id=self.gcp_conn_id)
        with \
                NamedTemporaryFile() as compressed_file, \
                tarfile.open(compressed_file.name, "w:gz") as tar:
            tar.add(self.dir, arcname=os.path.basename(self.dir))
            gcs_hook.upload(
                bucket_name=self.gcs_staging_bucket,
                object_name=self.gcs_staging_blob,
                filename=compressed_file.name,
            )

    def run_dbt(self, dbt_cmd: List[str]):
        """
         Run the dbt cli

         :param dbt_cmd: The dbt whole command to run
         :type dbt_cmd: List[str]
         """
        """See: https://cloud.google.com/cloud-build/docs/api/reference/rest
        /v1/projects.builds"""

        if self.upload:
            self.upload_dbt_sources()

        results = self.cloud_build_hook.create_build(
            build={
                'steps': [{
                    'name': f'fishtownanalytics/dbt:{self.dbt_version}',
                    'entrypoint': '/bin/sh',
                    'args': ['-c', *dbt_cmd],
                    'env': [f'{k}={v}' for k, v in self.env.items()]
                }],
                'source': {
                    'storageSource': {
                        "bucket": self.gcs_staging_bucket,
                        "object": self.gcs_staging_blob,
                    }
                }
            },
            project_id=self.project_id,
            wait=True,
            timeout=self.timeout,
            metadata=self.env,
        )
        logging.info(
            f'Triggered build {results["id"]}\nYou can find the logs at '
            f'{results["logUrl"]}'
        )

    def on_kill(self):
        """Stopping the build is not implemented until google providers v6"""
        raise NotImplementedError