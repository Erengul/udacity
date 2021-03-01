from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    """
    Loading staging table from S3 path
    redshift_conn_id: connection info for redshift
    aws_credentials_id: credentials info for aws
    target_table: staging table name in Redshift
    s3_bucket: S3 bucket location
    s3_key: S3 path location
    region: AWS region for redshift
    delimiter : data delimiter in csv files.
    """
    ui_color = '#358140'
    # sql for getting data from S3 to Redshift
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION AS '{}'
        DELIMITER '{}'
        CSV
        dateformat 'auto'
        timeformat 'auto'
        NULL AS 'NA'
        IGNOREHEADER 1
        acceptinvchars
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 target_table="",
                 s3_bucket="",
                 s3_key="",
                 region="",
                 delimiter="",
                 *args, **kwargs):
        
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.target_table = target_table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.delimiter = delimiter


    def execute(self, context):
        self.log.info('StageToRedshift started')
        # set S3 and Reshift connection
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Redshift connection created.")

        # delete data from target table
        redshift.run("DELETE FROM {}".format(self.target_table))
        self.log.info("Delete data from target table {}".format(self.target_table))

        # copy data from S3 to reshift
        s3_path_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, s3_path_key)

        new_sql = StageToRedshiftOperator.copy_sql.format(
        self.target_table,
        s3_path,
        credentials.access_key,
        credentials.secret_key,
        self.region,
        self.delimiter
        )
        redshift.run(new_sql)

        self.log.info("Data inserted to target table")
