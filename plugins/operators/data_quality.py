from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class DataQualityOperator(BaseOperator):
    """
     Checking count of the table rows to ensure data quality for table on the Redshift
     redshift_conn_id : connection info for redshift
     dq_list : sql lists for checking tables
    """
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 dq_checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks

    def execute(self, context):

        self.log.info('Data quality started for tables list')

        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for check in self.dq_checks:

            check_sql = check["check_sql"]
            exp_result = check["expected_result"]
            
            result = redshift_hook.get_records(check_sql)

            if result[0][0] != exp_result:
                raise ValueError(f"Data quality checked failed for sql {check_sql}")
            else:
                self.log.info(f"Data quality checked is succesfull for sql {check_sql}")
