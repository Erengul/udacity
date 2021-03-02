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
                 dq_checks1=[],
                 dq_checks2=[],
                 dq_type1='',
                 dq_type2='',
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dq_type1 = dq_type1
        self.dq_type2 = dq_type2
        self.dq_checks1 = dq_checks1
        self.dq_checks2 = dq_checks2
        
    def execute(self, context):

        self.log.info('Data quality started for tables list')

        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # Data Quality Check 1 : Null values for primary keys
        if self.dq_type1 == "null_values":
            for check in self.dq_checks1:

                check_sql = check["check_sql"]
                exp_result = check["expected_result"]

                result = redshift_hook.get_records(check_sql)

                if result[0][0] != exp_result:
                    raise ValueError(f"Data quality checked for null values failed for sql {check_sql}")
                else:
                    self.log.info(f"Data quality checked for null values is succesfull for sql {check_sql}")
        
        # Data Quality Check 2 : Duplicated values for primary keys

        if self.dq_type2 == "duplicated_values":
            for check in self.dq_checks2:

                check_sql = check["check_sql"]
                exp_result = check["expected_result"]

                result = redshift_hook.get_records(check_sql)

                if result[0][0] != exp_result:
                    raise ValueError(f"Data quality checked for duplicated values failed for sql {check_sql}")
                else:
                    self.log.info(f"Data quality checked for duplicated values is succesfull for sql {check_sql}")