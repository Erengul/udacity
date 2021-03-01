from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class LoadDimensionOperator(BaseOperator):
    """
    Loading dimension tables from fact tables
    redshift_conn_id : connection info for redshift
    table : target dimension table to Loading
    load_sql: Sql statement for loading data from staging and fact tables to dimension table
    """
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                  redshift_conn_id='',
                  table='',
                  load_sql='',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.load_sql = load_sql

    insert_sql = """TRUNCATE TABLE {};
                    INSERT INTO {} {};
                    COMMIT;
                """

    def execute(self, context):
        self.log.info(f'LoadDimension {self.table} is started')

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        formatted_sql = LoadDimensionOperator.insert_sql.format(
        self.table,
        self.table,
        self.load_sql
        )
        redshift.run(formatted_sql)

        self.log.info(f'Loading dimension table {self.table} is succesfull')
