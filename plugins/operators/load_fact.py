from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    Loading fact tables from stagingtables
    redshift_conn_id : connection info for redshift
    table : target fact table to loading
    load_sql: Sql statement for loading data from staging tables
    """
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table='',
                 load_sql='',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.load_sql = load_sql

    insert_sql = """
                    INSERT INTO {} {};
                    COMMIT;
                """

    def execute(self, context):
        self.log.info('LoadFact is started')

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        formatted_sql = LoadFactOperator.insert_sql.format(
        self.table,
        self.load_sql
        )
        redshift.run(formatted_sql)

        self.log.info('Loading fact table is succesfull')
