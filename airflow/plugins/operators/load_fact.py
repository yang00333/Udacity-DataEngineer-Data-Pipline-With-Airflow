from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.operators.postgres_operator import PostgresOperator

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 postgres_conn_id='',
                 sql='',
                 table='',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id=postgres_conn_id
        self.sql=sql
        self.table=table

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        redshift.run(self.sql)
        self.log.info("Loading complete.")