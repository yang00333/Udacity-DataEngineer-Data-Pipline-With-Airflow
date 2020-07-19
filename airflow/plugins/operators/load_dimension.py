from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 postgres_conn_id='',
                 sql='',
                 mode='append',
                 table='',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id=postgres_conn_id
        self.sql=sql
        self.table=table
        self.mode=mode

    def execute(self, context):
        if self.mode == 'truncate':
            self.log.info(f'Deleting data from {self.table} dimension table...')
            redshift_hook.run(f'DELETE FROM {self.table};')
            self.log.info("Deletion complete.")
        if self.mode == 'append':
            redshift = PostgresHook(postgres_conn_id=self.postgres_conn_id)
            insert_sql = """
                INSERT INTO {table}
                {select_sql};
                """.format(table=self.table, select_sql=self.sql)
            redshift.run(insert_sql)
            self.log.info("Loading complete.")