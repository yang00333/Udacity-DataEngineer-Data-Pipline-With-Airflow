from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self, table='',*args, **kwargs):
        self.table = table
        super(DataQualityOperator, self).__init__(*args, **kwargs)
    def execute(self, context):
        redshift_hook = PostgresHook("redshift")
        for t in self.table:
            print(t)
            records = redshift_hook.get_records("SELECT COUNT(*) FROM {}".format(t))
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError("Data quality check failed. {} returned no results".format(t))
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {t} contained 0 rows")
            self.log.info(f"Data quality on table {t} check passed with {records[0][0]} records")
                