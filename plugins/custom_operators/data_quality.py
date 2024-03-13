from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables_and_columns=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables_and_columns = tables_and_columns

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Starting data quality checks for facts and dimmension tables")
        for table_info in self.tables_and_columns:
            table_name = table_info['table']
            table_columns = table_info['columns']

            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table_name}")

            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table_name} returned no results")
            num_records = records[0][0]

            if num_records < 1:
                raise ValueError(f"Data quality check failed. {table_name} contained 0 rows")

            nulls_query = "SELECT " + ", ".join(table_columns) + f" FROM {table_name} WHERE "
            column_condition = ["{col} IS NULL".format(col=col) for col in table_columns]
            nulls_query += " OR ".join(column_condition)

            null_results = redshift_hook.get_records(nulls_query)

            if len(null_results) > 0:
                raise ValueError(f"Data quality check failed. {table_name} has null results for query: {nulls_query}")

            self.log.info(f"Data quality on table {table_name} check passed with {records[0][0]} records")