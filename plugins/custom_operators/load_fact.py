from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 aws_credentials_id="aws_credentials",
                 table="",
                 create_table="",
                 insert_query="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.create_table = create_table
        self.insert_query = insert_query

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.create_table: 
            self.log.info("Creating table if it doesn't exists")
            redshift.run(self.create_table)
        else:
            self.log.info("No create table query provided, proceeding to insert")
        
        self.log.info("Running insert statement from staging tables")
        redshift.run(self.insert_query)