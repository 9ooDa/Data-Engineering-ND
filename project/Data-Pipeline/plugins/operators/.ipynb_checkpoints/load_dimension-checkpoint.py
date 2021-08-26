from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    truncate_sql = "TRUNCATE {}"
    append_sql = """
        INSERT INTO {}
        {}
    """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id = "",
                 sql_stmt = "",
                 destination_table = "",
                 truncation = False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.sql_stmt = sql_stmt
        self.destination_table = destination_table
        self.truncation = truncation

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        if self.truncation == True:
            self.log.info('Truncating the table when the destination is not empty')
            truncate_table = LoadDimensionOperator.truncate_sql.format(
                self.destination_table
            )
            redshift.run(truncate_table)
            
        self.log.info('Inserting data into table')
        insert_table = LoadDimensionOperator.append_sql.format(
            self.destination_table,
            self.sql_stmt
        )
        
        redshift.run(insert_table)
        
