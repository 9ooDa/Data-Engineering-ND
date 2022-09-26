from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    #drop_sql = "DROP TABLE IF EXISTS {}"
    truncate_sql = "TRUNCATE {}"
    append_sql = """
        INSERT INTO {}
        {}
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 sql_stmt = "",
                 destination_table = "",
                 truncation = False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
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
        
        #self.log.info('Dropping the table in case it exists before execution')
        #drop_table = LoadDimensionOperator.drop_sql.format(
        #    self.destination_table
        #)
        #redshift.run(drop_table)