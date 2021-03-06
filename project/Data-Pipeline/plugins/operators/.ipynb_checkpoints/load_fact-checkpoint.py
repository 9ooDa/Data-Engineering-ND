from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    
    #drop_sql = "DROP TABLE IF EXISTS {}"
    sql = """
        INSERT INTO {}
        {}
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 sql_stmt = "",
                 destination_table = "",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_stmt = sql_stmt
        self.destination_table = destination_table

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        self.log.info('Inserting data into table')
        load_fact = LoadFactOperator.sql.format(
            self.destination_table,
            self.sql_stmt
        )
        redshift.run(load_fact)
        
        #self.log.info('Dropping the table in case it exists before execution')
        #drop_table = LoadFactOperator.drop_sql.format(
        #    self.destination_table
        #)
        #redshift.run(drop_table)