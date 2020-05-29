from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 delete_on_load = False,
                 sql_statement = "",
                 table = "",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.delete_on_load = delete_on_load
        self.sql_statement = sql_statement
        self.table = table
        
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)    
        
        if self.delete_on_load:
            self.log.info(f"Clearing table {self.table}")
            redshift.run(f"DELETE FROM {self.table}")   
            self.log.info(f"Table {self.table} cleared") 
            
        self.log.info(f"Filling {self.table}")
        redshift.run(self.sql_statement)
        self.log.info(f"Table {self.table} filled")
