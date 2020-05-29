from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    staging_sql_template = """
    COPY {} FROM '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    FORMAT AS json '{}';
    """
    
    @apply_defaults
    def __init__(self,
                 aws_key_id = "",
                 aws_access_key = "",
                 json_file = "auto",
                 redshift_conn_id = "",
                 s3_source = "",
                 table = "",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_key_id = aws_key_id
        self.aws_access_key = aws_access_key
        self.json_file = json_file
        self.redshift_conn_id = redshift_conn_id
        self.s3_source = s3_source
        self.table = table

    def execute(self, context):
        qry = self.staging_sql_template.format(self.table, self.s3_source, self.aws_key_id, self.aws_access_key, self.json_file)
        
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        self.log.info(f"Copying data from S3 and filling table {self.table}")
        redshift.run(qry)
        self.log.info("Successfully copied data.")