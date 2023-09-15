from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator, Variable
from airflow.utils.decorators import apply_defaults
from sql import SqlQueries

class StageToRedshiftOperator(BaseOperator):

    """
    This operator is able to load given dataset in JSON or CSV format
    from specified location on S3 into target Redshift table.
    """

    ui_color = '#358140'

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="",
        aws_credentials_id="",
        table="",
        s3_bucket="",
        s3_key="",
        region = "",
        create_table = "",
        file_format="",
        *args, **kwargs
    ):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = Variable.get(s3_bucket)
        self.s3_key = s3_key
        self.file_format = file_format
        self.region = region
        self.create_table = create_table
        self.aws_credentials_id = aws_credentials_id

    def execute(self, context):

        try:
            redshift_hook = PostgresHook(self.redshift_conn_id)
            aws_hook = AwsHook(self.aws_conn_id)
            credentials = aws_hook.get_credentials()

            self.log.info(f"Clearing data from Redshift {self.table}.")
            redshift_hook.run("DROP TABLE IF EXISTS {}".format(self.table))

            redshift_hook.run(self.create_table)
            self.log.info(f'StageToRedshiftOperator will start copying data from S3 to Redshift.')

            redshift_hook.run(SqlQueries.truncate_table.format(self.target_table))
        except:
            raise ValueError("Unable to connect AWS and Airflow")

        rendered_key = self.s3_key.format(context['execution_date'])
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = SqlQueries.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.format
        )
        redshift_hook.run(formatted_sql)


class LoadFactOperator(BaseOperator):

    """
    This operator is able to load data into Fact table 
    by executing specified SQL statement for specified target table.
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id='',
        table = "",
        create_table="",
        load_table="",
        *args, **kwargs
    ):

        super(LoadFactOperator, self).__init__(*args, **kwargs)

        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.create_table = create_table
        self.load_table = load_table

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Create if {self.table} not exists in Redshift.")
        redshift_hook.run(self.create_table)

        self.log.info(f"Loading into destination Redshift {self.table}.")
        redshift_hook.run("INSERT INTO {} {}".format(self.table, self.load_table))
        self.log.info(f"Done inserting data into fact table: {self.table}.")


class LoadDimensionOperator(BaseOperator):

    """
    This operator is able to load data into Dimension table 
    by executing specified SQL statement for specified target table.
    It also allows to optionally truncate the dimension table before inserting new data into it.
    """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id='',
        table = "",
        create_table="",
        load_table="",
        truncateInsert=True,
        *args, **kwargs
    ):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
                
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.create_table = create_table
        self.load_table = load_table
        self.truncateInsert = truncateInsert

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from Redshift.")
        if self.truncateInsert == True:
            redshift_hook.run(f"DROP TABLE IF EXISTS {self.table}.")

        redshift_hook.run(self.create_table)

        self.log.info("Loading into destination Redshift table")
        load_sql_str = LoadDimensionOperator.load_sql.format(self.table, self.load_table)
        redshift_hook.run(load_sql_str)
        self.log.info(f"Done inserting data into dimension table: {self.table}.")


class DataQualityOperator(BaseOperator):

    """
    This operator is able to perform quality checks of the data in tables.
    It accepts list of pairs of sql statement and expected value as arguments.
    For each SQL statement, it executes it on Redshift and 
    compares the retrieved result with expected value.
    In case of mismatch, it raises exception to indicate failure of test.
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id='',
        sql_tests=[],
        expected_results = [],
        *args, **kwargs
    ):

        super(DataQualityOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.sql_tests = sql_tests
        self.expected_results = expected_results

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if len(self.sql_tests) != len(self.expected_results):
            raise ValueError("Tests and expected results do not match in lengths.")

        for i in range(len(self.sql_tests)):
            res = redshift_hook.get_first(self.sql_tests[i])
            if res[0] != self.expected_results[i]:
                raise ValueError(f"Test {i} failed.")
            else:
                self.log.info(f"Test {i} passed.")
