
# Python 3.7.4


# utilities for running codes related to ingesting, segmentation and extracting
#from google.cloud import bigquery
#from google.cloud import storage
#from google.oauth2 import service_account
#from google.cloud import bigquery_datatransfer_v1
#import google.protobuf.json_format

class BigQueryUtils:
    def __init__(self,project, service_account_file_path=None):
        self.project = project
        self.service_account_file_path = service_account_file_path
        self.bq_client = self.create_bigquery_client()
        self.bq_transfer_client = self.create_bigquery_transfer_client()
    

    def create_bigquery_client(self):
        try:
            from google.cloud import bigquery
            from google.oauth2 import service_account
            if self.service_account_file_path:
                credentials = service_account.Credentials.from_service_account_file(
                                                                filename = self.service_account_file_path
                                                            )
                bq_client = bigquery.Client(
                    credentials=credentials,
                    project=self.project,
                )
                print("SERVICE ACCOUNT CLIENT ACTIVATED")
                return bq_client
            else:
                print("NOT SERVICE ACCOUNT CLIENT ACTIVATED")
                bq_client = bigquery.Client(
                    project=self.project,
                )
                return bq_client
        except Exception as e:
            print(e)


    def create_bigquery_transfer_client(self):
        try:
            from google.cloud import bigquery_datatransfer_v1
            if self.service_account_file_path:
                bigquery_transfer_client = bigquery_datatransfer_v1.DataTransferServiceClient().from_service_account_json(self.service_account_file_path)
                return bigquery_transfer_client
            else:
                print("bigquery_transfer_client needs service_account_file_path, no client was created.")
                return None
        except Exception as e:
            print(e)
        
    
    def create_table(self, schema,project_name, table_name, dataset_name, partition_column_day_name=None):
        """
        schema: table schema, should be created already.
        table_id: name of table.
        dataset_id: name of dataset table is located.
        partition_column_day: Binary, If yes then the name of column which it will be partition based on DAY should be provided.
        """
        try:
            from google.cloud import bigquery
            table_id = '{}.{}.{}'.format(project_name, dataset_name, table_name)
            table = bigquery.Table(table_id, schema=schema)

            if partition_column_day_name:
                table.time_partitioning = bigquery.TimePartitioning(
                       type_=bigquery.TimePartitioningType.DAY,
                       field=partition_column_day_name,        
                )
            table = self.bq_client.create_table(table)
            print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))

        except Exception as e:
            message = "during running create_table function, following error raised: {0}".format(e)
            print(e)
    

    def insert_query_results_to_table(self,project_name, dataset_name, table_name,query,write_disposition,create_disposition, allow_large_results=False):
        """
        writeDisposition: [WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY]
        createDisposition: [CREATE_IF_NEEDED, CREATE_NEVER]
        """
        try:
            from google.cloud import bigquery
            table_id = '{}.{}.{}'.format(project_name, dataset_name, table_name)
            job_config = bigquery.QueryJobConfig()
            job_config.destination=table_id
            job_config.create_disposition=create_disposition
            job_config.write_disposition=write_disposition
            if allow_large_results:
                job_config.allow_large_results=allow_large_results
                job_config.use_legacy_sql=True
            sql = query
            # Start the query, passing in the extra configuration.
            query_job = self.bq_client.query(sql, job_config=job_config)  # Make an API request.
            query_job.result()  # Wait for the job to complete.
            print("Query results loaded to the table {}".format(table_id))
        except Exception as e:
            message = "during running insert_query_results_to_table function, following error raised: {0}".format(e)
            print(e)


    def update_table_description(self, project_name, dataset_name, table_name, table_description):
        try:
            table_id = '{}.{}.{}'.format(project_name, dataset_name, table_name)
            table = self.bq_client.get_table(table_id)
            table.description = table_description
            table = self.bq_client.update_table(table, ["description"])  # API request
            print(
                "table '{}.{}.{}' description updated.".format(table.project, table.dataset_id, table.table_id)
                 )
        except Exception as e:
            message = "during running update_table_description function, following error raised: {0}".format(e)
            print(e)
    
    
    def update_table_expiration_time(self,project_name, dataset_name, table_name,number_of_days):
        try:
            import datetime
            import pytz

            table_id = '{}.{}.{}'.format(project_name, dataset_name, table_name)
            table = self.bq_client.get_table(table_id)

            # set table to expire number_of_days days from now
            expiration = datetime.datetime.now(pytz.utc) + datetime.timedelta(days=number_of_days)
            table.expires = expiration
            table = self.bq_client.update_table(table, ["expires"])  # API request

            # expiration is stored in milliseconds
            #margin = datetime.timedelta(microseconds=1000)
            #assert expiration - margin <= table.expires <= expiration + margin
            print(
                "table '{}.{}.{}' expires updated.".format(table.project, table.dataset_id, table.table_id)
                 )
        except Exception as e:
            message = "during running update_table_expiration_time function, following error raised: {0}".format(e)
            print(e)
    
    
    def copy_table(self,src_project_name, src_dataset_name, src_table_name,dst_project_name, dst_dataset_name, dst_table_name):
        try:
            source_table_id = "{}.{}.{}".format(src_project_name, src_dataset_name, src_table_name)
            destination_table_id = "{}.{}.{}".format(dst_project_name, dst_dataset_name, dst_table_name)

            job = self.bq_client.copy_table(source_table_id, destination_table_id)
            job.result()  # Wait for the job to complete.
            print("A copy of the table created.")
        except Exception as e:
            message = "during running copy_table function, following error raised: {0}".format(e)
            print(e)
    
    def delete_table(self,project_name, dataset_name, table_name):
        try:
            table_id = '{}.{}.{}'.format(project_name, dataset_name, table_name)
            # If the table does not exist, delete_table raises
            # google.api_core.exceptions.NotFound unless not_found_ok is True.
            self.bq_client.delete_table(table_id, not_found_ok=False)  # Make an API request.
            print("Deleted table '{}'.".format(table_id))
        except Exception as e:
            message = "during running delete_table function, following error raised: {0}".format(e)
            print(e)

    
    def create_view(self,project_name, view_name, dataset_name, view_query):
        try:
            from google.cloud import bigquery
            view_id = '{}.{}.{}'.format(project_name, dataset_name, view_name)
            view = bigquery.Table(view_id)
            view.view_query = view_query
            view = self.bq_client.create_table(view)
            print("Successfully created view at {}".format(view.full_table_id))
        except Exception as e:
            message = "during running create_view function, following error raised: {0}".format(e)
            print(e)

    
    def export_table_to_storage(self,project_name, dataset_name, table_name, bucket_name, file_name, gzip=None):
        try:
            from google.cloud import bigquery
            destination_uri = "gs://{}/{}".format(bucket_name, file_name)
            dataset_ref = bigquery.DatasetReference(project_name, dataset_name)
            table_ref = dataset_ref.table(table_name)

            job_config = bigquery.job.ExtractJobConfig()
            if gzip:
                job_config.compression = bigquery.Compression.GZIP
            extract_job = self.bq_client.extract_table(
                table_ref,
                destination_uri,
                # Location must match that of the source table.
                location="australia-southeast1",
                job_config=job_config,
            )  # API request
            extract_job.result()  # Waits for job to complete.

            print(
                "Exported {}:{}.{} to {}".format(project_name, dataset_name, table_name, destination_uri)
            )
        except Exception as e:
            message = "during running export_table_to_storage function, following error raised: {0}".format(e)
            print(e)


    def load_csv_to_bq_from_storage(self,
                                    project_name, 
                                    dataset_name, 
                                    table_name, 
                                    bucket_name, 
                                    file_name,
                                    create_disposition="CREATE_IF_NEEDED",
                                    write_disposition="WRITE_TRUNCATE",
                                    field_delimiter=",",
                                    skip_leading_rows=1, 
                                    maxBadRecords=0,
                                    autodetect=None,
                                    schema=None, 
                                    time_partitioning_day_field_name=None
                                    ):

        """
        createDisposition: [CREATE_IF_NEEDED, CREATE_NEVER]
        writeDisposition: [WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY]
        """
        try:
            from google.cloud import bigquery
            
            table_id = '{}.{}.{}'.format(project_name, dataset_name, table_name)
            
            job_config = bigquery.LoadJobConfig()
            job_config.source_format = bigquery.SourceFormat.CSV
            job_config.create_disposition = create_disposition
            job_config.write_disposition = write_disposition
            job_config.field_delimiter = field_delimiter
            job_config.maxBadRecords = maxBadRecords
            if autodetect:
                job_config.autodetect = autodetect
            if not autodetect:
                job_config.schema = schema
            if skip_leading_rows:
                job_config.skip_leading_rows = skip_leading_rows
            if time_partitioning_day_field_name:
                job_config.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field="date",  # Name of the column to use for partitioning.
                    # expiration_ms=7776000000,  # 90 days.
                )
                
            uri = "gs://{}/{}".format(bucket_name, file_name)
            
            load_job = self.bq_client.load_table_from_uri(
                uri, table_id, job_config=job_config
            )  # Make an API request.
            
            load_job.result()  # Wait for the job to complete.
            
            table = self.bq_client.get_table(table_id)
            print("Loaded {} rows to table {}".format(table.num_rows, table_id))
        except Exception as e:
            message = "during running load_csv_to_bq_from_storage function, following error raised: {0}".format(e)
            print(e)

    

    def list_tables(self, project_name, dataset_name):
        try:
            dataset_id = '{}.{}'.format(project_name,dataset_name)
            tables = self.bq_client.list_tables(dataset_id)  # Make an API request.
            print("Tables contained in '{}':".format(dataset_id))
            for table in tables:
                print("{}.{}.{}".format(table.project, table.dataset_id, table.table_id))
            return tables
        except Exception as e:
            message = "during running list_tables function, following error raised: {0}".format(e)
            print(e)
        
    
    def extract_table_info(self, project_name, dataset_name, table_name):
        try:
            table_id = '{}.{}.{}'.format(project_name, dataset_name, table_name)
            table = self.bq_client.get_table(table_id)  # Make an API request.
            
            # View table properties
            print(
                "Got table '{}.{}.{}'.".format(table.project, table.dataset_id, table.table_id)
            )
            print("Table schema: {}".format(table.schema))
            print("Table description: {}".format(table.description))
            print("Table has {} rows".format(table.num_rows))
            return table.schema
        except Exception as e:
            message = "during running get_table_schema function following error raised: {0}".format(e)
            print(message)
    

    def run_query(self, query):
        try:
            query_job = self.bq_client.query(query)  # Make an API request.
            query_job.result()  # Wait for the job to complete.
            print("Query completed")
        except Exception as e:
            message = "during running run_query function, following error raised: {0}".format(e)
            print(e)

    def create_transfer(self,
                        project_name, 
                        dataset_name, 
                        table_name, 
                        display_name, 
                        schedule_time,
                        data_path_template,
                        field_delimiter,
                        file_format="CSV",
                        max_bad_records="0",
                        skip_leading_rows="1",
                        location='australia-southeast1', 
                        data_source_id="google_cloud_storage",
                        notification_pubsub_topic=None
                        ):
        """
        schedule_time: e.g. "every fri 16:00"
        data_source_id: ["scheduled_query", "google_cloud_storage"]
        """
        try:
            from google.cloud import bigquery_datatransfer_v1
            import google.protobuf.json_format

            parent = self.bq_transfer_client.location_path(project_name, location)
            transfer_config = google.protobuf.json_format.ParseDict(
                {
                    "destination_dataset_id": dataset_name,
                    "display_name": display_name,
                    "data_source_id": data_source_id,
                    "params": {
                        "destination_table_name_template": table_name,
                        "write_disposition": "APPEND",
                        # "partitioning_field": "",
                        # "partitioning_field": "DAY(insertDate)",
                        "field_delimiter": field_delimiter,
                        "max_bad_records": max_bad_records,
                        "skip_leading_rows": skip_leading_rows,
                        "data_path_template": data_path_template,
                        "file_format": file_format,
                    },
                    "schedule": schedule_time,
                    "notification_pubsub_topic": notification_pubsub_topic,
                },
                bigquery_datatransfer_v1.types.TransferConfig(),
            )
            response = self.bq_transfer_client.create_transfer_config(
                parent, transfer_config
            )
            print("Created transfer '{}'".format(response.name))
        except Exception as e:
            message = "during running create_transfer function, following error raised: {0}".format(e)
            print(e)

    def create_query_scheduler(self, 
                               query, 
                               schedule_time, 
                               project_name, 
                               display_name, 
                               data_source_id, 
                               location,
                               notification_pubsub_topic=None):
        """
        schedule_time: e.g. "every fri 16:00"
        data_source_id: ["scheduled_query", "google_cloud_storage"]
        """
        try:
            from google.cloud import bigquery_datatransfer_v1
            import google.protobuf.json_format
            parent = self.bq_transfer_client.location_path(project_name, location)
            transfer_config = google.protobuf.json_format.ParseDict(
                {
                    "display_name": display_name,
                    "data_source_id": "scheduled_query",
                    "params": {
                        "query": query,
                    },
                    "schedule": schedule_time,
                    "notification_pubsub_topic": notification_pubsub_topic,
                },
                bigquery_datatransfer_v1.types.TransferConfig(),
            )
            response = self.bq_transfer_client.create_transfer_config(
                parent, transfer_config
            )
            print("Created scheduled query '{}'".format(response.name))
        except Exception as e:
            message = "during running create_query_scheduler function, following error raised: {0}".format(e)
            print(e)   

 



if __name__ == "__main__":
    project = "winterfell-winter"
    dataset_name = "dsProcess"
    table_name = "exp_7plus"
    bqutils = BigQueryUtils(project=project, service_account_file_path= "C:/tega/gcloud_service_account/tega-swm-47b706f46835.json")
    bqutils.extract_table_info(project_name=project, dataset_name=dataset_name, table_name=table_name)