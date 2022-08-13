# bigquery_utils
BigQuery utilities


## Templates format

Templates are located in templates directory in YAML format.
Please specify the following parameters.

 name: Name of the Job.

 start_time: Start of the Job. ex: every 24 hours

 end_time: End of the Job. leave it empty in case if you dont want to end.

 destination_dataset: Data set the results are posted.

 destination_table: Destination table the results are posted. Prefix summary_ will be added to destination table.

 partition_on: Partition table on one of the results column. If the column dosent exist job will fail.

 results_append: Set true if results should be appended to table.

 location: geo location of the results set.

 query: Jinja template of the query 

 version: Version of the Job, incase if existing version gets bumped, job will be deleted and new job will be scheduled.
 
 ## Usage
 
 
