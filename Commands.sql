-- Use role
use role accountadmin;

-- Create database
create or replace database snowpipe_demo;

-- Create table 
create or replace table orders_data_lz(
    order_id int,
    product varchar(20),
    quantity int,
    order_status varchar(30),
    order_date date
);

-- Create a Cloud Storage Integration in Snowflake

create or replace storage integration gcs_bucket_read_int
 type = external_stage
 storage_provider = gcs
 enabled = true
 storage_allowed_locations = ('gcs://snowpipe_raw/');

-- Retrieve the Cloud Storage Service Account for your snowflake account
desc storage integration gcs_bucket_read_int;

-- Service account info for storage integration
--k97e00000@gcpuscentral1-1dfa.iam.gserviceaccount.com

-- Stage means reference to a specific external location where data will arrive
create or replace stage snowpipe_stage
  url = 'gcs://snowpipe_raw/'
  storage_integration = gcs_bucket_read_int;


-- Create PUB-SUB Topic and Subscription
-- gsutil notification create -t snowpipe_pubsub_topic -f json gs://snowpipe_raw/

-- create notification integration
create or replace notification integration notification_from_pubsub_int
 type = queue
 notification_provider = gcp_pubsub
 enabled = true
 gcp_pubsub_subscription_name = 'projects/hypnotic-shard-427008-v8/subscriptions/snowpipe_pubsub_topic-sub';

-- Describe integration
desc integration notification_from_pubsub_int;

-- Service account for PUB-SUB
-- ka7e00000@gcpuscentral1-1dfa.iam.gserviceaccount.com

-- Create Snow Pipe

Create or replace pipe gcs_to_snowflake_pipe
auto_ingest = true
integration = notification_from_pubsub_int
as
copy into orders_data_lz
from @snowpipe_stage
file_format = (type = 'CSV');

-- Check the history of ingestion
Select * 
from table(information_schema.copy_history(table_name=>'orders_data_lz', start_time=> dateadd(hours, -1, current_timestamp())));

select * from orders_data_lz;