select count(*)
from raw_stream.website_events_stream_flink
where _processed_at > current_timestamp - interval '10' MINUTE;

select count(*)
from raw_stream.website_events_stream_flink;

select count(*)
from raw_stream.purchase_events_stream_flink;

select count(*)
from raw_stream.purchase_events_stream_flink
where _processed_at > current_timestamp - interval '10' MINUTE;



select *
from marts.stg_purchase_events
where dbt_loaded_at > current_timestamp - interval '2' Hour;

select count(*)
from marts.stg_website_events;
where dbt_loaded_at > current_timestamp - interval '10' MINUTE;



SELECT slot_name, plugin, active FROM pg_replication_slots;
SELECT * FROM pg_publication;