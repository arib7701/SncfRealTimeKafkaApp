# SncfRealTimeKafkaApp

Get all disruptions of SNCF train's network in Real Time.
Use the following API : https://www.digital.sncf.com/startup/api

Aggregate statistics of different categories using Kafka Streams.
 - By Train Id (Total)
 - By Stop Location Id (Total)
 - By Cause (Total)
 - By Train Id Per Day
 - By Stop Location Id Per Day
 - By Cause Per Day
 
 Compute delays for each train/stop: 
 
 Send new computed data to Elastic Search DB using Kafka Connect. 
 
 
 To Run :
 - Download Confluent
 - Add Confluent to path
 - Start Confluent : confluent start
 - Run ./create-topics.sh
 - Run ./create-connectors.sh
 - Run "SncfApiProducer" to get data from API
 - Run "SncfDiscardDuplicates" to remove duplicated disruptions
 - Run "SncfDisruptionsAggregation" to get aggregated data
 - Run "SncfComputeDelays" to get delays per stop per train 
