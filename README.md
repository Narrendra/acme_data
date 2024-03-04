Acme data analysis:

Tool stack used:
    Azure for storage 
    Databricks for development

Ingestion:

I have created respective containers for location, product and transaction data in the azure blob storage and copied all the raw data to the containers 
To ingest files:
    I first created connection from databricks to azure via access key and then ingested raw data to databricks as delta tables 

Curation:
    Upon successful ingestion, I have created another notebook for curation where I performed all the data cleasing and analysis work
