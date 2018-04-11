# Start All Services - you can use -d to run as daemon

docker-compose up


# Configure Postgres connector - This adds the stream from PostgreSQL WAL into Kafka

curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://localhost:8083/connectors/ -d @register-postgres.json


# Start Kafka DB Event Consumer/Denormalizer - Simple Consumer written to denormalize the companies_view

docker-compose run app python -c "from consumer import CDCConsumer; CDCConsumer().run();"


# InitDB with Dummy Data - Creates the tables and inserts dummy data 30K. 
# Note: It doesnt insert data into the denormalized view. The consumer handles that automatically by reading the create/update events.

docker-compose run --rm app python -c "from db import initdb, dummy_data; initdb(); dummy_data()"


# Connect to PostgresSQL
psql -h localhost -U user poc
# Password: pass
# You can make changes directly to the companies, people and users tables and see the denormalized table company_view_denormalized get updated. 

## The query that is being denormalized
SELECT companies.id,
       companies.name AS company_name,
       companies.phone AS company_phone,
       companies.website AS company_website,
       ceo.name AS ceo_name,
       primary_contact.name AS primary_contact_name,
       primary_contact.phone AS primary_contact_phone,
       sold_by.name AS sold_by_name,
       manager.name as sold_by_manager_name
FROM companies
LEFT JOIN people as ceo ON ceo.id = companies.ceo_id
LEFT JOIN people as primary_contact ON primary_contact.id = companies.primary_contact_id
LEFT JOIN users as sold_by ON companies.sold_by_id = sold_by.id
LEFT JOIN users as manager ON manager.id = sold_by.manager_id


# Note: To get a peek into the Debezium kafka message structure

docker-compose exec kafka /kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 \
    --from-beginning \
    --property print.key=true \
    --topic dbserver1.public.companies


# Stop Docker containers
docker-compose stop

# Destroy docker containers
docker-compose down


