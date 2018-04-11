# Start All Services - you can use -d to run as daemon
`docker-compose up`

# Configure Postgres connector - This adds the stream from PostgreSQL WAL into Kafka
`curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://localhost:8083/connectors/ -d @register-postgres.json`

# Start Kafka DB Event Consumer/Denormalizer - Simple Consumer written to denormalize the companies_view

`docker-compose run app python -c "from consumer import CDCConsumer; CDCConsumer().run();"`

# InitDB with Dummy Data - Creates the tables and inserts dummy data. 
# Note: It doesnt insert data into the denormalized view. The consumer handles that by reading the create/update events.
`docker-compose run --rm app python -c "from db import initdb, dummy_data; initdb(); dummy_data()"`

`psql -h localhost -U user poc`
# Password: pass

# You can make changes directly to the companies, people and users tables and see the denormalized table company_view_denormalized get updated. 

# Note: To get a peek into the Debezium kafka message structure
`docker-compose exec kafka /kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 \
    --from-beginning \
    --property print.key=true \
    --topic dbserver1.public.companies`