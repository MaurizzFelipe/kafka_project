from kafka import KafkaProducer, KafkaConsumer
import json
import time
import requests
import psycopg2

# Set your NASDAQ API key here
nasdaq_api_key = 'INSERT YOU API KEY HERE'

# Set the NASDAQ API endpoint for FB (Facebook) stock data
nasdaq_endpoint = 'https://data.nasdaq.com/api/v3/datasets/BSE/BOM500112.json?' # change the api as required

# Set your Kafka broker's IP and port here
kafka_broker = 'localhost:9092'

#You can also set this as environment variables and us os get pass so you dont need to hardcode your credentials
# Set the Kafka topic name to produce data to
kafka_topic = 'insert topic name'
# Set your PostgreSQL credentials
postgres_host = "localhost" # insert your host
postgres_port = 5432 #default
postgres_database = "your-data-base-name" # insert database name
postgres_user = "your-user-name" # insert username 
postgres_password = "your-password" #insert password
new_schema = "your-schema" #insert schema , public is default
new_table = "table-name" #insert the table that will receive the data

# Create a KafkaProducer instance
producer = KafkaProducer(bootstrap_servers=kafka_broker,
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

# Function to fetch data from NASDAQ API
def fetch_nasdaq_data():
    try:
        response = requests.get(nasdaq_endpoint, params={'api_key': nasdaq_api_key})
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as http_err: # Debbuging in order to know any errors that may block our connection
        print(f'HTTP error occurred: {http_err}')
    except requests.exceptions.RequestException as req_err:
        print(f'Request exception occurred: {req_err}')
    except Exception as err:
        print(f'An error occurred: {err}')
    return None


# Function to save data to PostgreSQL
def save_to_postgresql(data):
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=postgres_host,
            port=postgres_port,
            database=postgres_database,
            user=postgres_user,
            password=postgres_password
        )

        # Create a cursor
        cursor = conn.cursor()

        # Define the PostgreSQL table name
        table_name = f"{new_schema}.{new_table}"

        # Assuming the 'data' field is a list of lists, you can insert each row into the table
        insert_query = f"INSERT INTO {table_name} (date, open, high, low, close, wap) VALUES (%s, %s, %s, %s, %s, %s)"

        for row in data['dataset']['data']:
            date = row[0]
            open_price = row[1]
            high = row[2]
            low = row[3]
            close = row[4]
            wap = row[5]

            # Insert the row into the table
            cursor.execute(insert_query, (date, open_price, high, low, close, wap))

        # Commit the changes and close the cursor and connection
        conn.commit()
        cursor.close()
        conn.close()

        print("Data successfully saved to PostgreSQL.")

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL:", error)

# This will fetch data 1 million times,  sometimes you wont need this
# Main function to fetch data from NASDAQ API, send to Kafka, and save to PostgreSQL
def main():
    call_count = 0
    while call_count < 1000001:
        # Fetch data from NASDAQ API
        data = fetch_nasdaq_data()
        if data:
            # Send data to Kafka topic
            producer.send(kafka_topic, value=data)
            print(f'Sent data to Kafka: {data}')

            # Save data to PostgreSQL
            save_to_postgresql(data)

        call_count += 1
        time.sleep(3)  # Wait for 3 seconds before fetching data again, so we dont overload the api

if __name__ == "__main__":
    main()
