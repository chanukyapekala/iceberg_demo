import os
import pandas as pd
import time

# Define the streaming source directory
streaming_source = os.path.join(os.getcwd(), "src/project/streaming/data")

# Ensure the directory exists
os.makedirs(streaming_source, exist_ok=True)

# Sample data to write
data = [
    {"id": 1, "name": "Antti", "age": 30, "city": "Helsinki", "country": "Finland", "occupation": "Engineer"},
    {"id": 2, "name": "Boris", "age": 25, "city": "Moscow", "country": "Russia", "occupation": "Scientist"},
    {"id": 3, "name": "Clara", "age": 28, "city": "Berlin", "country": "Germany", "occupation": "Artist"},
    {"id": 4, "name": "Dmitri", "age": 35, "city": "Moscow", "country": "Russia", "occupation": "Doctor"},
    {"id": 5, "name": "Eva", "age": 22, "city": "Oslo", "country": "Norway", "occupation": "Designer"},
    {"id": 6, "name": "Fiona", "age": 29, "city": "Stockholm", "country": "Sweden", "occupation": "Teacher"},
    {"id": 7, "name": "George", "age": 40, "city": "Copenhagen", "country": "Denmark", "occupation": "Manager"},
    {"id": 8, "name": "Hannah", "age": 31, "city": "Helsinki", "country": "Finland", "occupation": "Nurse"},
    {"id": 9, "name": "Igor", "age": 27, "city": "Tallinn", "country": "Estonia", "occupation": "Developer"},
    {"id": 10, "name": "Julia", "age": 26, "city": "Riga", "country": "Latvia", "occupation": "Analyst"}
]

# Function to write data in a streaming style
def write_to_parquet_streaming():
    # Define the output file path
    file_name = "streaming_records.parquet"
    file_path = os.path.join(streaming_source, file_name)

    # Initialize an empty DataFrame
    df = pd.DataFrame()

    for record in data:
        # Append the record to the DataFrame
        df = pd.concat([df, pd.DataFrame([record])], ignore_index=True)

        # Write the DataFrame to a Parquet file
        df.to_parquet(file_path, index=False)
        print(f"Appended record {record} to: {file_path}")

        # Sleep for 2 seconds to simulate streaming
        time.sleep(2)

# Run the producer
if __name__ == "__main__":
    write_to_parquet_streaming()