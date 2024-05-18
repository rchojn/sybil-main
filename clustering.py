import pandas as pd
import time
from datetime import timedelta
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import cpu_count

# Define a safe print function with delay
def safe_print(*args, **kwargs):
    print(*args, **kwargs)
    time.sleep(0.05)  # Sleep for 50ms to ensure print statements are readable

# Load the data
safe_print("Loading data...")
data = pd.read_csv('merged_main.csv')

# Ensure the correct column names and order
required_columns = [
    "Wallet Address", "Activation day", "zk date", "scroll date", "arb date", "zora date",
    "zk value", "scroll value", "arb value", "zora value",
    "zk to scroll dif", "zk to arb dif", "zk to zora dif",
    "scroll to arb dif", "scroll to zora dif",
    "arb to zora dif",
    "Activation to zk dif", "Activation to scroll dif", "Activation to arb dif",
    "Activation to zora dif", "Activation to cluster dif", 
    "Hash zk", "Hash scroll", "Hash arb", "Hash zora", "Hash cluster"
]
data = data[required_columns]

# Fill missing values with a placeholder
data = data.fillna('-')

# Convert date columns to datetime where possible, otherwise keep as '-'
date_columns = ["Activation day", "zk date", "scroll date", "arb date", "zora date"]
for col in date_columns:
    data[col] = data[col].apply(lambda x: pd.to_datetime(x, errors='coerce').date() if x != '-' else '-')

# Ensure dif columns are numeric
dif_columns_list = [
    "zk to scroll dif", "zk to arb dif", "zk to zora dif",
    "scroll to arb dif", "scroll to zora dif",
    "arb to zora dif"
]
for col in dif_columns_list:
    data[col] = pd.to_numeric(data[col], errors='coerce').fillna(0)

# Function to process a chunk of data
def process_chunk(chunk, dif_column, value_columns, date_columns):
    safe_print(f"Clustering based on {dif_column} for chunk...")
    clusters = []
    date_tolerance = 4  # Tolerance for differences in days
    date_range_tolerance = timedelta(days=4)  # Tolerance for date differences, including Activation day
    volume_tolerance = 0.30  # Percentage tolerance for volume differences

    # Sort chunk by the base dif column
    chunk = chunk.sort_values(dif_column)

    for index, row in chunk.iterrows():
        if row[dif_column] == 0:
            continue  # Skip rows where the base dif column is 0

        if not clusters:
            clusters.append([row.to_dict()])
        else:
            added_to_cluster = False
            for cluster in clusters:
                last_item = cluster[-1]
                if abs(row[dif_column] - last_item[dif_column]) <= date_tolerance:
                    avg_values = {col: sum(item[col] for item in cluster) / len(cluster) for col in value_columns}
                    value_diffs = {col: abs(row[col] - avg_values[col]) / avg_values[col] if avg_values[col] != 0 else 0 for col in value_columns}
                    
                    date_diffs = {col: abs(row[col] - last_item[col]) if (row[col] != '-' and last_item[col] != '-') else timedelta(days=7) for col in date_columns + ["Activation day"]}
                    
                    if all(diff <= volume_tolerance for diff in value_diffs.values()) and all(diff <= date_range_tolerance for diff in date_diffs.values()):
                        cluster.append(row.to_dict())
                        added_to_cluster = True
                        break
            if not added_to_cluster:
                clusters.append([row.to_dict()])

    return clusters

# Define parameters for each dif column
dif_columns = [
    ("zk to scroll dif", ["zk value", "scroll value"], ["zk date", "scroll date"]),
    ("zk to arb dif", ["zk value", "arb value"], ["zk date", "arb date"]),
    ("zk to zora dif", ["zk value", "zora value"], ["zk date", "zora date"]),
    ("scroll to arb dif", ["scroll value", "arb value"], ["scroll date", "arb date"]),
    ("scroll to zora dif", ["scroll value", "zora value"], ["scroll date", "zora date"]),
    ("arb to zora dif", ["arb value", "zora value"], ["arb date", "zora date"])
]

# Use concurrent.futures to perform clustering for each dif column
num_cores = min(cpu_count(), 32)
print(f"Using {num_cores} CPU cores for clustering.")
chunk_size = len(data) // num_cores
chunks = [data.iloc[i:i + chunk_size] for i in range(0, len(data), chunk_size)]

all_clusters = []
with ProcessPoolExecutor(max_workers=num_cores) as executor:
    futures = [executor.submit(process_chunk, chunk, dif_column, value_columns, date_columns)
               for chunk in chunks for dif_column, value_columns, date_columns in dif_columns]
    
    for future in as_completed(futures):
        all_clusters.extend(future.result())

# Filter clusters to only include those with more than 20 entries
final_clusters = [cluster for cluster in all_clusters if len(cluster) > 20]

# Prepare output
merged_output = []
for idx, cluster in enumerate(final_clusters):
    cluster_id = idx + 1
    cluster_size = len(cluster)
    cluster_id_row = {'Cluster ID': f'Cluster {cluster_id}', 'Size': cluster_size, 'Wallet Address': '---'}
    merged_output.append(cluster_id_row)
    merged_output.extend(cluster)

# Convert to DataFrame
merged_df = pd.DataFrame(merged_output)

# Reorder columns to ensure original columns are in place and new columns are added to the right
output_columns = ['Cluster ID', 'Size'] + required_columns
merged_df = merged_df[output_columns]

# Save the final validated clusters to a CSV file
output_file = 'merged_main_1.csv'
merged_df.to_csv(output_file, index=False)
safe_print(f"Clustering complete and saved to {output_file}.")

# Post-processing to sort clusters by size
def sort_clusters_by_size(input_file, output_file):
    # Load the data
    safe_print(f"Loading data for sorting from {input_file}...")
    data = pd.read_csv(input_file)
    
    # Find cluster headers and their indices
    cluster_headers = data[data['Wallet Address'] == '---']
    sorted_clusters = []
    
    for idx, header in cluster_headers.iterrows():
        cluster_id = header['Cluster ID']
        cluster_size = header['Size']
        header_index = header.name
        
        # Find the start and end of each cluster
        next_header_index = cluster_headers.index[cluster_headers.index > header_index].min()
        if pd.isna(next_header_index):
            next_header_index = len(data)
        
        # Extract the cluster including the header
        cluster = data.iloc[header_index:next_header_index]
        sorted_clusters.append((cluster_size, cluster))
    
    # Sort the clusters by size
    sorted_clusters.sort(reverse=True, key=lambda x: x[0])
    
    # Concatenate sorted clusters
    sorted_data = pd.concat([cluster for _, cluster in sorted_clusters], ignore_index=True)
    
    # Save sorted data to a new CSV file
    sorted_data.to_csv(output_file, index=False)
    safe_print(f"Sorted clusters saved to {output_file}")

# Sort clusters by size in the saved CSV
sort_clusters_by_size('merged_main1.csv', 'merged_main2.csv')

# Post-processing to ensure each address is only in the largest cluster
def ensure_unique_addresses(input_file, output_file):
    # Load the sorted data
    safe_print(f"Loading sorted data from {input_file}...")
    data = pd.read_csv(input_file)
    
    # Create a set to track addresses already included in the largest cluster
    unique_addresses = set()
    current_cluster_id = None
    current_cluster_size = 0
    rows_to_keep = []
    cluster_map = {}

    # Iterate through the data, keeping only the first occurrence of each address
    for idx, row in data.iterrows():
        if row['Wallet Address'] == '---':
            current_cluster_id = row['Cluster ID']
            current_cluster_size = row['Size']
            if current_cluster_id not in cluster_map:
                cluster_map[current_cluster_id] = []
            rows_to_keep.append(row)
        else:
            address = row['Wallet Address']
            if address not in unique_addresses:
                unique_addresses.add(address)
                cluster_map[current_cluster_id].append(row)
            else:
                continue

    # Remove any clusters that have become empty
    final_rows_to_keep = []
    for cluster_id, rows in cluster_map.items():
        if len(rows) > 0:
            final_rows_to_keep.extend([row for row in rows_to_keep if row['Cluster ID'] == cluster_id])
            final_rows_to_keep.extend(rows)

    # Convert the kept rows to a DataFrame
    unique_df = pd.DataFrame(final_rows_to_keep)
    
    # Save the data to a new CSV file
    safe_print("Saving clusters with unique addresses...")
    unique_df.to_csv(output_file, index=False)
    safe_print(f"Clusters with unique addresses saved to {output_file}")

# Ensure each address is only in the largest cluster
ensure_unique_addresses('merged_main2.csv', 'merged_main3.csv')

# Function to remove empty clusters and clusters with less than 20 entries, and recalculate Cluster Size
def remove_empty_or_small_clusters(input_file, output_file):
    # Load the data
    safe_print(f"Loading data for cleaning from {input_file}...")
    data = pd.read_csv(input_file)
    
    # Identify and remove empty clusters or clusters with fewer than 20 entries
    cleaned_data = []
    cluster_size = 0
    cluster_rows = []
    for idx, row in data.iterrows():
        if row['Wallet Address'] == '---':
            if cluster_size >= 20:
                # Save previous cluster if it has at least 20 entries
                for cluster_row in cluster_rows:
                    if cluster_row['Wallet Address'] == '---':
                        cluster_row['Size'] = cluster_size
                    cleaned_data.append(cluster_row)
            # Start a new cluster
            cluster_size = 0
            cluster_rows = [row]
        else:
            cluster_size += 1
            cluster_rows.append(row)

    # Handle the last cluster
    if cluster_size >= 20:
        for cluster_row in cluster_rows:
            if cluster_row['Wallet Address'] == '---':
                cluster_row['Size'] = cluster_size
            cleaned_data.append(cluster_row)

    # Convert cleaned data to DataFrame
    cleaned_df = pd.DataFrame(cleaned_data)
    
    # Save the cleaned data to a new CSV file
    safe_print("Saving cleaned data without empty or small clusters...")
    cleaned_df.to_csv(output_file, index=False)
    safe_print(f"Cleaned data saved to {output_file}")

# Remove empty clusters and clusters with fewer than 20 entries, and recalculate Cluster Size
remove_empty_or_small_clusters('merged_main3.csv', 'merged_main_final.csv')
