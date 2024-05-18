import csv
import time
import random
import logging
from collections import defaultdict
from moralis import evm_api
from concurrent.futures import ThreadPoolExecutor, as_completed
import queue
from datetime import datetime

# Constants
MAX_RETRIES = 5  # Maximum number of retries for fetching data
BASE_DELAY_SECONDS = 1  # Base delay in seconds before retrying
API_KEY = ""  # API key for Moralis
THREADS = 30  # Number of threads to use for concurrent processing

# Contracts
CONTRACTS = {
    "zk": "0xaBEA9132b05A70803a4E85094fD0e1800777fBEF",
    "scroll": "0x6774Bcbd5ceCeF1336b5300fb5186a12DDD8b367",
    "arb": "0x4Dbd4fc535Ac27206064B68FfCf827b0A60BAB3f",
    "zora": "0x1a0ad011913A150f69f6A19DF447A0CfD9551054",
    "cluster": "0x00000000000E1A99dDDd5610111884278BDBda1D"
}

# Function to parse timestamps and format as "YYYY-MM-DD"
def parse_and_format_timestamp(timestamp):
    try:
        if 'T' in timestamp:
            parsed_date = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
        else:
            parsed_date = datetime.strptime(timestamp, "%Y-%m-%d")
        return parsed_date.strftime("%Y-%m-%d")
    except ValueError:
        return None

# Function to parse timestamps and return datetime object
def parse_timestamp(timestamp):
    try:
        if 'T' in timestamp:
            return datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
        else:
            return datetime.strptime(timestamp, "%Y-%m-%d")
    except ValueError:
        return None

# Function to fetch transaction data for a given wallet address and network
def fetch_wallet_data(address, chain):
    print(f"Fetching data for wallet {address} on chain {chain}")
    params = {
        "chain": chain,
        "order": "DESC",
        "address": address
    }
    result = evm_api.wallets.get_wallet_history(api_key=API_KEY, params=params)
    return {"result": result.get("result", [])}

# Function to attempt fetching data with retries
def fetch_with_retries(wallet, chain):
    attempt = 0
    while attempt < MAX_RETRIES:
        try:
            wallet_data = fetch_wallet_data(wallet, chain)
            if 'result' in wallet_data and wallet_data['result']:
                return wallet_data['result']
            else:
                return []
        except Exception as e:
            print(f"Error fetching data for wallet {wallet}: {e}")
            delay = BASE_DELAY_SECONDS * (2 ** attempt) + random.uniform(0, 1)
            time.sleep(delay)
            attempt += 1
    return []

# Function to extract required transaction details
def extract_transaction_details(wallet, transactions):
    activation_day = None
    contract_data = {key: {'date': None, 'value': None, 'hash': None} for key in CONTRACTS}
    
    for tx in transactions:
        tx_timestamp = parse_timestamp(tx['block_timestamp'])
        
        if tx_timestamp is None:
            continue
        
        if tx['from_address'].lower() == wallet.lower():
            if activation_day is None or tx_timestamp < parse_timestamp(activation_day):
                activation_day = tx['block_timestamp']
        
        for transfer in tx.get('native_transfers', []):
            if transfer['from_address'].lower() == wallet.lower():
                for name, address in CONTRACTS.items():
                    if transfer['to_address'].lower() == address.lower():
                        if contract_data[name]['date'] is None or tx_timestamp < parse_timestamp(contract_data[name]['date']):
                            contract_data[name]['date'] = tx['block_timestamp']
                            contract_data[name]['value'] = transfer['value_formatted']
                            contract_data[name]['hash'] = tx['hash']

    if activation_day:
        activation_day = parse_and_format_timestamp(activation_day)
    for name in contract_data:
        if contract_data[name]['date']:
            contract_data[name]['date'] = parse_and_format_timestamp(contract_data[name]['date'])
    
    return activation_day, contract_data

# Function to process a single wallet and return the collected data
def process_wallet(index, wallet, chain, results_queue):
    try:
        wallet = wallet.strip()
        if wallet.startswith("0x"):
            transactions = fetch_with_retries(wallet, chain)
            activation_day, contract_data = extract_transaction_details(wallet, transactions)
            
            result = [
                wallet,
                activation_day if activation_day else 'N/A',
                contract_data['zk']['date'] if contract_data['zk']['date'] else 'N/A',
                contract_data['scroll']['date'] if contract_data['scroll']['date'] else 'N/A',
                contract_data['arb']['date'] if contract_data['arb']['date'] else 'N/A',
                contract_data['zora']['date'] if contract_data['zora']['date'] else 'N/A',
                contract_data['cluster']['date'] if contract_data['cluster']['date'] else 'N/A',
                contract_data['zk']['value'] if contract_data['zk']['value'] else 'N/A',
                contract_data['scroll']['value'] if contract_data['scroll']['value'] else 'N/A',
                contract_data['arb']['value'] if contract_data['arb']['value'] else 'N/A',
                contract_data['zora']['value'] if contract_data['zora']['value'] else 'N/A',
                contract_data['cluster']['value'] if contract_data['cluster']['value'] else 'N/A',
            ]
            
            # Calculate time differences
            date_pairs = [
                ('zk', 'scroll'),
                ('zk', 'arb'),
                ('zk', 'zora'),
                ('zk', 'cluster'),
                ('scroll', 'arb'),
                ('scroll', 'zora'),
                ('scroll', 'cluster'),
                ('arb', 'zora'),
                ('arb', 'cluster'),
                ('zora', 'cluster')
            ]
            for (a, b) in date_pairs:
                if contract_data[a]['date'] and contract_data[b]['date']:
                    time_diff = (parse_timestamp(contract_data[b]['date']) - parse_timestamp(contract_data[a]['date'])).days
                    result.append(time_diff)
                else:
                    result.append('N/A')

            # Time differences from activation
            for name in CONTRACTS:
                if activation_day and contract_data[name]['date']:
                    time_diff = (parse_timestamp(contract_data[name]['date']) - parse_timestamp(activation_day)).days
                    result.append(time_diff)
                else:
                    result.append('N/A')

            result.extend([
                contract_data['zk']['hash'] if contract_data['zk']['hash'] else 'N/A',
                contract_data['scroll']['hash'] if contract_data['scroll']['hash'] else 'N/A',
                contract_data['arb']['hash'] if contract_data['arb']['hash'] else 'N/A',
                contract_data['zora']['hash'] if contract_data['zora']['hash'] else 'N/A',
                contract_data['cluster']['hash'] if contract_data['cluster']['hash'] else 'N/A'
            ])
        else:
            result = [wallet] + [''] * 32
        
        results_queue.put((index, result))
    except Exception as e:
        results_queue.put((index, [wallet] + [''] * 32))

# Function to add details to the CSV
def add_details_to_csv(input_csv, output_csv, chain):
    with open(input_csv, mode='r') as infile, open(output_csv, mode='w', newline='') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        
        header = ['Wallet Address', 'Activation day', 'zk date', 'scroll date', 'arb date', 'zora date', 'cluster date',
                  'zk value', 'scroll value', 'arb value', 'zora value', 'cluster value',
                  'zk to scroll dif', 'zk to arb dif', 'zk to zora dif', 'zk to cluster dif',
                  'scroll to arb dif', 'scroll to zora dif', 'scroll to cluster dif',
                  'arb to zora dif', 'arb to cluster dif',
                  'zora to cluster dif',
                  'Activation to zk dif', 'Activation to scroll dif', 'Activation to arb dif', 'Activation to zora dif', 'Activation to cluster dif',
                  'Hash zk', 'Hash scroll', 'Hash arb', 'Hash zora', 'Hash cluster']
        writer.writerow(header)
        
        wallets = list(reader)
        results_queue = queue.Queue()
        with ThreadPoolExecutor(max_workers=THREADS) as executor:
            futures = [executor.submit(process_wallet, index, wallet[0], chain, results_queue) for index, wallet in enumerate(wallets)]
            for _ in as_completed(futures):
                pass

        # Collect results in the correct order
        results = [None] * len(wallets)
        while not results_queue.empty():
            index, updated_row = results_queue.get()
            results[index] = updated_row

        for row in results:
            writer.writerow(row)

# Define the input and output CSV file paths and the blockchain chain (e.g., 'eth')
input_csv_path = 'wallets.csv'
output_csv_path = 'wallets_with_info.csv'
blockchain_chain = 'eth'  # Replace with your desired blockchain chain

# Add details to the CSV
add_details_to_csv(input_csv_path, output_csv_path, blockchain_chain)
