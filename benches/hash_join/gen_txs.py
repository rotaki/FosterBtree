import csv
import argparse
import random
import time

def generate_transactions(data_file, output_file, total_ops, insert_ratio, update_ratio, delete_ratio):
    # Load data
    data = []
    with open(data_file, 'r') as csvfile:
        reader = csv.reader(csvfile)
        data = list(reader)

    # Calculate operation counts
    insert_count = int(total_ops * insert_ratio)
    update_count = int(total_ops * update_ratio)
    delete_count = int(total_ops * delete_ratio)

    operations = []
    # Generate insert operations
    for _ in range(insert_count):
        key, pkey, value = random.choice(data)
        ts = int(time.time() * 1000)
        tx_id = random.randint(1, 1000)
        operations.append(['insert', key, pkey, value, ts, tx_id])

    # Generate update operations
    for _ in range(update_count):
        key, pkey, value = random.choice(data)
        value = value[::-1]  # Example modification
        ts = int(time.time() * 1000)
        tx_id = random.randint(1, 1000)
        operations.append(['update', key, pkey, value, ts, tx_id])

    # Generate delete operations
    for _ in range(delete_count):
        key, pkey, _ = random.choice(data)
        value = ''  # Value is not needed for delete
        ts = int(time.time() * 1000)
        tx_id = random.randint(1, 1000)
        operations.append(['delete', key, pkey, value, ts, tx_id])

    # Shuffle operations to simulate randomness
    random.shuffle(operations)

    # Write transactions to file
    with open(output_file, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        for op in operations:
            writer.writerow(op)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate transactions for hash join table.')
    parser.add_argument('-df', '--data_file', default='data.csv', help='Input data file.')
    parser.add_argument('-o', '--output', default='transactions.csv', help='Output transactions file.')
    parser.add_argument('-t', '--total_ops', type=int, default=1000, help='Total number of operations.')
    parser.add_argument('-i', '--insert_ratio', type=float, default=0.7, help='Ratio of insert operations.')
    parser.add_argument('-u', '--update_ratio', type=float, default=0.2, help='Ratio of update operations.')
    parser.add_argument('-d', '--delete_ratio', type=float, default=0.1, help='Ratio of delete operations.')
    args = parser.parse_args()

    # Validate ratios
    total_ratio = args.insert_ratio + args.update_ratio + args.delete_ratio
    if abs(total_ratio - 1.0) > 0.001:
        print("Error: Ratios must sum to 1.0")
        exit(1)

    generate_transactions(
        args.data_file,
        args.output,
        args.total_ops,
        args.insert_ratio,
        args.update_ratio,
        args.delete_ratio
    )
