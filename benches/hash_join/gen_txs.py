import csv
import argparse
import random
from collections import defaultdict
import string  # Import string module

def generate_transactions_and_operations(
    data_file,
    txs_file,
    ops_file,
    num_transactions,
    min_cmds_per_tx,
    max_cmds_per_tx,
    read_only_ratio,
    insert_ratio,
    update_ratio,
    delete_ratio
):
    # Load data and determine pkey length
    data = []
    pkey_lengths = set()
    value_lengths = []  # To collect lengths of values
    with open(data_file, 'r') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            if len(row) < 3:
                continue  # Skip invalid rows
            key, pkey_str, value = row
            data.append((key, pkey_str, value))
            pkey_lengths.add(len(pkey_str))
            value_lengths.append(len(value))  # Collect value lengths

    # Ensure pkey lengths are consistent
    if len(pkey_lengths) != 1:
        raise ValueError("Inconsistent pkey lengths in data file.")
    else:
        pkey_length = pkey_lengths.pop()

    # Determine min and max value lengths from data
    if value_lengths:
        min_value_length = min(value_lengths)
        max_value_length = max(value_lengths)
    else:
        # If data is empty, use default lengths
        min_value_length = 32
        max_value_length = 64

    # Prepare for generating new pkeys
    existing_pkeys = set(pkey_str for _, pkey_str, _ in data)
    existing_pkey_numbers = [int(pkey.strip('_')) for pkey in existing_pkeys if pkey.strip('_').isdigit()]
    max_existing_pkey = max(existing_pkey_numbers) if existing_pkey_numbers else 0
    new_pkey_counter = max_existing_pkey + 1

    # Initialize the pool of pkeys for operations
    available_pkeys = set(existing_pkeys)
    pkey_info = {}  # Maps pkey to (key, pkey, value)

    for key, pkey, value in data:
        pkey_info[pkey] = (key, pkey, value)

    # Validate ratios
    if abs(insert_ratio + update_ratio + delete_ratio - 1.0) > 0.001:
        raise ValueError("Insert, update, and delete ratios must sum to 1.0")
    if not 0 <= read_only_ratio <= 1:
        raise ValueError("Read-only ratio must be between 0 and 1")

    total_read_only_txs = int(num_transactions * read_only_ratio)
    total_read_write_txs = num_transactions - total_read_only_txs

    # Generate transactions
    transactions = []
    tx_id = 1
    ts = 1

    for _ in range(num_transactions):
        num_cmds = random.randint(min_cmds_per_tx, max_cmds_per_tx)
        commands = []
        is_read_only = random.random() < read_only_ratio
        for _ in range(num_cmds):
            if is_read_only:
                # Read-only transaction
                if not available_pkeys:
                    continue  # No available pkeys to read
                pkey = random.choice(list(available_pkeys))
                key, pkey_str, _ = pkey_info[pkey]
                commands.append({
                    'tx_id': tx_id,
                    'ts': ts,
                    'op_type': 'get',
                    'key': key,
                    'pkey': pkey_str,
                    'value': ''
                })
            else:
                # Read-write transaction
                op_type = random.choices(
                    ['insert', 'update', 'delete'],
                    weights=[insert_ratio, update_ratio, delete_ratio],
                    k=1
                )[0]
                if op_type == 'insert':
                    # Generate a new pkey with the same length, padded with underscores
                    new_pkey_str = str(new_pkey_counter)
                    if len(new_pkey_str) < pkey_length:
                        new_pkey = new_pkey_str.rjust(pkey_length, '_')
                    else:
                        new_pkey = new_pkey_str[:pkey_length]
                    new_pkey_counter += 1
                    # Select a key from the key pool
                    if data:
                        key = random.choice(data)[0]
                    else:
                        key = 'key'.ljust(pkey_length, '_')[:pkey_length]  # Default key if data is empty
                    # Generate a new value using min and max lengths
                    value_length = random.randint(min_value_length, max_value_length)
                    new_value = ''.join(random.choices(string.ascii_letters + string.digits, k=value_length))
                    commands.append({
                        'tx_id': tx_id,
                        'ts': ts,
                        'op_type': 'insert',
                        'key': key,
                        'pkey': new_pkey,
                        'value': new_value
                    })
                    # Add new pkey to available pkeys
                    available_pkeys.add(new_pkey)
                    pkey_info[new_pkey] = (key, new_pkey, new_value)
                else:
                    if not available_pkeys:
                        continue  # No pkeys available for update/delete
                    pkey = random.choice(list(available_pkeys))
                    key, pkey_str, value = pkey_info[pkey]
                    if op_type == 'update':
                        # Generate a new random value
                        value_length = random.randint(min_value_length, max_value_length)
                        updated_value = ''.join(random.choices(string.ascii_letters + string.digits, k=value_length))
                        commands.append({
                            'tx_id': tx_id,
                            'ts': ts,
                            'op_type': 'update',
                            'key': key,
                            'pkey': pkey_str,
                            'value': updated_value
                        })
                        # Update the value in pkey_info
                        pkey_info[pkey] = (key, pkey_str, updated_value)
                    elif op_type == 'delete':
                        commands.append({
                            'tx_id': tx_id,
                            'ts': ts,
                            'op_type': 'delete',
                            'key': key,
                            'pkey': pkey_str,
                            'value': ''
                        })
                        # Remove pkey from available pkeys
                        available_pkeys.remove(pkey)
                        del pkey_info[pkey]
        if commands:
            transactions.append({
                'tx_id': tx_id,
                'ts': ts,
                'commands': commands
            })
            tx_id += 1
            ts += 1

    # Write transactions to txs.csv
    with open(txs_file, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        for tx in transactions:
            for cmd in tx['commands']:
                writer.writerow([
                    cmd['tx_id'], cmd['ts'], cmd['op_type'], cmd['key'], cmd['pkey'], cmd['value']
                ])
            # Add an empty line between transactions for readability
            writer.writerow([])

    print(f"Generated {len(transactions)} transactions in {txs_file}")

    # Generate operations for ops.csv
    # Collect all commands into a single list
    all_operations = []
    op_id = 0
    for tx in transactions:
        for cmd in tx['commands']:
            cmd['id'] = op_id
            all_operations.append(cmd)
            op_id += 1

    # Build dependency graph
    graph = defaultdict(set)
    in_degree = defaultdict(int)
    pkey_ops = defaultdict(list)

    # Collect operations per pkey
    for op in all_operations:
        pkey_ops[op['pkey']].append(op)

    # For each pkey, sort operations by ts and add edges
    for pkey, ops in pkey_ops.items():
        ops.sort(key=lambda x: x['ts'])
        for i in range(len(ops) - 1):
            from_op = ops[i]['id']
            to_op = ops[i + 1]['id']
            graph[from_op].add(to_op)
            in_degree[to_op] += 1

    # Perform randomized topological sort
    zero_in_degree = [op['id'] for op in all_operations if in_degree[op['id']] == 0]
    random.shuffle(zero_in_degree)
    sorted_ops = []
    while zero_in_degree:
        op_id = zero_in_degree.pop()
        op = all_operations[op_id]
        sorted_ops.append(op)
        for neighbor in graph[op_id]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                zero_in_degree.append(neighbor)
                random.shuffle(zero_in_degree)

    if len(sorted_ops) != len(all_operations):
        raise ValueError("Cycle detected in operations; cannot perform topological sort.")

    # Write mixed operations to ops.csv
    with open(ops_file, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        for op in sorted_ops:
            writer.writerow([
                op['tx_id'], op['ts'], op['op_type'], op['key'], op['pkey'], op['value']
            ])

    print(f"Generated mixed operations in {ops_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Generate transactions and mixed operations for hash join table.',
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        '-df', '--data_file',
        default='data.csv',
        help='Input data file containing keys, pkeys, and values. (default: data.csv)'
    )
    parser.add_argument(
        '-tf', '--txs_file',
        default='txs.csv',
        help='Output transactions file to write generated transactions. (default: txs.csv)'
    )
    parser.add_argument(
        '-of', '--ops_file',
        default='ops.csv',
        help='Output operations file with mixed order. (default: ops.csv)'
    )
    parser.add_argument(
        '-n', '--num_transactions',
        type=int,
        default=10,
        help='Total number of transactions to generate. (default: 10)'
    )
    parser.add_argument(
        '-minc', '--min_cmds_per_tx',
        type=int,
        default=5,
        help='Minimum number of commands per transaction. (default: 5)'
    )
    parser.add_argument(
        '-maxc', '--max_cmds_per_tx',
        type=int,
        default=10,
        help='Maximum number of commands per transaction. (default: 10)'
    )
    parser.add_argument(
        '-ro', '--read_only_ratio',
        type=float,
        default=0.1,
        help=(
            'Ratio of read-only transactions.\n'
            'Specifies the fraction of transactions that are read-only (contain only get operations).\n'
            'Must be between 0 and 1. (default: 0.1)'
        )
    )
    parser.add_argument(
        '-i', '--insert_ratio',
        type=float,
        default=0.3,
        help='Ratio of insert operations in read-write transactions. (default: 0.3)'
    )
    parser.add_argument(
        '-u', '--update_ratio',
        type=float,
        default=0.5,
        help='Ratio of update operations in read-write transactions. (default: 0.5)'
    )
    parser.add_argument(
        '-d', '--delete_ratio',
        type=float,
        default=0.2,
        help='Ratio of delete operations in read-write transactions. (default: 0.2)'
    )
    args = parser.parse_args()

    # Validate ratios
    total_ratio = args.insert_ratio + args.update_ratio + args.delete_ratio
    if abs(total_ratio - 1.0) > 0.001:
        print("Error: Insert, update, and delete ratios must sum to 1.0")
        exit(1)

    generate_transactions_and_operations(
        data_file=args.data_file,
        txs_file=args.txs_file,
        ops_file=args.ops_file,
        num_transactions=args.num_transactions,
        min_cmds_per_tx=args.min_cmds_per_tx,
        max_cmds_per_tx=args.max_cmds_per_tx,
        read_only_ratio=args.read_only_ratio,
        insert_ratio=args.insert_ratio,
        update_ratio=args.update_ratio,
        delete_ratio=args.delete_ratio
    )
