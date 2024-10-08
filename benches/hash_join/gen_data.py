import csv
import argparse
import random
import string

def number_to_words(n):
    single_digits = ["zero", "one", "two", "three", "four",
                     "five", "six", "seven", "eight", "nine"]

    if n == 0:
        return "zero"
    else:
        digits = [int(d) for d in str(n)]
        words = [single_digits[d] for d in digits]
        return '-'.join(words)

def generate_key_pool(key_pool_size):
    key_pool = []
    for i in range(key_pool_size):
        key_word = number_to_words(i)
        key_pool.append(key_word)
    return key_pool

def generate_data(output_file, num_records, key_length, pkey_length, value_min_length, value_max_length, key_pool_size):
    # Generate the key pool
    key_pool = generate_key_pool(key_pool_size)

    # Generate pkeys as incrementing numbers, formatted to the specified length
    pkey_counter = 1  # Start from 1

    with open(output_file, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        for _ in range(num_records):
            # Select a key from the key pool
            key = random.choice(key_pool)
            # Ensure the key has the specified length, pad or truncate as necessary
            if len(key) < key_length:
                key = key.ljust(key_length, '_')  # Pad with underscores for readability
            else:
                key = key[:key_length]

            # Generate pkey, filled with underscores
            pkey = str(pkey_counter)
            if len(pkey) < pkey_length:
                pkey = pkey.rjust(pkey_length, '_')  # Pad with underscores
            else:
                pkey = pkey[:pkey_length]
            pkey_counter += 1

            # Generate a value with random length between value_min_length and value_max_length
            value_length = random.randint(value_min_length, value_max_length)
            value = ''.join(random.choices(string.ascii_letters + string.digits, k=value_length))

            writer.writerow([key, pkey, value])

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Generate data for hash join table.',
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        '-o', '--output',
        default='data.csv',
        help='Output data file. (default: data.csv)'
    )
    parser.add_argument(
        '-n', '--num_records',
        type=int,
        default=1000,
        help='Number of records to generate. (default: 1000)'
    )
    parser.add_argument(
        '-k', '--key_length',
        type=int,
        default=16,
        help='Length of keys. (default: 16)'
    )
    parser.add_argument(
        '-p', '--pkey_length',
        type=int,
        default=8,
        help='Length of primary keys (pkeys). (default: 8)'
    )
    parser.add_argument(
        '-vmin', '--value_min_length',
        type=int,
        default=32,
        help='Minimum length of values. (default: 32)'
    )
    parser.add_argument(
        '-vmax', '--value_max_length',
        type=int,
        default=64,
        help='Maximum length of values. (default: 64)'
    )
    parser.add_argument(
        '-s', '--key_pool_size',
        type=int,
        default=10,
        help=(
            'Number of unique keys in the key pool.\n'
            'Defines how many distinct keys will be used in the dataset.\n'
            'A smaller key pool size will result in more duplicate keys.\n'
            '(default: 10)'
        )
    )
    args = parser.parse_args()

    generate_data(
        args.output,
        args.num_records,
        args.key_length,
        args.pkey_length,
        args.value_min_length,
        args.value_max_length,
        args.key_pool_size
    )
