import csv
import argparse
import random
import string

def generate_data(output_file, num_records):
    with open(output_file, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        for _ in range(num_records):
            key = ''.join(random.choices(string.ascii_letters + string.digits, k=16))
            pkey = ''.join(random.choices(string.ascii_letters + string.digits, k=16))
            value = ''.join(random.choices(string.ascii_letters + string.digits, k=64))
            writer.writerow([key, pkey, value])

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate data for hash join table.')
    parser.add_argument('-o', '--output', default='data.csv', help='Output data file.')
    parser.add_argument('-n', '--num_records', type=int, default=1000, help='Number of records to generate.')
    args = parser.parse_args()

    generate_data(args.output, args.num_records)
