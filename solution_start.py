import argparse
import pandas as pd
import json
import os


def get_params() -> dict:
    parser = argparse.ArgumentParser(description='DataTest')
    parser.add_argument('--customers_location', required=False, default="./input_datastarter/customers.csv")

    parser.add_argument('--products_location', required=False, default="./input_datastarter/products.csv")
    parser.add_argument('--transactions_location', required=False, default="./input_datastarter/transactions/")

    parser.add_argument('--output_location', required=False, default="./output_data/")
    # parser.add_argument('--aex','-x', required=False, default="arun")
    return vars(parser.parse_args())


def main():
    params = get_params()

    df1 = pd.read_csv(params['customers_location'])

    df2 = pd.read_csv(params['products_location'])

    df3 = read_ransactions(params['transactions_location'])

    df = combine_data(df1, df2, df3)
    convert_to_json(df, params['output_location'])


def read_ransactions(p):
    for root, dirs, files in os.walk(p):
        for file in files:
            if file.endswith(".json"):
                all_json.append(os.path.join(root, file))

    for file in all_json:
        for line in open(file, 'r'):
            blank_list.append(json.loads(line))

    for i in blank_list:

        x = [[i['customer_id'], x['product_id'], x['price']] for x in i['basket']]
        for o in x:
            transactions_data.append(o)

    df = pd.DataFrame(transactions_data, columns=['customer_id', 'product_id', 'price'])
    return df


def combine_data(df1, df2, df3):
    new_df = df1.merge(df3, on='customer_id').merge(df2, on='product_id')

    return new_df


def convert_to_json(df, op_path):
    df.to_json(op_path + 'temp.json', orient='records', lines=True)


if __name__ == "__main__":
    blank_list = []
    transactions_data = []
    all_json = []
    main()
