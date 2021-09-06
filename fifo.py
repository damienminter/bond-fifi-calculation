from typing import List

import pandas as pd
from pandas.core.groupby import DataFrameGroupBy

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


def read_and_group_by_isin(filename: str) -> DataFrameGroupBy:
    # Import Sample Data
    df = pd.read_csv(filename)

    df['EXECUTION_TIMESTAMP'] = pd.to_datetime(df['EXECUTION_TIMESTAMP'])
    df.sort_values(by=['EXECUTION_TIMESTAMP'], inplace=True)

    # delete unwanted cols to save memory
    df.drop(['BOND_NAME', 'AS_OF_DATE', 'ID', 'TRADE_TYPE', 'CURRENCY', 'PRICE', 'AMOUNT'], axis=1, inplace=True)

    # Add addtional columns to datafram
    # df['REMAINDER'] = df['POSITION']
    # df['STATE'] = 'OPEN'
    # df['BUY_TRADE_ID'] = None

    # Convert sell position to negative
    # df['POSITION'] = df.apply(lambda x: x['POSITION'] * -1 if x['EVENT'] == 'SELL' else x['POSITION'], axis=1)

    df_by_isin = df.groupby('ISIN')
    for name, df in df_by_isin:
        del df['ISIN']
        yield name, df


def select_by_event(df: pd.DataFrame, event_name: str, sort_by=None) -> pd.DataFrame:
    if sort_by is None:
        sort_by = ['EXECUTION_TIMESTAMP']
    return df[df['EVENT'] == event_name][['EXECUTION_TIMESTAMP', 'EVENT', 'TRADE_ID', 'POSITION']].sort_values(
        by=sort_by)


def is_buy(transaction) -> bool:
    return transaction['EVENT'] == 'BUY'


def try_match(transaction, transaction_ledger):
    # Return remaining position
    match_eligible_transactions = transaction_ledger[
        (transaction_ledger['EXECUTION_TIMESTAMP'] < transaction['EXECUTION_TIMESTAMP']) & (
                    transaction_ledger['POSITION'] > 0)]
    if match_eligible_transactions.empty:
        print(f"No matching transaction found for {transaction['TRADE_ID']}")
        return transaction['POSITION'], transaction_ledger

    for idx, eligible_txn in match_eligible_transactions.iterrows():
        # Perfect match
        if transaction['POSITION'] == eligible_txn['POSITION']:
            print(f"{transaction['TRADE_ID']} perfectly matched with {transaction_ledger.at[idx, 'TRADE_ID']}")
            transaction_ledger.at[idx, 'POSITION'] = 0
            transaction['POSITION'] = 0
        elif transaction['POSITION'] > eligible_txn['POSITION']:
            transaction['POSITION'] = transaction['POSITION'] - eligible_txn['POSITION']
            transaction_ledger.at[idx, 'POSITION'] = 0
        else:
            transaction['POSITION'] = 0
            transaction_ledger.at[idx, 'POSITION'] =  eligible_txn['POSITION'] - transaction['POSITION']

        if transaction['POSITION'] == 0:
            return 0, transaction_ledger
    return transaction['POSITION'], transaction_ledger


def allocate_trades(filename: str = "bonds_sample_transactions.csv") -> pd.DataFrame:
    for name, df in read_and_group_by_isin(filename):
        # Create list of dictionalries for Buys and Sells
        buys = select_by_event(df, 'BUY')
        sells = select_by_event(df, 'SELL')

        for idx, transaction in df[['EXECUTION_TIMESTAMP', 'EVENT', 'TRADE_ID', 'POSITION']].copy().iterrows():
            print(f"\n\nProcessing {transaction['TRADE_ID']}")
            if is_buy(transaction):
                buys.at[idx, 'POSITION'], sells = try_match(transaction, sells)
            else:
                sells.at[idx,'POSITION'], buys  = try_match(transaction, buys)

            # Display partially/unallocated trades
            print(buys[buys['POSITION'] > 0])
            print(sells[sells['POSITION'] > 0])


if __name__ == '__main__':
    allocated_trades = allocate_trades()
    print(allocated_trades)
    # allocated_trades.to_csv('./output.csv')
