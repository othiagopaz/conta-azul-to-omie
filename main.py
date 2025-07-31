import pandas as pd
import os
from src.provider import Provider
from src.mappers import Mappers


def main():
    provider = Provider()
    mappers = Mappers()

    # cities = provider.get_cities()
    # cities.to_csv("outputs/cidades.csv", index=False)
    # cities = pd.read_csv("outputs/cidades.csv", dtype=str)

    # persons = pd.read_csv("inputs/pessoas.csv", dtype=str)
    # persons = mappers.map_persons(persons, cities)
    # persons_errors = provider.create_persons_in_batch(persons)
    # persons_errors.to_csv("outputs/pessoas_errors.csv", index=False)
    # persons.to_csv("outputs/pessoas.csv", index=False)
    persons = pd.read_csv("outputs/pessoas.csv", dtype=str)

    # financial_accounts = provider.get_all_financial_accounts()
    # financial_accounts.to_csv("outputs/financial_accounts.csv", index=False)
    financial_accounts = pd.read_csv("outputs/financial_accounts.csv", dtype=str)

    # categories = provider.get_all_categories()
    # categories.to_csv("outputs/categories.csv", index=False)
    categories = pd.read_csv("outputs/categories.csv", dtype=str)

    financial_transactions = pd.read_csv("inputs/financeiro.csv", dtype=str)
    financial_transactions = mappers.map_financial_transactions(
        financial_transactions, categories, persons, financial_accounts
    )

    financial_transactions.to_csv("outputs/financial_transactions.csv", index=False)


if __name__ == "__main__":
    main()
