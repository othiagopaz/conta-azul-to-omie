import pandas as pd
import os
from src.provider import Provider
from src.mappers import Mappers


def main():
    provider = Provider()
    mappers = Mappers()

    # 1. Create all financial accounts [OK]
    # 2. Register all initial balance for each financial account [OK]
    # 3. Create all persons
    # 4. Cross persons with transactions
    # 5. Create remaining persons without register on item 3

    # cities = provider.get_cities()
    # cities.to_csv("outputs/cidades.csv", index=False)
    cities = pd.read_csv("outputs/cidades.csv", dtype=str)
    persons = pd.read_csv("inputs/pessoas.csv", dtype=str)
    persons = mappers.map_persons(persons, cities)
    persons = provider.create_person_in_batch(persons)
    persons.to_csv("outputs/pessoas.csv", index=False)


if __name__ == "__main__":
    main()
