from src.config import get_config
import requests
import time
import pandas as pd


class Provider:
    def __init__(self):
        pass

    def get_all_persons(self):
        url = get_config()["OMIE_PERSONS_URL"]
        all_persons = []
        pagina = 1
        registros_por_pagina = 500

        while True:
            payload = {
                "app_key": get_config()["OMIE_CLIENT"],
                "app_secret": get_config()["OMIE_SECRET"],
                "call": "ListarClientes",
                "param": {
                    "pagina": pagina,
                    "registros_por_pagina": registros_por_pagina,
                },
            }
            response = requests.post(url, json=payload)
            data = response.json()
            # Omie API returns a list of clients in "clientes_cadastro"
            clientes = data.get("clientes_cadastro", [])
            all_persons.extend(clientes)

            # Check if we have fetched all pages
            total_de_paginas = data.get("total_de_paginas", 1)
            if pagina >= total_de_paginas:
                break
            pagina += 1
            print(f"Fetching page {pagina} of {total_de_paginas}")
            time.sleep(0.2)  # Be nice to the API

        return pd.DataFrame(all_persons, dtype=str)

    def get_all_financial_accounts(self):
        url = get_config()["OMIE_FINANCIAL_URL"]
        all_accounts = []
        pagina = 1
        registros_por_pagina = 500

        while True:
            payload = {
                "app_key": get_config()["OMIE_CLIENT"],
                "app_secret": get_config()["OMIE_SECRET"],
                "call": "ListarContasCorrentes",
                "param": {
                    "pagina": pagina,
                    "registros_por_pagina": registros_por_pagina,
                    "apenas_importado_api": "N",
                },
            }
            response = requests.post(url, json=payload)
            data = response.json()
            contas = data.get("ListarContasCorrentes", [])
            all_accounts.extend(contas)

            total_de_paginas = data.get("total_de_paginas", 1)
            if pagina >= total_de_paginas:
                break
            pagina += 1
            print(f"Fetching financial accounts page {pagina} of {total_de_paginas}")
            time.sleep(0.2)  # Be nice to the API

        return pd.DataFrame(all_accounts, dtype=str)

    def create_person_in_batch(self, persons: pd.DataFrame) -> pd.DataFrame:
        batch_size = 50
        status_list = []

        lote = 0

        for i in range(0, len(persons), batch_size):
            batch = persons.iloc[i : i + batch_size]
            payload = {
                "app_key": get_config()["OMIE_CLIENT"],
                "app_secret": get_config()["OMIE_SECRET"],
                "call": "IncluirClientesPorLote",
                "param": {
                    "clientes_cadastro": batch.to_dict(orient="records"),
                    "lote": str(lote),
                },
            }
            lote += 1
            response = requests.post(get_config()["OMIE_PERSONS_URL"], json=payload)
            try:
                data = response.json()
            except Exception:
                data = {}

            batch_result = batch.copy()
            if data.get("codigo_status") == "0":
                print(f"Batch {lote} of {len(persons)} created successfully")
                print(data)
                batch_result["status"] = "success"
            else:
                print(f"Batch {lote} of {len(persons)} failed")
                print(data)
                batch_result["status"] = data.get("descricao_status")

            yield batch_result

            time.sleep(4)

    def get_cities(self):
        url = get_config()["OMIE_CITIES_URL"]

        all_cities = []
        pagina = 1
        registros_por_pagina = 500

        while True:
            payload = {
                "app_key": get_config()["OMIE_CLIENT"],
                "app_secret": get_config()["OMIE_SECRET"],
                "call": "PesquisarCidades",
                "param": {
                    "pagina": pagina,
                    "registros_por_pagina": registros_por_pagina,
                },
            }
            response = requests.post(url, json=payload)
            data = response.json()
            # Omie API returns a list of clients in "clientes_cadastro"
            cities = data.get("lista_cidades", [])
            all_cities.extend(cities)

            # Check if we have fetched all pages
            total_de_paginas = data.get("total_de_paginas", 1)
            if pagina >= total_de_paginas:
                break
            pagina += 1
            print(f"Fetching page {pagina} of {total_de_paginas}")
            time.sleep(1)  # Be nice to the API

        return pd.DataFrame(all_cities, dtype=str)
