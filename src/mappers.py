import pandas as pd
import uuid


class Mappers:
    def __init__(self):
        pass

    def map_persons(self, persons: pd.DataFrame, cities: pd.DataFrame):
        persons.rename(
            columns={
                "nome": "nome_fantasia",
                "tipo_pessoa": "pessoa_fisica",
                "fone_comercial": "telefone1_numero",
                "cpf_cnpj": "cnpj_cpf",
                "numero": "endereco_numero",
                "tipo": "tags",
                "fl_ativo": "inativo",
            },
            inplace=True,
        )
        persons["pessoa_fisica"] = persons["pessoa_fisica"].apply(
            lambda x: "S" if x == "F" else "N"
        )
        persons["pesquisar_cep"] = persons["cep"].apply(
            lambda x: "S" if pd.notna(x) and x != "" else "N"
        )
        persons["codigo_cliente_integracao"] = persons.apply(
            lambda x: str(uuid.uuid4()), axis=1
        )
        persons["inativo"] = persons["inativo"].apply(lambda x: "S" if not x else "N")
        persons["endereco"] = persons["endereco"].apply(lambda x: str(x)[:60])
        persons["nome_fantasia"] = persons["nome_fantasia"].apply(
            lambda x: str(x)[:100]
        )
        persons["complemento"] = persons["complemento"].apply(
            lambda x: str(x)[0:60] if pd.notna(x) else ""
        )
        persons["razao_social"] = persons.apply(
            lambda row: (
                str(row["nome_fantasia"])[:60]
                if pd.isna(row["razao_social"])
                else str(row["razao_social"])[:60]
            ),
            axis=1,
        )

        persons["cidade"] = persons["cidade"].str.lower()
        cities["cNome"] = cities["cNome"].str.lower()

        persons = persons.merge(
            cities,
            left_on="cidade",
            right_on="cNome",
            how="left",
        )

        persons["cidade"] = persons["nCodIBGE"].where(persons["nCodIBGE"].notna(), "")
        persons["estado"] = persons["cUF"].where(persons["cUF"].notna(), "")

        persons.fillna("", inplace=True)

        persons.drop(
            columns=[
                "fone_celular",
                "inscricao_estadual",
                "isento_icms",
                "id_empresa",
                "cCod",
                "cNome",
                "nCodIBGE",
                "nCodSIAFI",
                "cUF",
            ],
            inplace=True,
        )

        return persons
