import requests


BASE_URL = "https://servicodados.ibge.gov.br/api/v3/agregados/"


def get_periodos(agregado):
    url = BASE_URL + "{agregado}/periodos".format(agregado=agregado)
    response = requests.get(url)
    return response.json()
