from importlib import resources


def load_municipios():
    with resources.open_text("ibge_tabelas", "municipios.txt") as f:
        municipios = [mun.strip() for mun in f.readlines()]
    return municipios
