import subprocess

def download_file(year, semester):
    link = f'https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/dsas/ca/ca-{year}-0{semester}.csv'
    subprocess.run(['wget', link, '--no-check-certificate', '-P', 'datalake/raw'])


for year in range(2004, 2020):
    download_file(year, 1)
    download_file(year, 2)