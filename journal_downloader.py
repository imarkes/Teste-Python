NOME_DO_CANDIDATO = 'Ivan Marques de Oliveira Santana'
EMAIL_DO_CANDIDATO = 'i.markes@hotmail.com'

import os
from datetime import datetime as dt
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from time import sleep
from typing import Dict, List, Tuple

import requests

MAIN_FOLDER = Path(__file__).parent.parent


def request_journals(start_date, end_date):
    url = 'https://engine.procedebahia.com.br/publish/api/diaries'

    r = requests.post(url, data={"cod_entity": '50', "start_date": start_date,
                                 "end_date": end_date})
    if r.status_code == 200:
        return r.json()
    elif r.status_code == 400:
        sleep(10)
        return request_journals(start_date, end_date)
    return {}


def download_jornal(edition, path):
    url = 'http://procedebahia.com.br/irece/publicacoes/Diario%20Oficial' \
          '%20-%20PREFEITURA%20MUNICIPAL%20DE%20IRECE%20-%20Ed%20{:04d}.pdf'.format(int(edition))
    r = requests.get(url, allow_redirects=True)
    if r.status_code == 200:
        with open(path, 'wb') as writer:
            writer.write(r.content)
        return edition, path
    return edition, ''


def download_mutiple_jornals(editions, paths):
    threads = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        for edition, path in zip(editions, paths):
            threads.append(executor.submit(download_jornal, edition, path))

        results = []
        for task in as_completed(threads):
            results.append(task.result())

    results = [[r for r in results if r[0] == e][0] for e in editions]
    return [r[1] for r in results]


class JournalDownloader:
    def __init__(self, start_date, end_date):
        self.end_date = end_date
        self.start_date = start_date
        self.pdfs_folder = MAIN_FOLDER / 'pdfs'
        self.jsons_folder = MAIN_FOLDER / 'out'
        self.journal = []
        self.pdfs_folder.mkdir(exist_ok=True)
        self.jsons_folder.mkdir(exist_ok=True)

        self.result = request_journals(self.start_date, self.end_date)
        assert self.start_date > '1970'
        assert self.end_date > '1970'

    def get_day_journals(self, year: int, month: int, day: int) -> List[str]:
        """
        Get all journals of a day, returns a list of JSON paths.
        :param year: 2022
        :param month: 1
        :param day: 6
        :return: ["Pdf_Path:./journals, Number_Edition:1797, Data_Edition:2022-01-04"]
        """

        try:

            number_edition = int(self.result['diaries'][0]['edicao'])
            data_edition = self.result['diaries'][0]['data']

            for editions in self.result['diaries']:
                editions['data'] = dt.strptime(editions['data'], '%Y-%m-%d').date()

                if editions['data'].year == year:
                    self.journal.append(editions)
                elif editions['data'].month == month:
                    self.journal.append(editions)
                elif editions['data'].day == day:
                    self.journal.append(editions)

            pdf_path = download_jornal(number_edition, './journals')

        except ValueError:
            raise 'Incorect start or end date'

        return [f'Pdf_Path:{pdf_path[1]}, Number_Edition:{number_edition},Data_Edition:{data_edition}']

    def get_month_journals(self, year: int, month: int) -> List[str]:
        """
        Get all journals of a month, returns a list of JSON paths.
        :param year: 2022
        :param month: 1
        :return: ["Pdf_Path:./journals, Number_Edition:1797, Data_Edition:2022-01-04"]

        """

        try:

            number_edition = int(self.result['diaries'][0]['edicao'])
            data_edition = self.result['diaries'][0]['data']

            for period in self.result['diaries']:
                period['data'] = dt.strptime(period['data'], '%Y-%m-%d').date()

                if period['data'].year == year:
                    self.journal.append(period)
                elif period['data'].month == month:
                    self.journal.append(period)

            pdf_path = download_jornal(number_edition, './journals')

        except ValueError:
            raise 'Incorect start or end date'

        return [f'Pdf_Path:{pdf_path[1]}, Number_Edition:{number_edition}, Data_Edition:{data_edition}']

    def get_year_journals(self, year: int) -> List[str]:
        """
        Get all journals of a year, returns a list of JSON paths.
        :param year: 2022
        :return: ["Pdf_Path:./journals, Number_Edition:1797, Data_Edition:2022-01-04"]
        """

        try:
            number_edition = int(self.result['diaries'][0]['edicao'])
            data_edition = self.result['diaries'][0]['data']

            for period in self.result['diaries']:
                period['data'] = dt.strptime(period['data'], '%Y-%m-%d').date()

                if period['data'].year == year:
                    self.journal.append(period)

            pdf_path = download_jornal(number_edition, './journals')

        except ValueError:
            raise 'Incorect start or end date'

        return [f'Pdf_Path:{pdf_path[1]}, Number_Edition:{number_edition},Data_Edition:{data_edition}']

    @staticmethod
    def parse(response: Dict) -> List[Tuple[str, str]]:
        """
        Parses the response and returns a tuple list of the date and edition.

        :param response: {'diaries': [{'data': '2022-01-04', 'hora': '17:10:46', 'ano': 'XI', 'edicao': 1797}]}
        :return: [('2022-01-04', '1797')]
        """

        return [(edition['data'], str(edition['edicao'])) for edition in response['diaries']]

    def download_all(self, editions: List[str]) -> List[str]:
        """Download journals and return a list of PDF paths. download in `self.pdfs_folder` folder
          OBS: make the file names ordered. Example: '0.pdf', '1.pdf'...

        :param editions: ['1797']
        :return: ['1.pdfs/journal']
        """

        if not os.path.exists('pdfs'):
            os.makedirs('pdfs')

        os.chdir('pdfs')

        ord = 0
        for pdf in range(len(editions)):
            path = '{}.pdf'.format(ord)
            ord += 1
            name, ext = os.path.basename(path).rsplit('.', 1)

            with open(f"{ord}.{ext}", 'wb') as file:
                pdf = download_mutiple_jornals(editions, file)
                return pdf

    def dump_json(self, pdf_path: str, edition: str, date: str) -> str:

        if pdf_path == '':
            return ''
        output_path = self.jsons_folder / f"{edition}.json"
        data = {
            'path': str(pdf_path),
            'name': str(edition),
            'date': date,
            'origin': 'Irece-BA/DOM'
        }
        with open(output_path, 'w', encoding='utf-8') as file:
            file.write(json.dumps(data,
                                  indent=4, ensure_ascii=False))
        return str(output_path)


if __name__ == '__main__':
    ...
    a = JournalDownloader('2022-01-01', '2022-01-30')
    # a.get_day_journals(2022, 1, 6)
    # a.get_month_journals(2022, 1)
    # a.get_year_journals(2022)
    a.download_all(['1797', '1799'])
    # a.dump_json('*.pdfs/journals', '1797', '2022-01-06')
    # JournalDownloader.parse(request_journals('2022-01-01', '2022-01-10'))
