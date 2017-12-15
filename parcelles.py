import requests
import urllib.request
import urllib.error
import subprocess
import os
import threading
from queue import Queue
from bs4 import BeautifulSoup
from retrying import retry

print_lock = threading.Lock()
url_list = []
errors = []


def list_dept(cadastre_url, ext=""):
    dept_page = requests.get(cadastre_url).text
    dept_soup = BeautifulSoup(dept_page, 'html.parser')
    for idx, dept_node in enumerate(dept_soup.find_all('a')):
        if dept_node.get('href').endswith(ext) and 0 < idx and dept_node.get('href') != "67/":
            list_comm(cadastre_url + dept_node.get('href'))


@retry(wait_fixed=2000, stop_max_attempt_number=5)
def list_comm(comm_url, ext=""):
    comm_page = requests.get(comm_url).text
    comm_soup = BeautifulSoup(comm_page, 'html.parser')
    for idx, comm_node in enumerate(comm_soup.find_all('a')):
        if comm_node.get('href').endswith(ext) and idx > 0:
            if idx < 2:
                list_files(comm_url + comm_node.get('href'), comm_node.get('href'), True)
            else:
                list_files(comm_url + comm_node.get('href'), comm_node.get('href'), False)


def list_files(files_url, comm_name, first):
    comm_name = comm_name.replace("/", "")
    file_name = 'cadastre-' + comm_name + "-parcelles.json.gz"
    url_list.append([files_url + file_name, file_name, first])


list_dept("https://cadastre.data.gouv.fr/data/etalab-cadastre/latest/geojson/communes/")


@retry(wait_fixed=2000, stop_max_attempt_number=5)
def download_file(file_url, file_name, first):
    print('starting ' + file_name)
    file_location = "./Input/" + file_name
    urllib.request.urlretrieve(file_url, file_location)
    json_to_postgis(file_location, first, file_name)


def json_to_postgis(file_location, first, file_name):
    if first:
        bash = 'ogr2ogr -f "PostgreSQL" --config PG_USE_COPY YES PG:"host=192.168.1.101 port=15432 dbname=cadastre ' \
               'user=france password=france" "/vsigzip/' + file_location + \
               '" -sql "SELECT SUBSTR(id, 1, 5) AS commune, SUBSTR(id, 6, 3) AS feuille, SUBSTR(id, 9, 2) AS section' \
               ', numero, contenance, created, updated from OGRGeoJSON"' \
               ' -nln parcelles -nlt GEOMETRY -gt 25000'
    else:
        bash = 'ogr2ogr -f "PostgreSQL" --config PG_USE_COPY YES PG:"host=192.168.1.101 port=15432 dbname=cadastre ' \
               'user=france password=france" "/vsigzip/' + file_location + \
               '" -sql "SELECT SUBSTR(id, 1, 5) AS commune, SUBSTR(id, 6, 3) AS feuille, SUBSTR(id, 9, 2) AS section' \
               ', numero, contenance, created, updated from OGRGeoJSON"' \
               ' -nln parcelles -append -gt 25000'
    proc = subprocess.run(bash, shell=True, timeout=120, check=True)
    if proc.returncode != 0:
        errors.append(file_location.replace("./Input/", ""))
        errors.append(file_name)
        print('error')
    else:
        print('ok')

    os.remove(file_location)
    if os.path.isfile(file_location + '.properties'):
        os.remove(file_location + '.properties')


def process_queue():
    while True:
        current_url = url_queue.get()
        download_file(current_url[0], current_url[1], current_url[2])
        url_queue.task_done()


url_queue = Queue()
for i in range(12):
    t = threading.Thread(target=process_queue)
    t.daemon = True
    t.start()

for idx, current_url in enumerate(url_list):
    if idx < 1:
        download_file(current_url[0], current_url[1], current_url[2])
    else:
        url_queue.put(current_url)

url_queue.join()

if errors:
    f = open("./Error/cadastre_parcelles.txt", "w")
    for error in errors:
        f.write(error + '\n')
    del f
