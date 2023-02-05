import os
from datetime import datetime
from mpi4py import MPI


def clear_folders():
    rmv_splited()
    rmv_counted()
    rmv_inverse_index()


def rmv_splited():
    for f in os.listdir("./out/splited"):
        os.remove(os.path.join("./out/splited", f))
    print("DONE: remove all files from \"splited\"")


def rmv_counted():
    for f in os.listdir("./out/counted"):
        os.remove(os.path.join("./out/counted", f))
    print("DONE: remove all files from \"counted\"")


def rmv_inverse_index():
    for f in os.listdir("./out/inverse_index"):
        os.remove(os.path.join("./out/inverse_index", f))
    print("DONE: remove all files from \"inverse_index\"")


def directory(path):
    return os.listdir(path)


def timestamp():
    return str(int(datetime.timestamp(datetime.now())))


def get_no_of_jobs(files_names, no_of_workers):
    return len(files_names) / no_of_workers


# Prima data impart aproximativ munca egal la toti workerii.
# Pentru a crea intevalele de [start, finish] ce trebuie prelucrate de catre worker,
# trebuie realizat un offset: in cazul in care urmatorul fisier va contine tot un
# cuvant din acelasi document cu cel actual, voi prelungi intervalul pentru a
# prelucra toate cuvintele din acelasi fisier cu acelasi worker.
# Ultimul worker va avea ce a mai ramas de prelucrat
def master_senders(files, no_of_workers, comm, tag):
    l = len(files)
    pas = int(len(files) / no_of_workers)
    current_worker = 1
    i = 0
    while i < (l - 1):
        start = i
        i += pas
        if current_worker == no_of_workers:
            finish = l
            req = comm.isend([start, finish], dest=current_worker, tag=MPI.ANY_TAG)
            break
        next_index_doc_id = files[i + 1].split('_')[0]
        i_doc_id = files[i].split('_')[0]
        while next_index_doc_id == i_doc_id and i < (l - 1):
            next_index_doc_id = files[i + 1].split('_')[0]
            i_doc_id = files[i].split('_')[0]
            i += 1
        finish = i
        req = comm.isend([start, finish], dest=current_worker, tag=tag)
        print(f"Eu sunt 0 si am trimis catre {current_worker} "
              f"intervalul [{start}, {finish}] de prelucrat")
        current_worker += 1
        i += 1
    req.wait()
