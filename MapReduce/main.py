import functii
import worker
from mpi4py import MPI
# import pydevd_pycharm

comm = MPI.COMM_WORLD
rank = comm.Get_rank()  # aflu rankul procesului curent
no_of_processes = comm.Get_size()  # aflu nr de procese

# port_mapping = [52085, 52096, 52097, 52098,
#                 52099, 52100, 52103, 52104]
# pydevd_pycharm.settrace('localhost', port=port_mapping[rank], stdoutToServer=True, stderrToServer=True)

tag_fisiere = 43
tag_fis_procesate = 23
tag_interval_sf = 19
tag_fis_counted = 77
tag_indexes = 38

path = "./test-files/"
splited_path = "./out/splited/"
counted_path = "./out/counted/"
tag = MPI.ANY_TAG

if __name__ == '__main__':
    no_of_workers = no_of_processes - 1

    if rank == 0:  # master
        functii.clear_folders()
        # verific cate fisiere am si impart la numarul de workeri
        files_names = functii.directory(path)
        no_of_jobs = functii.get_no_of_jobs(files_names, no_of_workers)

        if not files_names:
            print("Nu sunt fisiere de parsat! Iesim!")
            exit(-1)

        # trimit no_of_jobs joburi la cate un worker
        worker_no = 1
        count = 1
        for file in files_names:
            # daca am numarul de joburi si daca nu sunt la ultimul worker
            if count == no_of_jobs and worker_no != no_of_workers:
                worker_no += 1
                count = 1
            req = comm.isend(file, dest=worker_no, tag=tag_fisiere)
            count += 1
            print(f"Eu sunt {rank} si am trimis {file} de spart")

        req.wait()

        req = comm.irecv(source=MPI.ANY_SOURCE, tag=tag_fis_procesate)
        done = req.wait()

        if not done:
            print("Am primit exceptie la mapare!")
            exit(-1)
        elif done:
            functii.master_senders(
                functii.directory(splited_path),
                no_of_workers,
                comm,
                tag_interval_sf
            )

        # astept "done" signal pentru fisiere cuvant_docID_count
        req = comm.irecv(source=MPI.ANY_SOURCE, tag=tag_fis_counted)
        done = req.wait()

        # trimitere indecsi, dar pe cuvinte, nu docID
        if done:
            functii.master_senders(
                functii.directory(counted_path),
                no_of_workers,
                comm,
                tag_indexes
            )

        # primesc raspunsul daca workerul a terminat treaba
        req = comm.irecv(source=MPI.ANY_SOURCE, tag=tag_indexes)
        done = req.wait()
        if done:
            print("MAPREDUCE S-A TERMINAT CU SUCCES!")

    else:  # worker

        # primesc numele fisierului
        req = comm.irecv(source=0, tag=tag_fisiere)
        file = req.wait()
        print(f"Eu sunt {rank} si am primit {file} de spart")
        response = worker.worker_map(file)
        comm.isend(response, dest=0, tag=tag_fis_procesate)

        # primesc intervalul [start, finish]
        req = comm.irecv(source=0, tag=tag_interval_sf)
        interval = req.wait()

        # include test_count
        response = worker.worker_count(interval)
        comm.isend(response, dest=0, tag=tag_fis_counted)

        # asteptare ultimul job
        req = comm.irecv(source=0, tag=tag_indexes)
        interval = req.wait()

        # realizare index invers
        response = worker.worker_inverse_index(interval)
        comm.isend(response, dest=0, tag=tag_indexes)