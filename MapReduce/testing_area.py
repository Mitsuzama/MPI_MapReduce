import os
import string
from datetime import datetime


def rmv():
    for f in os.listdir("./out/splited"):
        os.remove(os.path.join("./out/splited", f))
    print("DONE: remove all files from \"splited\"")
    for f in os.listdir("./out/counted"):
        os.remove(os.path.join("./out/counted", f))
    print("DONE: remove all files from \"counted\"")
    for f in os.listdir("./out/inverse_index"):
        os.remove(os.path.join("./out/inverse_index", f))
    print("DONE: remove all files from \"inverse_index\"")


def test_senders(files, no_of_workers):
    l = len(files)
    pas = int(len(files) / no_of_workers)
    current_worker = 1
    # print("L:", l)
    i = 0
    while i < (l - 1):
        start = i
        i += pas
        if current_worker == no_of_workers:
            finish = l
            break
        next_index_doc_id = files[i + 1].split('_')[0]
        i_doc_id = files[i].split('_')[0]
        while next_index_doc_id == i_doc_id and i < (l - 1):
            next_index_doc_id = files[i + 1].split('_')[0]
            i_doc_id = files[i].split('_')[0]
            i += 1
        finish = i
        print(f"Eu sunt 0 si am trimis catre {current_worker} "
              f"intervalul [{start}, {finish}] de prelucrat")
        current_worker += 1
        i += 1


def test_map(file):
    # deschid fisierul si il impart in cuvinte splituite
    bulk = open("./test-files/" + file, 'r')
    words = bulk.read().split(' ')
    to_replace = string.punctuation + '\n'
    clean_words = []
    for i in words:
        temp = i
        for p in to_replace:
            temp = temp.replace(p, '')
        clean_words.append(temp.lower())

    # ca id unic preiau timestamp
    ts = str(int(datetime.timestamp(datetime.now())))

    # salvez fiecare cuvant ca fisier separat
    for word in clean_words:
        name = file.split(".txt")[0] + "_" + word + "_" + ts + ".txt"
        path_out = "./out/splited/" + name
        open(path_out, "w")
    print(f"{file}: done")


def test_count(interval):
    files = os.listdir("./out/splited")
    filename = files[0].split("_")[0]
    [start, finish] = interval
    dict = {}
    i = start
    while i < finish-1:
        temp = files[i].split('_')[1]
        if temp in dict.keys():
            dict[temp] = dict[temp] + 1
        else:
            dict[temp] = 1
        i += 1

    # creez fisierele cu count inclus
    for a in dict:
        final = str(a) + "_" + filename + "_" + str(dict[a]) + ".txt"
        path_out = "./out/counted/" + final
        open(path_out, "w")


def test_index(interval):
    path = "./out/inverse_index/"
    files = os.listdir("./out/counted")
    [start, finish] = interval
    term_last = files[0].split("_")[0]

    i = start
    while i < finish - 1:
        term_actual = files[i + 1].split("_")[0]
        docID_actual = files[i + 1].split("_")[1]
        count_actual = files[i + 1].split("_")[2].split(".txt")[0]
        name = str(term_actual) + "_" + str(docID_actual) + "_" + str(count_actual)

        while term_actual == term_last:
            docID_actual = files[i].split("_")[1]
            count_actual = files[i].split("_")[2].split(".txt")[0]
            # if int(count_actual) > 1:
            #     print(count_actual)
            name += "_" + str(docID_actual) + "_" + str(count_actual)
            i += 1
            [term_last, term_actual] = [term_actual, files[i].split("_")[0]]
        path_out = path + name + ".txt"
        open(path_out, "w")
        term_last = term_actual
        i += 1


if __name__ == '__main__':
    rmv()
    # files_names = os.listdir("./test-files")
    #
    # test_map(files_names[0])
    # test_map(files_names[1])
    #
    # splited = os.listdir("./out/splited")
    # test_senders(splited, 8)
    #
    # test_count([0, 9367])
    # test_senders(os.listdir("./out/counted/"), 8)
    #
    # test_index([0, 2542])
