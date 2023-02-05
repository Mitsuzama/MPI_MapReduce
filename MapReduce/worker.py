import string

import functii


def worker_map(file):
    try:
        # deschid fisierul si il impart in cuvinte splituite
        bulk = open(file, 'r')
        words = bulk.read().split(' ')
        to_replace = string.punctuation + '\n' + ''
        clean_words = []
        for i in words:
            temp = i
            for p in to_replace:
                temp = temp.replace(p, '')
            clean_words.append(temp)

        # ca id unic preiau timestamp
        ts = functii.timestamp()

        # salvez fiecare cuvant ca fisier separat
        for word in clean_words:
            name = file.split(".txt")[0] + "_" + word + "_" + ts + ".txt"
            path_out = "./out/splited/" + name
            open(path_out, "w")
        return True
    except Exception as e:
        print(e)
        return False


# Functia realizeaza un dictionar de forma {"cuvant" : count} pentru a realiza perechi
# de forma cuvant_docID_count.txt. Creeaza si fisierele cu ajutorul dictionarului.
def worker_count(interval):
    [start, finish] = interval
    files = functii.directory("./out/splited")
    filename = files[start].split("_")[0]
    dict = {}
    i = start
    while i < finish:
        temp = files[i].split('_')[1]
        if temp in dict.keys():
            dict[temp] = dict[temp] + 1
            print("ceva")
        else:
            dict[temp] = 1
        i += 1

    # creez fisierele cu count inclus
    for a in dict:
        final = str(a) + "_" + filename + "_" + str(dict[a]) + ".txt"
        path_out = "./out/counted/" + final
        open(path_out, "w")
    return True


def worker_inverse_index(interval):
    path = "./out/inverse_index/"
    files = functii.directory("./out/counted")
    [start, finish] = interval
    term_last = files[start].split("_")[0]

    i = start
    while i < finish - 1:
        term_actual = files[i + 1].split("_")[0]
        docID_actual = files[i + 1].split("_")[1]
        count_actual = files[i + 1].split("_")[2].split(".txt")[0]
        name = str(term_actual) + "_" + str(docID_actual) + "_" + str(count_actual)

        while term_actual == term_last:
            docID_actual = files[i].split("_")[1]
            count_actual = files[i].split("_")[2].split(".txt")[0]
            if count_actual > 1:
                print(count_actual)
            name += "_" + str(docID_actual) + "_" + str(count_actual)
            i += 1
            [term_last, term_actual] = [term_actual, files[i].split("_")[0]]
        path_out = path + name + ".txt"
        open(path_out, "w")
        term_last = term_actual
        i += 1
        return True
