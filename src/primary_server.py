from __future__ import print_function
import socket
import Pyro4
import multiprocessing
import re
import os
import pickle
import os.path
import io
import shutil
from mimetypes import MimeTypes
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload


# ---------------- Utility: text/word handling ----------------

def get_all_words(w):  # split whole text into individual words
    words = []
    delimiters = ", !.?;"
    for line in w.split('\n'):
        words.extend(re.split(f"[{re.escape(delimiters)}]", line))
    # remove empty strings
    words = [a for a in words if a.strip() != '']
    return words


# ---------------- Google Drive helpers ----------------

SCOPES = ['https://www.googleapis.com/auth/drive']


def get_gdrive_service():
    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('./client_secrets.json', SCOPES)
            creds = flow.run_local_server(port=8080)
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('drive', 'v3', credentials=creds)
    return service


def FileDownload(service, file_id, file_name):
    """Download the file from Google Drive directly to the server."""
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request, chunksize=204800)
    done = False

    try:
        while not done:
            done = downloader.next_chunk()

        fh.seek(0)
        with open(file_name, 'wb') as f:
            shutil.copyfileobj(fh, f)

        print("File Downloaded")
        return True
    except Exception as e:
        print("Something went wrong during download:", e)
        return False


# ---------------- Word Count distributed logic ----------------

def send_for_word_count(slave, l, queue):
    """Send list of words to a slave and push result into queue."""
    ans = slave.getMap(l)
    queue.put(ans)


def goInfinite(c, name):
    """Keep serving the same client until they disconnect."""
    while True:
        op = c.recv(1024).decode()
        if not op:
            break
        if op == "1":  # client wants word count
            WordCountFunction(c, name)
        else:          # client wants matrix multiplication
            MatrixMultiplicationFunction(c, name)


def WordCountFunction(c, name):
    service = get_gdrive_service()
    allSlaves = []
    q = multiprocessing.Queue()

    # connect to slaves in parallel
    procs = []
    for n in name:
        try:
            p = multiprocessing.Process(target=ConnectSlave, args=(n, q))
            p.start()
            procs.append(p)
        except Exception:
            continue

    for p in procs:
        p.join()

    while not q.empty():
        allSlaves.append(q.get())

    # receive file id from client
    file_id = c.recv(1024).decode()
    if not file_id:
        return

    local_filename = f"{file_id}.txt"
    FileDownload(service, file_id, local_filename)

    with open(local_filename, encoding='utf8') as f:
        w = f.read()
    os.remove(local_filename)

    words = get_all_words(w)
    n = len(name)
    if n == 0:
        c.send(b"No slaves connected")
        return

    each_words_count = int(len(words) / n)
    st = 0
    i = 0
    result_queue = multiprocessing.Queue()
    processes = []

    while st < len(words) and allSlaves:
        try:
            slave = allSlaves[i]
            if each_words_count == 0:
                en = st
            else:
                en = min(st + each_words_count - 1, len(words) - 1)

            if en >= len(words):
                break

            segment = words[st:en + 1]
            st = en + 1

            p = multiprocessing.Process(
                target=send_for_word_count,
                args=(slave, segment, result_queue)
            )
            p.start()
            processes.append(p)
        except Exception as e:
            print(f"Slave {i + 1}: error", e)

        i = (i + 1) % len(allSlaves)

    for p in processes:
        p.join()

    d = {}
    # merge results from slaves
    while not result_queue.empty():
        s = result_queue.get()
        parts = s.split(" ")
        for y in parts:
            t = y.split(":")
            if len(t) == 2:
                word, count = t[0], t[1]
                if word:
                    d[word] = d.get(word, 0) + int(count)

    s = ""
    for k, v in d.items():
        print(f"{k}: {v}")
        s += f"{k}:{v} "
    c.send(s.encode('utf-8'))


# ---------------- Matrix multiplication distributed logic ----------------

def send_for_matrix(slave, i, m1, matrix, queue):
    """Send a row of matrix1 and whole matrix2 to a slave."""
    ans = slave.matmul(m1, matrix)
    l = ans.split(" ")
    l1 = [i]
    for h in l:
        if h:
            l1.append(int(h))
    queue.put(l1)


def ConnectSlave(name, queue):
    """Connect to a Pyro slave via the nameserver on localhost."""
    try:
        ns = Pyro4.locateNS('127.0.0.1')  # Nameserver should run on localhost:9090
        uri = ns.lookup(name)
        s = Pyro4.Proxy(uri)
        try:
            print(f"{name}: {s.getStatus()}")
            queue.put(s)
        except Exception as e:
            print(f"Error while connecting {name}:", e)
    except Exception as e:
        print(f"Error while connecting {name}:", e)


def MatrixMultiplicationFunction(c, name):
    matrix_data = b''
    allSlaves = []

    matrix_data += c.recv(1024)

    q = multiprocessing.Queue()
    procs = []

    for n in name:
        try:
            p = multiprocessing.Process(target=ConnectSlave, args=(n, q))
            p.start()
            procs.append(p)
        except Exception:
            continue

    for p in procs:
        p.join()

    while not q.empty():
        allSlaves.append(q.get())

    matrix = pickle.loads(matrix_data)
    result_queue = multiprocessing.Queue()

    c.send(b"ack")

    matrix1_data = b''
    matrix1_data += c.recv(1024)
    matrix1 = pickle.loads(matrix1_data)

    processes = []
    n = 0
    i = 0

    while i < len(matrix) and allSlaves:
        try:
            slave = allSlaves[n]
            print(f"Connected with Slave {n + 1}: {slave.getStatus()}")
            p = multiprocessing.Process(
                target=send_for_matrix,
                args=(slave, i, matrix[i], matrix1, result_queue)
            )
            p.start()
            processes.append(p)
            i += 1
        except Exception as e:
            print(f"Error while connecting Slave {n + 1}:", e)
        n = (n + 1) % len(allSlaves)

    for p in processes:
        p.join()

    ans = [[0] * len(matrix1[0]) for _ in range(len(matrix))]
    while not result_queue.empty():
        l = result_queue.get()
        row_index = l[0]
        row_values = l[1:]
        ans[row_index] = row_values

    print(ans)
    msg = pickle.dumps(ans)
    c.send(msg)


# ---------------- Main TCP server ----------------

if __name__ == "__main__":
    # Use the same IP and port that the client connects to
    SERVER_HOST = "10.42.29.144"   # your machine's Wi-Fi IP
    SERVER_PORT = 8000

    s = socket.socket()
    print("Socket created")

    slave_names = ["slave1", "slave2", "slave3"]

    s.bind((SERVER_HOST, SERVER_PORT))
    print(f"IP: {SERVER_HOST}")
    s.listen(1)

    print("Waiting for connection")

    while True:
        c, addr = s.accept()
        print("Connection made with", addr)
        multiprocessing.Process(target=goInfinite, args=(c, slave_names)).start()

    s.close()
