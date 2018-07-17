import requests
import sys

# Author:
# https://stackoverflow.com/a/39225039/5973334


def download_file_from_google_drive(id, destination):
    def get_confirm_token(response):
        for key, value in response.cookies.items():
            if key.startswith('download_warning'):
                return value

        return None

    def save_response_content(response, destination):
        CHUNK_SIZE = 32768

        with open(destination, "wb") as f:
            sys.stdout.write("Downloading...")
            sys.stdout.flush()
            counter = 1
            for chunk in response.iter_content(CHUNK_SIZE):
                counter += 1
                if counter % 100 == 0:
                    sys.stdout.write(".")
                    sys.stdout.flush()
                if chunk:  # filter out keep-alive new chunks
                    f.write(chunk)
            sys.stdout.write(" Done!")
            sys.stdout.flush()

    URL = "https://docs.google.com/uc?export=download"

    session = requests.Session()

    response = session.get(URL, params={'id': id}, stream=True)
    token = get_confirm_token(response)
    print("Confirmation token received...")
    if token:
        params = {'id': id, 'confirm': token}
        response = session.get(URL, params=params, stream=True)

    save_response_content(response, destination)


if __name__ == "__main__":
    import sys
    params = []
    if len(sys.argv) < 2 or len(sys.argv) > 3:
        sys.stderr.write(("Usage: python download.py",
                          "<drive_file_id destination_file_path|file.list>")
                         )
        sys.exit(2)
    if len(sys.argv) is 2:
        with open(sys.argv[1], 'r') as lfile:
            print("Loading list from file")
            for line in lfile:
                if not line.startswith("#"):
                    tokens = filter(None, line.strip().split(' '))
                    params.append(tokens)
    else:
        params.append((sys.argv[1], sys.argv[2]))

    for id, dest in params:
        # TAKE ID FROM SHAREABLE LINK
        file_id = id
        # DESTINATION FILE ON YOUR DISK
        destination = dest
        print("Downloading file {} saving as {}".format(file_id, destination))
        download_file_from_google_drive(file_id, destination)
        print("")
