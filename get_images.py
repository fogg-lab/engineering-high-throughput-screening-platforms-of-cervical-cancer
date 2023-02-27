import os
import os.path as osp
import json
from multiprocessing import Pool as ThreadPool
import urllib3.request
import zipfile
from pathlib import Path

from time import sleep

MAX_PARALLEL_EXTRACTIONS = 3        # Maximum number of archives to extract simultaneously
DOWNLOAD_ATTEMPTS = 3               # Number of times to retry a download if it fails
ABORT_ON_FAILED_DOWNLOAD = False    # Whether to abort the script if a download fails

# Function to extract files and delete archive
def decompress_and_delete(filepath):        
    with zipfile.ZipFile(filepath, "r") as zip_ref:
        zip_ref.extractall(osp.dirname(filepath))
    os.remove(filepath)
    print(f"Extracted images from {osp.basename(filepath)} and deleted archive.")

def main():
    print("This script will download and extract the images from the dataset.")
    print(f"\nDefault output directory: {osp.join(os.getcwd(), 'data')}")

    output_dir = input(f"\nSpecify output directory or leave blank for default: ")
    if output_dir == "":
        output_dir = osp.join(os.getcwd(), 'data')

    # Make sure the parent directory of the output directory exists
    parent_dir = Path(output_dir).parent
    if not parent_dir.exists():
        raise Exception(f"Directory {parent_dir} does not exist.")

    # Create the output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Load the urls to the image archives
    with open("dataset_urls.json", "r") as f:
        urls_by_set = json.load(f)

    print("\nAvailable sets of images:")
    for i, set_name in enumerate(urls_by_set.keys()):
        print(f"{i+1}. {set_name}")

    print("\nNOTE: All image sets combined are ~1TB in size after extraction.")

    # Ask which sets of images to download

    set_indices = input(
        "Enter a comma-separated list of set numbers to download or leave blank to download all sets: ")
    if set_indices == "":
        set_indices = list(range(1, len(urls_by_set) + 1))
    else:
        set_indices = set_indices.replace(" ", "")
        set_indices = [int(i) for i in set_indices.split(",")]

    print("Downloading and extracting the following image sets:")
    for i in set_indices:
        print(f"{i}. {list(urls_by_set.keys())[i-1]}")

    # Iterate over each set of images and download/extract each ZIP file
    for i, (set_name, urls) in enumerate(urls_by_set.items()):
        if i+1 not in set_indices:
            continue

        # Create a thread pool to decompress archives in parallel
        # Downloads will continue one at a time in the main thread
        decompression_thread_pool = ThreadPool(MAX_PARALLEL_EXTRACTIONS)

        # Create a subdirectory for this set of images
        set_dir = os.path.join(output_dir, set_name)
        os.makedirs(set_dir, exist_ok=True)
        http_request_pool = urllib3.PoolManager(num_pools=1)
        for url in urls:
            filename = osp.basename(url)
            filepath = osp.join(output_dir, set_dir, filename)
            print(f"Downloading {filename}...")
            # download and save the file
            for attempt in range(DOWNLOAD_ATTEMPTS):
                try:
                    r = http_request_pool.request('GET', url, preload_content=False)
                    with open(filepath, 'wb') as f:
                        data = r.read(1024)
                        while data:
                            f.write(data)
                            data = r.read(1024)
                    r.release_conn()
                    break
                except Exception as exc:
                    print(f"Download failed for file {filename}: {exc}")
                    if attempt == DOWNLOAD_ATTEMPTS - 1:
                        action = "aborting" if ABORT_ON_FAILED_DOWNLOAD else "skipping"
                        print(f"Download failed after {DOWNLOAD_ATTEMPTS} attempts. {action}.")
                        if ABORT_ON_FAILED_DOWNLOAD:
                            exit(1)
                        continue
                    print(f"Retrying download... ({attempt+1}/{DOWNLOAD_ATTEMPTS})")
                    sleep(3)

            print(f"Extracting {filename}...")
            decompression_thread_pool.apply_async(decompress_and_delete, (filepath,))

        decompression_thread_pool.close()
        decompression_thread_pool.join()

        print(f"Done downloading and extracting {set_name}.")

    print("All images downloaded and extracted successfully.")

if __name__ == "__main__":
    main()
