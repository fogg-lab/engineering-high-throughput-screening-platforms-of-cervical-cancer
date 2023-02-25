import os
import os.path as osp
import json
import urllib.request
import zipfile
from pathlib import Path

# Prompt the user for the output directory
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

# Load the JSON file
with open("dataset_urls.json", "r") as f:
    data = json.load(f)

# Ask the user which sets of images to download (or all of them)
print("\nAvailable sets of images:")
for i, set_name in enumerate(data["image_urls_by_set"].keys()):
    print(f"{i+1}. {set_name}")

print("\nNOTE: All image sets combined are ~1TB in size after extraction.")

set_indices = input(
    "Enter a comma-separated list of set numbers to download or leave blank to download all sets: ")
if set_indices == "":
    set_indices = list(range(1, len(data["image_urls_by_set"]) + 1))
else:
    set_indices = set_indices.replace(" ", "")
    set_indices = [int(i) for i in set_indices.split(",")]

print("Downloading and extracting the following image sets:")
for i in set_indices:
    print(f"{i}. {list(data['image_urls_by_set'].keys())[i-1]}")

# Iterate over each set of images and download/extract them
for i, (set_name, urls) in enumerate(data["image_urls_by_set"].items()):
    if i+1 not in set_indices:
        continue

    # Create a subdirectory for this set of images
    set_dir = os.path.join(output_dir, set_name)
    os.makedirs(set_dir, exist_ok=True)

    # Download and extract each ZIP file
    for url in urls:
        filename = osp.basename(url)
        filepath = osp.join(output_dir, set_dir, filename)
        print(f"Downloading {url} to {filepath}...")

        urllib.request.urlretrieve(url, filepath)

        print(f"Extracting {filepath}...")
        with zipfile.ZipFile(filepath, "r") as zip_ref:
            zip_ref.extractall(set_dir)

        # Delete the ZIP file after extraction
        os.remove(filepath)

print("All images downloaded and extracted successfully.")
