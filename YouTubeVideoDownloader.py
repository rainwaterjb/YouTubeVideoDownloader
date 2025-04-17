import csv
import yt_dlp
import os
import subprocess
import datetime
import re
import tkinter as tk
from tkinter import filedialog
import shutil

def get_csv_file():
    root = tk.Tk()
    root.withdraw()
    print("Please select your CSV file...")
    file_path = filedialog.askopenfilename(
        title="Select CSV File",
        filetypes=[("CSV Files", "*.csv"), ("All Files", "*.*")]
    )
    root.destroy()
    if not file_path:
        raise ValueError("No file selected. Exiting.")
    if not os.path.isfile(file_path):
        raise ValueError(f"Selected file does not exist: {file_path}")
    return file_path

def get_download_directory():
    root = tk.Tk()
    root.withdraw()
    print("Please select the directory to save MP4 files...")
    dir_path = filedialog.askdirectory(
        title="Select Download Directory",
        mustexist=True
    )
    root.destroy()
    if not dir_path:
        raise ValueError("No directory selected. Exiting.")
    if not os.path.isdir(dir_path):
        raise ValueError(f"Selected directory does not exist: {dir_path}")
    return dir_path

def sanitize_filename(title):
    invalid_chars = r'[<>:"/\\|?*]'
    safe_title = re.sub(invalid_chars, '_', title)
    safe_title = re.sub(r'_+', '_', safe_title)
    safe_title = safe_title.strip('_ ').strip()
    return safe_title

def download_video(url, save_path, title):
    try:
        safe_title = sanitize_filename(title)
        expected_file = os.path.join(save_path, f"{safe_title}.mp4")
        if os.path.exists(expected_file):
            print(f"File already exists, skipping download: {expected_file}")
            return expected_file
        ydl_opts = {
            'outtmpl': f'{save_path}/{safe_title}.%(ext)s',
            'format': 'bestvideo+bestaudio/best',
            'merge_output_format': 'mp4',
        }
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)
            filename = ydl.prepare_filename(info)
            print(f"Downloaded: {url}")
            return filename
    except Exception as e:
        print(f"Error downloading {url}: {e}")
        return None

def add_metadata(video_path, title, speaker, sermon_series, year):
    try:
        output_path = video_path.replace(".mp4", "_meta.mp4")
        cmd = [
            "ffmpeg", "-i", video_path, "-c", "copy",
            "-metadata", f"title={title}",
            "-metadata", f"artist={speaker}",
            "-metadata", f"album={sermon_series}",
            "-metadata", f"date={year}",
            output_path
        ]
        # Run ffmpeg with a timeout to prevent hanging
        result = subprocess.run(cmd, check=True, capture_output=True, text=True, timeout=300)
        print(f"ffmpeg output: {result.stdout}")
        if not os.path.exists(output_path):
            raise FileNotFoundError(f"Temporary file {output_path} was not created.")
        # Verify write permissions for original file
        if not os.access(video_path, os.W_OK):
            raise PermissionError(f"No write permission for {video_path}")
        os.remove(video_path)
        os.rename(output_path, video_path)
        print(f"Updated metadata for: {video_path}")
    except subprocess.TimeoutExpired:
        print(f"Error: ffmpeg timed out while processing {video_path}")
        if os.path.exists(output_path):
            os.remove(output_path)  # Clean up partial file
    except PermissionError as e:
        print(f"Permission error: {e}")
    except FileNotFoundError as e:
        print(f"File error: {e}")
    except Exception as e:
        print(f"Error updating metadata for {video_path}: {e}")
        if os.path.exists(output_path):
            os.remove(output_path)  # Clean up partial file

# Check for ffmpeg
if not shutil.which("ffmpeg"):
    print("Error: ffmpeg is not installed or not found in PATH.")
    print("Please install ffmpeg:")
    print("  On macOS: `brew install ffmpeg`")
    print("  On Windows: Download from https://ffmpeg.org/download.html")
    print("  On Linux: `sudo apt-get install ffmpeg` (Ubuntu) or equivalent")
    exit(1)

try:
    # Get CSV file
    csv_file = get_csv_file()

    # Get download directory
    download_path = get_download_directory()

    # Ensure download directory exists
    os.makedirs(download_path, exist_ok=True)

    # Validate CSV columns
    required_columns = ['Title - Verses', 'Date', 'Speaker', 'Sermon Series', 'Sermon URL']
    with open(csv_file, newline='', encoding='utf-8-sig') as file:
        reader = csv.DictReader(file)
        missing_columns = [col for col in required_columns if col not in reader.fieldnames]
        if missing_columns:
            print(f"Error: CSV file is missing required columns: {', '.join(missing_columns)}")
            print(f"Detected columns: {', '.join(reader.fieldnames)}")
            exit(1)

    # Read and process the CSV file
    with open(csv_file, newline='', encoding='utf-8-sig') as file:
        reader = csv.DictReader(file)
        for row in reader:
            video_url = row['Sermon URL']
            title = row['Title - Verses']
            speaker = row['Speaker']
            sermon_series = row['Sermon Series']
            try:
                year = datetime.datetime.strptime(row['Date'], '%Y-%m-%d').year
            except ValueError:
                year = ""
                print(f"Warning: Invalid date format for {title}, skipping year metadata")

            video_path = download_video(video_url, download_path, title)
            if video_path and os.path.exists(video_path):
                add_metadata(video_path, title, speaker, sermon_series, year)
            else:
                print(f"Skipping metadata for {title} due to download failure")

except KeyboardInterrupt:
    print("\nInterrupted by user. Cleaning up...")
    # Clean up any temporary _meta.mp4 files
    for temp_file in glob.glob(os.path.join(download_path, "*_meta.mp4")):
        try:
            os.remove(temp_file)
            print(f"Removed temporary file: {temp_file}")
        except Exception as e:
            print(f"Error removing {temp_file}: {e}")
    exit(1)
except ValueError as e:
    print(e)
    exit(1)
except Exception as e:
    print(f"Unexpected error: {e}")
    exit(1)