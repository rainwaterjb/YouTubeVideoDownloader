import csv
import yt_dlp
import os
import subprocess
import datetime
import re
import tkinter as tk
from tkinter import filedialog, ttk, messagebox
import shutil
import threading
import queue
import glob
from concurrent.futures import ThreadPoolExecutor


class DownloadTask:
    def __init__(self, url, title, save_path, speaker, sermon_series, year, index):
        self.url = url
        self.title = title
        self.save_path = save_path
        self.speaker = speaker
        self.sermon_series = sermon_series
        self.year = year
        self.index = index
        self.process = None
        self.paused = False
        self.terminated = False
        self.progress = 0
        self.eta = "N/A"
        self.status = "Pending"
        self.filename = None
        self.partial_file = None  # Track partial download file
        self.downloaded_bytes = 0  # Track downloaded bytes for resuming


class VideoDownloaderApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Video Downloader")
        self.root.geometry("1000x700")
        self.download_tasks = []
        self.max_concurrent_downloads = 10  # Updated to 10 simultaneous downloads
        self.executor = ThreadPoolExecutor(max_workers=self.max_concurrent_downloads)
        self.queue = queue.Queue()
        self.download_path = ""
        self.csv_file = ""
        self.running = True
        self.tree_items = {}  # Maps task index to Treeview item ID
        self.active_slots = {}  # Maps slot to current task

        # GUI Elements
        self.create_gui()

        # Check for ffmpeg
        if not shutil.which("ffmpeg"):
            messagebox.showerror("Error", "ffmpeg is not installed or not found in PATH.\nPlease install ffmpeg.")
            self.root.quit()

        # Periodically update GUI
        self.update_gui()

    def create_gui(self):
        # CSV and Directory Selection
        self.select_frame = ttk.Frame(self.root)
        self.select_frame.pack(pady=10, padx=10, fill="x")

        ttk.Button(self.select_frame, text="Select CSV File", command=self.select_csv).pack(side="left", padx=5)
        self.csv_label = ttk.Label(self.select_frame, text="No CSV selected")
        self.csv_label.pack(side="left", padx=5)

        ttk.Button(self.select_frame, text="Select Download Directory", command=self.select_directory).pack(side="left",
                                                                                                            padx=5)
        self.dir_label = ttk.Label(self.select_frame, text="No directory selected")
        self.dir_label.pack(side="left", padx=5)

        # Download and Global Control Buttons
        self.control_frame = ttk.Frame(self.root)
        self.control_frame.pack(pady=5)

        self.start_button = ttk.Button(self.control_frame, text="Start Downloads", command=self.start_downloads,
                                       state="disabled")
        self.start_button.pack(side="left", padx=5)

        self.pause_all_button = ttk.Button(self.control_frame, text="Pause All", command=self.pause_all,
                                           state="disabled")
        self.pause_all_button.pack(side="left", padx=5)

        self.resume_all_button = ttk.Button(self.control_frame, text="Resume All", command=self.resume_all,
                                            state="disabled")
        self.resume_all_button.pack(side="left", padx=5)

        self.cancel_all_button = ttk.Button(self.control_frame, text="Cancel All", command=self.cancel_all,
                                            state="disabled")
        self.cancel_all_button.pack(side="left", padx=5)

        # CSV Content Table (Scrollable)
        self.table_frame = ttk.Frame(self.root)
        self.table_frame.pack(pady=10, padx=10, fill="both", expand=True)

        # Create a frame for the Treeview and scrollbar
        self.tree_container = ttk.Frame(self.table_frame)
        self.tree_container.pack(fill="both", expand=True)

        # Scrollbar
        self.scrollbar = ttk.Scrollbar(self.tree_container, orient="vertical")
        self.scrollbar.pack(side="right", fill="y")

        # Treeview for CSV content
        self.tree = ttk.Treeview(
            self.tree_container,
            columns=("Title", "Speaker", "Sermon Series", "Date", "Status"),
            show="headings",
            yscrollcommand=self.scrollbar.set
        )
        self.tree.heading("Title", text="Title")
        self.tree.heading("Speaker", text="Speaker")
        self.tree.heading("Sermon Series", text="Sermon Series")
        self.tree.heading("Date", text="Date")
        self.tree.heading("Status", text="Status")
        self.tree.column("Title", width=350, anchor="w")
        self.tree.column("Speaker", width=150, anchor="w")
        self.tree.column("Sermon Series", width=200, anchor="w")
        self.tree.column("Date", width=100, anchor="center")
        self.tree.column("Status", width=150, anchor="center")
        self.tree.pack(fill="both", expand=True)

        # Configure scrollbar
        self.scrollbar.configure(command=self.tree.yview)

        # Configure mouse wheel scrolling
        self.tree.bind("<MouseWheel>", lambda e: self.tree.yview_scroll(int(-1 * (e.delta / 120)), "units"))

        # Download Task List (Non-scrollable)
        self.task_frame = ttk.Frame(self.root)
        self.task_frame.pack(pady=10, padx=10, fill="x")

        self.task_labels = []
        self.progress_bars = []
        self.status_labels = []
        self.control_buttons = []

        # Create placeholders for up to 10 simultaneous downloads
        for i in range(self.max_concurrent_downloads):
            frame = ttk.Frame(self.task_frame)
            frame.pack(fill="x", pady=5)

            label = ttk.Label(frame, text="No task", width=50)
            label.pack(side="left", padx=5)
            self.task_labels.append(label)

            progress = ttk.Progressbar(frame, length=100, mode="determinate")
            progress.pack(side="left", padx=5)
            self.progress_bars.append(progress)

            status = ttk.Label(frame, text="Idle", width=10)
            status.pack(side="left", padx=5)
            self.status_labels.append(status)

            pause_btn = ttk.Button(frame, text="Pause", command=lambda x=i: self.toggle_pause(x), state="disabled")
            pause_btn.pack(side="left", padx=5)
            cancel_btn = ttk.Button(frame, text="Cancel", command=lambda x=i: self.terminate_download(x),
                                    state="disabled")
            cancel_btn.pack(side="left", padx=5)
            self.control_buttons.append((pause_btn, cancel_btn))

    def select_csv(self):
        self.csv_file = filedialog.askopenfilename(
            title="Select CSV File",
            filetypes=[("CSV Files", "*.csv"), ("All Files", "*.*")]
        )
        if self.csv_file:
            self.csv_label.config(text=os.path.basename(self.csv_file))
            self.load_csv_to_table()
            self.check_start_button()

    def select_directory(self):
        self.download_path = filedialog.askdirectory(
            title="Select Download Directory",
            mustexist=True
        )
        if self.download_path:
            self.dir_label.config(text=os.path.basename(self.download_path))
            self.check_start_button()

    def check_start_button(self):
        if self.csv_file and self.download_path:
            self.start_button.config(state="normal")
        else:
            self.start_button.config(state="disabled")

    def load_csv_to_table(self):
        # Clear existing items
        for item in self.tree.get_children():
            self.tree.delete(item)
        self.tree_items.clear()

        try:
            with open(self.csv_file, newline='', encoding='utf-8-sig') as file:
                reader = csv.DictReader(file)
                required_columns = ['Title - Verses', 'Date', 'Speaker', 'Sermon Series', 'Sermon URL']
                missing_columns = [col for col in required_columns if col not in reader.fieldnames]
                if missing_columns:
                    messagebox.showerror("Error", f"CSV missing columns: {', '.join(missing_columns)}")
                    return

                for idx, row in enumerate(reader):
                    date = row['Date']
                    status = "Pending"
                    item_id = self.tree.insert("", "end", values=(
                        row['Title - Verses'],
                        row['Speaker'],
                        row['Sermon Series'],
                        date,
                        status
                    ))
                    self.tree_items[idx] = item_id
        except Exception as e:
            messagebox.showerror("Error", f"Failed to load CSV: {e}")

    def start_downloads(self):
        try:
            # Validate CSV and create tasks
            with open(self.csv_file, newline='', encoding='utf-8-sig') as file:
                reader = csv.DictReader(file)
                self.download_tasks = []
                for idx, row in enumerate(reader):
                    try:
                        year = datetime.datetime.strptime(row['Date'], '%Y-%m-%d').year
                    except ValueError:
                        year = ""
                    task = DownloadTask(
                        url=row['Sermon URL'],
                        title=row['Title - Verses'],
                        save_path=self.download_path,
                        speaker=row['Speaker'],
                        sermon_series=row['Sermon Series'],
                        year=year,
                        index=idx
                    )
                    self.download_tasks.append(task)

                # Start processing tasks
                self.start_button.config(state="disabled")
                self.pause_all_button.config(state="normal")
                self.resume_all_button.config(state="normal")
                self.cancel_all_button.config(state="normal")
                threading.Thread(target=self.process_tasks, daemon=True).start()

        except Exception as e:
            messagebox.showerror("Error", f"Unexpected error: {e}")

    def process_tasks(self):
        task_index = 0

        while task_index < len(self.download_tasks) and self.running:
            # Find an available slot
            available_slot = None
            for slot in range(self.max_concurrent_downloads):
                if slot not in self.active_slots or self.active_slots[slot].status in ["Completed", "Terminated",
                                                                                       "Error"]:
                    available_slot = slot
                    break

            if available_slot is not None and task_index < len(self.download_tasks):
                task = self.download_tasks[task_index]
                self.active_slots[available_slot] = task
                self.update_task_gui(available_slot, task)
                self.executor.submit(self.download_video, task, available_slot)
                task_index += 1
            else:
                try:
                    self.queue.get_nowait()
                except queue.Empty:
                    self.root.after(100, lambda: None)  # Yield control

        # Wait for all tasks to complete
        while any(slot in self.active_slots and self.active_slots[slot].status in ["Downloading", "Paused",
                                                                                   "Adding Metadata"] for slot in
                  range(self.max_concurrent_downloads)) and self.running:
            try:
                self.queue.get_nowait()
            except queue.Empty:
                self.root.after(100, lambda: None)  # Yield control

        self.start_button.config(state="normal")
        self.pause_all_button.config(state="disabled")
        self.resume_all_button.config(state="disabled")
        self.cancel_all_button.config(state="disabled")

    def update_task_gui(self, slot, task):
        self.task_labels[slot].config(text=f"{task.title} ({task.progress:.1f}%, ETA: {task.eta})")
        self.status_labels[slot].config(text=task.status)
        self.progress_bars[slot].config(value=task.progress)
        pause_btn, cancel_btn = self.control_buttons[slot]
        pause_btn.config(state="normal")
        cancel_btn.config(state="normal")
        if task.index in self.tree_items:
            self.tree.set(self.tree_items[task.index], "Status", task.status)

    def clear_task_gui(self, slot):
        self.task_labels[slot].config(text="No task")
        self.status_labels[slot].config(text="Idle")
        self.progress_bars[slot].config(value=0)
        pause_btn, cancel_btn = self.control_buttons[slot]
        pause_btn.config(state="disabled")
        cancel_btn.config(state="disabled")
        if slot in self.active_slots:
            del self.active_slots[slot]

    def pause_all(self):
        for slot in range(self.max_concurrent_downloads):
            if slot in self.active_slots:
                task = self.active_slots[slot]
                if task.status == "Downloading":
                    task.paused = True
                    task.status = "Paused"
                    task.process = None  # Signal to stop current download
                    self.status_labels[slot].config(text=task.status)
                    self.task_labels[slot].config(text=f"{task.title} ({task.progress:.1f}%, ETA: {task.eta})")
                    self.control_buttons[slot][0].config(text="Resume")
                    if task.index in self.tree_items:
                        self.tree.set(self.tree_items[task.index], "Status", task.status)

    def resume_all(self):
        for slot in range(self.max_concurrent_downloads):
            if slot in self.active_slots:
                task = self.active_slots[slot]
                if task.status == "Paused":
                    task.paused = False
                    task.status = "Downloading"
                    self.status_labels[slot].config(text=task.status)
                    self.task_labels[slot].config(text=f"{task.title} ({task.progress:.1f}%, ETA: {task.eta})")
                    self.control_buttons[slot][0].config(text="Pause")
                    if task.index in self.tree_items:
                        self.tree.set(self.tree_items[task.index], "Status", task.status)
                    self.executor.submit(self.download_video, task, slot)

    def cancel_all(self):
        for slot in range(self.max_concurrent_downloads):
            if slot in self.active_slots:
                task = self.active_slots[slot]
                if task.status in ["Downloading", "Paused"]:
                    task.terminated = True
                    task.status = "Terminated"
                    self.status_labels[slot].config(text=task.status)
                    if task.index in self.tree_items:
                        self.tree.set(self.tree_items[task.index], "Status", task.status)
                    # Clean up partial file
                    if task.partial_file and os.path.exists(task.partial_file):
                        os.remove(task.partial_file)
                    self.clear_task_gui(slot)
                    self.queue.put("done")

    def toggle_pause(self, slot):
        if slot in self.active_slots:
            task = self.active_slots[slot]
            if task.status in ["Downloading", "Paused"]:
                task.paused = not task.paused
                task.status = "Paused" if task.paused else "Downloading"
                self.status_labels[slot].config(text=task.status)
                self.task_labels[slot].config(text=f"{task.title} ({task.progress:.1f}%, ETA: {task.eta})")
                self.control_buttons[slot][0].config(text="Resume" if task.paused else "Pause")
                if task.index in self.tree_items:
                    self.tree.set(self.tree_items[task.index], "Status", task.status)
                if task.paused:
                    task.process = None  # Signal to stop current download
                else:
                    # Resume by restarting download in a new thread
                    self.executor.submit(self.download_video, task, slot)

    def terminate_download(self, slot):
        if slot in self.active_slots:
            task = self.active_slots[slot]
            if task.status in ["Downloading", "Paused"]:
                task.terminated = True
                task.status = "Terminated"
                self.status_labels[slot].config(text=task.status)
                if task.index in self.tree_items:
                    self.tree.set(self.tree_items[task.index], "Status", task.status)
                # Clean up partial file
                if task.partial_file and os.path.exists(task.partial_file):
                    os.remove(task.partial_file)
                self.clear_task_gui(slot)
                self.queue.put("done")

    def sanitize_filename(self, title):
        invalid_chars = r'[<>:"/\\|?*]'
        safe_title = re.sub(invalid_chars, '_', title)
        safe_title = re.sub(r'_+', '_', safe_title)
        return safe_title.strip('_ ').strip()

    def download_video(self, task, slot):
        try:
            task.status = "Downloading"
            self.status_labels[slot].config(text=task.status)
            if task.index in self.tree_items:
                self.tree.set(self.tree_items[task.index], "Status", task.status)
            safe_title = self.sanitize_filename(task.title)
            expected_file = os.path.join(task.save_path, f"{safe_title}.mp4")

            if os.path.exists(expected_file):
                task.status = "Completed (Exists)"
                self.status_labels[slot].config(text=task.status)
                if task.index in self.tree_items:
                    self.tree.set(self.tree_items[task.index], "Status", task.status)
                self.queue.put("done")
                return

            ydl_opts = {
                'outtmpl': f'{task.save_path}/{safe_title}.%(ext)s',
                'format': 'bestvideo+bestaudio/best',
                'merge_output_format': 'mp4',
                'progress_hooks': [lambda d: self.progress_hook(d, task, slot)],
                'continuedl': True,  # Enable resuming partial downloads
            }

            while task.status == "Downloading" and not task.terminated:
                try:
                    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                        task.process = ydl
                        info = ydl.extract_info(task.url, download=True)
                        task.filename = ydl.prepare_filename(info)
                        task.partial_file = None  # Clear partial file on completion
                        if not task.terminated:
                            task.status = "Adding Metadata"
                            self.status_labels[slot].config(text=task.status)
                            if task.index in self.tree_items:
                                self.tree.set(self.tree_items[task.index], "Status", task.status)
                            self.add_metadata(task.filename, task.title, task.speaker, task.sermon_series, task.year)
                            task.status = "Completed"
                            self.status_labels[slot].config(text=task.status)
                            if task.index in self.tree_items:
                                self.tree.set(self.tree_items[task.index], "Status", task.status)
                        break
                except yt_dlp.utils.DownloadError as e:
                    if task.paused:
                        task.partial_file = f"{task.save_path}/{safe_title}.mp4.part"  # Track partial file
                        break
                    elif task.terminated:
                        break
                    else:
                        raise e

        except Exception as e:
            if not task.terminated:
                task.status = f"Error: {str(e)}"
                self.status_labels[slot].config(text=task.status)
                if task.index in self.tree_items:
                    self.tree.set(self.tree_items[task.index], "Status", task.status)
        finally:
            if not task.terminated and task.status not in ["Paused"]:
                self.queue.put("done")
            if task.status != "Paused":
                self.clear_task_gui(slot)

    def progress_hook(self, d, task, slot):
        if task.terminated:
            raise yt_dlp.utils.DownloadError("Download terminated by user")
        if task.paused:
            task.downloaded_bytes = d.get('downloaded_bytes', task.downloaded_bytes)
            raise yt_dlp.utils.DownloadError("Download paused by user")
        if d['status'] == 'downloading':
            if 'downloaded_bytes' in d and 'total_bytes' in d:
                task.downloaded_bytes = d['downloaded_bytes']
                task.progress = (task.downloaded_bytes / d['total_bytes']) * 100
                self.progress_bars[slot].config(value=task.progress)
                task.eta = d.get('eta', 'N/A')
                if isinstance(task.eta, (int, float)):
                    task.eta = f"{int(task.eta)}s"
                self.task_labels[slot].config(text=f"{task.title} ({task.progress:.1f}%, ETA: {task.eta})")
        elif d['status'] == 'finished':
            task.progress = 100
            task.eta = "0s"
            self.progress_bars[slot].config(value=task.progress)
            self.task_labels[slot].config(text=f"{task.title} ({task.progress:.1f}%, ETA: {task.eta})")

    def add_metadata(self, video_path, title, speaker, sermon_series, year):
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
            subprocess.run(cmd, check=True, capture_output=True, text=True, timeout=300)
            if not os.path.exists(output_path):
                raise FileNotFoundError(f"Temporary file {output_path} was not created.")
            os.remove(video_path)
            os.rename(output_path, video_path)
        except Exception as e:
            print(f"Error updating metadata for {video_path}: {e}")
            if os.path.exists(output_path):
                os.remove(output_path)

    def update_gui(self):
        if self.running:
            self.root.after(100, self.update_gui)

    def on_closing(self):
        self.running = False
        for task in self.download_tasks:
            task.terminated = True
        self.executor.shutdown(wait=True)
        # Clean up temporary files
        for temp_file in glob.glob(os.path.join(self.download_path, "*_meta.mp4")):
            try:
                os.remove(temp_file)
            except Exception:
                pass
        for task in self.download_tasks:
            if task.partial_file and os.path.exists(task.partial_file):
                try:
                    os.remove(task.partial_file)
                except Exception:
                    pass
        self.root.destroy()


if __name__ == "__main__":
    root = tk.Tk()
    app = VideoDownloaderApp(root)
    root.protocol("WM_DELETE_WINDOW", app.on_closing)
    root.mainloop()