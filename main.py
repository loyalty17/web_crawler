import os
import threading
import tkinter as tk
from concurrent.futures import wait, ThreadPoolExecutor, ALL_COMPLETED
from tkinter import ttk, NSEW, filedialog
from tkinter.messagebox import showerror
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, select, update, func
from sqlalchemy.orm import sessionmaker

from models import Base, CrawlerInfo

WINDOW_SIZE = (800, 600)


class WebCrawler:
    window: tk.Tk
    db_session: sessionmaker
    db_connected = False
    db_url: str
    urls_to_crawl = []
    new_links = set()
    new_links_count = 0
    total_new_links_found = 0
    crawling_mode = True
    crawled_ids = set()
    in_progress_ids = set()

    def initialize_gui(self):
        self.window = tk.Tk()
        self.window.title("Web Crawler")
        self.window.geometry(f"{WINDOW_SIZE[0]}x{WINDOW_SIZE[1]}")

        db_label_frame = ttk.LabelFrame(self.window, text="Database")
        db_label_frame.pack(in_=self.window, fill="x", pady=10, padx=20)
        db_label_frame.columnconfigure((0, 1), weight=1, uniform="first")
        self.current_db_label = ttk.Label(
            db_label_frame, text="CONNECTED TO DB to database: NOT CONNECTED TO DB"
        )
        self.current_db_label.grid(
            column=0, row=0, padx=20, columnspan=2, pady=10, sticky="WE"
        )
        db_action_label = ttk.Label(
            db_label_frame, text="Create new DB or open an existing one:"
        )
        db_action_label.grid(
            column=0, row=1, padx=20, pady=10, columnspan=2, sticky=tk.NW
        )

        self.create_db_btn = ttk.Button(
            db_label_frame, text="Create", command=self.create_db
        )
        self.create_db_btn.grid(column=0, row=2, padx=20, sticky=NSEW)
        self.open_db_btn = ttk.Button(db_label_frame, text="Open", command=self.open_db)
        self.open_db_btn.grid(column=1, row=2, padx=20, sticky=NSEW)

        import_from_directory_label = ttk.Label(
            db_label_frame, text="Import or export data:"
        )
        import_from_directory_label.grid(
            column=0, row=3, padx=20, pady=10, sticky=tk.NW
        )
        self.import_from_directory_btn = ttk.Button(
            db_label_frame,
            text="Import",
            command=self.import_files_from_directory,
            state="disabled",
        )
        self.import_from_directory_btn.grid(column=0, row=4, padx=20, sticky=NSEW)

        self.export_to_file_btn = ttk.Button(
            db_label_frame,
            text="Export to file",
            command=self.export_to_file,
            state="disabled",
        )
        self.export_to_file_btn.grid(column=1, row=4, padx=20, sticky=NSEW)

        web_crawler_frame = ttk.LabelFrame(self.window, text="Crawler")
        web_crawler_frame.pack(in_=self.window, fill="x", pady=10, padx=20)
        web_crawler_frame.columnconfigure((0, 1), weight=1, uniform="first")

        threads_count_label = ttk.Label(web_crawler_frame, text="Threads count:")
        threads_count_label.grid(column=0, row=0, sticky=NSEW, padx=20, pady=10)
        self.threads_count_entry = ttk.Entry(web_crawler_frame)
        self.threads_count_entry.grid(column=0, row=1, sticky=NSEW, padx=20)
        self.start_crawl_btn = ttk.Button(
            web_crawler_frame,
            text="Start",
            command=self.start_crawling,
            state="disabled",
        )
        self.start_crawl_btn.grid(column=0, row=2, padx=20, pady=10, sticky=NSEW)

        self.stop_crawl_btn = ttk.Button(
            web_crawler_frame, text="Stop", command=self.stop_crawling, state="disabled"
        )
        self.stop_crawl_btn.grid(column=1, row=2, padx=20, pady=10, sticky=NSEW)

        statistics_frame = ttk.LabelFrame(self.window, text="Statistics")
        statistics_frame.pack(in_=self.window, fill="x", pady=10, padx=20)
        statistics_frame.columnconfigure((0, 1), weight=1, uniform="first")

        self.current_state = ttk.Label(
            statistics_frame, text="Current state: WAITING FOR DB CONNECTION"
        )
        self.current_state.grid(column=0, row=0, padx=20, pady=10, sticky=NSEW)

        self.current_new_links_count_label = ttk.Label(
            statistics_frame, text="New links waiting to be stored: 0"
        )
        self.current_new_links_count_label.grid(
            column=0, row=1, padx=20, pady=10, sticky=NSEW
        )

        self.total_new_links_count_label = ttk.Label(
            statistics_frame, text="New links found (total): 0"
        )
        self.total_new_links_count_label.grid(
            column=1, row=1, padx=20, pady=10, sticky=NSEW
        )

        self.links_in_db_count_label = ttk.Label(
            statistics_frame, text="Links in DB count: 0"
        )
        self.links_in_db_count_label.grid(
            column=0, row=2, padx=20, pady=10, sticky=NSEW
        )

    def collect_statistics(self):
        if self.db_connected:
            session = self.db_session()
            links_in_db_count = session.execute(
                select(CrawlerInfo).with_only_columns(func.count(CrawlerInfo.id))
            ).scalar()
            session.close()
            self.links_in_db_count_label[
                "text"
            ] = f"Total links in DB count: {links_in_db_count}"
            self.current_new_links_count_label[
                "text"
            ] = f"New links waiting to be stored: {len(self.new_links)}"
            self.total_new_links_count_label[
                "text"
            ] = f"New links found (total): {self.total_new_links_found}"

        self.window.after(3000, self.collect_statistics)

    def import_to_db(self, links_to_import):
        if links_to_import:
            to_insert = [
                CrawlerInfo(link=link, crawled=False) for link in links_to_import
            ]
            session = self.db_session()
            session.bulk_save_objects(to_insert, return_defaults=False)
            session.commit()
            session.close()

    def import_files_from_directory(self):
        self.current_state["text"] = "Current state: IMPORTING"
        self.window.update()
        import_file_paths = filedialog.askopenfilenames(
            title="Select files for import", filetypes=[("Text files", "*.txt")]
        )
        if not import_file_paths:
            return

        for import_file_path in import_file_paths:
            with open(import_file_path, mode="r") as import_file:
                links_to_import = import_file.readlines()
                links_to_import = set(map(lambda x: x.split("\n")[0], links_to_import))
            self.import_to_db(links_to_import)
        self.current_state["text"] = "Current state: CONNECTED TO DB"

    def export_to_file(self):
        self.current_state["text"] = "Current state: EXPORTING"
        self.window.update()
        export_filename = filedialog.asksaveasfilename(
            title="Export unique domains from DB",
            confirmoverwrite=True,
            filetypes=[("Text files", "*.txt")],
        )
        with open(export_filename, mode="w") as export_file:
            session = self.db_session()
            to_export = set()
            total_count = session.execute(
                select(CrawlerInfo).with_only_columns(func.count(CrawlerInfo.id))
            ).scalar()
            offset = 0
            limit = 100_000
            while offset < total_count:
                query = select(CrawlerInfo.link).limit(limit).offset(offset)
                for link in session.scalars(query):
                    netloc = urlparse(link).netloc
                    if netloc:
                        to_export.add(f"{netloc}\n")
                    else:
                        pass
                offset += limit

            export_file.writelines(to_export)

        session.close()
        self.current_state["text"] = "Current state: CONNECTED TO DB"

    def connect_to_db(self, filepath):
        if filepath:
            filepath = (
                filepath.replace("/", "\\\\") if os.name.lower() == "nt" else filepath
            )
            self.db_url = f"sqlite:///{filepath}"
        self.engine = create_engine(self.db_url, echo=False)
        self.db_session = sessionmaker(bind=self.engine)
        self.current_db_label.config(text=f"Connected to database: {self.db_url}")
        self.import_from_directory_btn["state"] = "normal"
        self.export_to_file_btn["state"] = "normal"
        self.start_crawl_btn["state"] = "normal"
        self.current_state["text"] = "Current state: CONNECTED TO DB"
        return True

    def create_db(self):
        filepath = filedialog.asksaveasfilename(
            title="Create new database", defaultextension=".db", confirmoverwrite=True
        )
        if self.connect_to_db(filepath):
            Base.metadata.create_all(self.engine)
            self.db_connected = True
            self.collect_statistics()

    def open_db(self):
        filepath = filedialog.askopenfilename(
            title="Open existing database",
            defaultextension=".db",
            filetypes=[("SQLite3 DB files", "*.db *.sqlite *.sqlite3")],
        )
        self.connect_to_db(filepath)
        self.db_connected = True
        self.collect_statistics()

    def start_crawling(self):
        self.current_state["text"] = "Current state: CRAWLING"
        self.window.update()
        self.crawling_mode = True
        self.stop_crawl_btn["state"] = "normal"
        self.start_crawl_btn["state"] = "disabled"
        self.import_from_directory_btn["state"] = "disabled"
        self.export_to_file_btn["state"] = "disabled"
        self.crawl_thread = threading.Thread(
            target=self.crawler_dispatcher, daemon=True
        )
        self.crawl_thread.start()

    def stop_crawling(self):
        self.current_state["text"] = "Current state: CONNECTED TO DB"
        self.crawling_mode = False
        self.stop_crawl_btn["state"] = "disabled"
        self.start_crawl_btn["state"] = "normal"
        self.import_from_directory_btn["state"] = "enabled"
        self.export_to_file_btn["state"] = "enabled"

    def crawler_dispatcher(self):
        while self.crawling_mode:
            self.mark_urls_as_crawled()
            self.get_urls_to_crawl_from_db()
            self.run_crawlers()
            self.save_new_links_to_db()

        self.current_state["text"] = "Current state: CONNECTED TO DB"

    def mark_urls_as_crawled(self):
        if self.crawled_ids:
            session = self.db_session()
            query = (
                update(CrawlerInfo)
                .where(CrawlerInfo.id.in_(self.crawled_ids.copy()))
                .values(crawled=True)
            )
            session.execute(query)
            session.commit()
            self.crawled_ids.clear()

    def save_new_links_to_db(self):
        self.current_state["text"] = "Current state: WRITING NEW LINKS TO DB"
        self.window.update()
        session = self.db_session()
        session.bulk_save_objects(
            [CrawlerInfo(link=url, crawled=False) for url in self.new_links]
        )
        session.commit()
        session.close()
        self.total_new_links_found += len(self.new_links)
        self.new_links.clear()

    def get_urls_to_crawl_from_db(self):
        self.current_state["text"] = "Current state: GETTING NOT CRAWLED LINKS FROM DB"
        self.window.update()
        session = self.db_session()
        query = select(CrawlerInfo).where(CrawlerInfo.crawled == False).limit(200)
        result = session.scalars(query)
        for crawler_info in result.all():
            self.in_progress_ids.add(crawler_info.id)
            self.urls_to_crawl.append(crawler_info)

    def run_crawlers(self):
        self.current_state["text"] = "Current state: CRAWLING"
        self.window.update()
        try:
            max_threads = int(self.threads_count_entry.get())
        except ValueError:
            self.stop_crawling()
            showerror("Error", "Not valid threads count")
            return

        chunk_max_size = len(self.urls_to_crawl) / max_threads
        chunks = []
        temp = []
        for url in self.urls_to_crawl:
            temp.append(url)
            if len(temp) >= chunk_max_size:
                chunks.append(list(temp))
                temp.clear()
                if len(chunks) == max_threads:
                    break

        if chunks:
            with ThreadPoolExecutor(max_workers=max_threads) as executor:
                futures = {
                    executor.submit(self.extract_links_from_urls, chunk)
                    for chunk in chunks
                }
                wait(futures, return_when=ALL_COMPLETED)
                self.urls_to_crawl.clear()
                if not self.crawling_mode:
                    executor.shutdown(wait=False)
                    return

    def extract_links_from_urls(self, urls):
        for url in urls:
            if not self.crawling_mode:
                return
            try:
                response = requests.get(url.link, timeout=(3.05, 10))
                if response.status_code == 200:
                    soup = BeautifulSoup(
                        response.content, "html.parser", from_encoding="iso-8859-1"
                    )
                    all_links = [a["href"] for a in soup.find_all("a", href=True)]
                    for link in all_links:
                        if "http://" in link or "https://" in link:
                            self.new_links.add(link.strip())
            except requests.RequestException:
                pass
            finally:
                self.crawled_ids.add(url.id)

    def run(self):
        self.window.mainloop()


if __name__ == "__main__":
    crawler = WebCrawler()
    crawler.initialize_gui()
    crawler.run()
