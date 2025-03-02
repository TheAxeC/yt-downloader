from tqdm.auto import tqdm, trange  # type: ignore
import yaml, ssl, os, argparse, re
from yt_dlp import YoutubeDL, postprocessor

DATA_FILE = 'data.yml'
FORMAT_VIDEO = 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best'
FORMAT_AUDIO = 'bestaudio[ext=m4a]/bestaudio'

DATA_PATH = "data"
if os.name == 'posix':
    DEFAULT_PATH, NAS_PATH = "~/Downloads/youtube", "/Volumes/media/youtube"
    COOKIES = ""
    import unicodedata
else:
    DEFAULT_PATH, NAS_PATH = "X:\\youtube", "X:\\youtube"
    COOKIES = ""
    
OUTTMPL_DEFAULT = '%(title)s.%(ext)s'
OUTTMPL_CHANNEL = '[%(uploader)s] - '
OUTTMPL_COUNT = '%(playlist_index)s - '

BASE_OPTIONS = {
    'allow_playlist_files': False,
    'ignoreerrors': 'only_download',
    'no_warnings': True,
    'retries': 10,
    'quiet': True,
    'noprogress': True,
    'abort_on_unavailable_fragments': True,
}

STATS_OPTIONS = {
    'extract_flat': 'in_playlist',
}

DOWNLOAD_OPTIONS = {
    'postprocessors': [
        {'actions': [(postprocessor.metadataparser.MetadataParserPP.interpretter,
                                  'description',
                                  '(?s)(?P<meta_comment>.+)')],
                     'key': 'MetadataParser',
                     'when': 'pre_process'},
        {'already_have_subtitle': False, 'key': 'FFmpegEmbedSubtitle'},
        {'add_chapters': True, 'add_infojson': 'if_exists', 'add_metadata': True, 'key': 'FFmpegMetadata'},
        {'already_have_thumbnail': False, 'key': 'EmbedThumbnail'},
        {'key': 'FFmpegConcat', 'only_multi_video': True, 'when': 'playlist'}
    ],
    'fragment_retries': 10,
    'subtitleslangs': ['en', 'a.en'],
    'writesubtitles': True,
    'writethumbnail': True,
    'format': FORMAT_VIDEO,
    'ratelimit': 5000000,
    'sleep_interval_requests': 2,
    'sleep_interval': 20,    
    'max_sleep_interval': 120,
    'sleep_interval_subtitles': 2,
}

class TQDMLogger:
    def __init__(self, pbar):
        self.pbar = pbar
    def debug(self, msg):
        pass
    def info(self, msg):
        pass
    def warning(self, msg):
        pass
    def error(self, msg):
        self.pbar.write(f"ERROR: {msg}")

def safe_filename(s: str, max_length: int = 255) -> str:
    """Sanitize a string making it safe to use as a filename."""
    characters = [r'"', r"\*", r"\.", r"\/", r"\:", r'"', r"\<", r"\>", r"\?", r"\\", r"\|", r"\\\\"]
    regex = re.compile("|".join([chr(i) for i in range(31)] + characters), re.UNICODE)
    filename = regex.sub("", s)
    return filename[:max_length].rsplit(" ", 0)[0]

class Stats:
    def __init__(self):
        self.stats = {}
        self.special_files = None

    def add_missing(self, missing):
        self._add_key('missing', missing)

    def add_data_missing(self, downloaded):
        self._add_key('data_missing', downloaded)
    
    def add_skipped(self, skipped):
        pass

    def add_failed(self, failed):
        self._add_key('failed', failed, discard=True)
    
    def add_submitted(self, submitted):
        self._add_key('submitted', submitted, discard=True)

    def calculate_globals(self, pbar):
        self.stats['global'] = {}
        for key in ['submitted', 'data_missing', 'missing', 'skipped', 'failed']:
            total = sum([elem[key] for elem in self.stats.values() if key in elem])
            if total > 0: self.stats['global'][key] = total
        if 'submitted' in self.stats['global']:
            pbar.write(f"Submitted {self.stats['global']['submitted']} videos in total")
        if self.special_files:
            with open("special.yml", 'w') as file:
                yaml.dump(self.special_files, file)
        with open("stats.yml", 'w') as file:
            yaml.dump(self.stats, file)

    def add_special_files(self, special_files):
        self.special_files = special_files

    def add_category(self, category, value):
        self.stats[category] = value.stats
        if value.special_files:
            if not self.special_files: self.special_files = {}
            self.special_files[category] = value.special_files
    
    def _add_key(self, key, value, discard=False):
        if key not in self.stats: self.stats[key] = 0
        self.stats[key] += 1
        if discard: return
        if key+"_file" not in self.stats: self.stats[key+"_file"] = []
        self.stats[key+"_file"].append(value)

    def output(self, pbar):
        if 'submitted' in self.stats: pbar.write(f"    Submitted {self.stats['submitted']} videos")
        if 'data_missing' in self.stats: pbar.write(f"    Data missing {self.stats['data_missing']} videos")
        if 'missing' in self.stats: pbar.write(f"    Missing {self.stats['missing']} videos")

class PlaylistData:
    def __init__(self, name):
        self.name = name
        self._load()
    
    def _load(self):
        self.playlist_data = {'downloaded' : set([]), 'info' : {}}
        playlist_data_file = os.path.join(DATA_PATH, self.name + '.yml')
        if os.path.exists(playlist_data_file):
            with open(playlist_data_file, 'r') as file:
                self.playlist_data = yaml.safe_load(file)

    @property
    def archive(self):
        return os.path.join(DATA_PATH, self.name + '.txt')
    
    def safe(self):
        playlist_data_file = os.path.join(DATA_PATH, self.name + '.yml')
        playlist_data_archive = os.path.join(DATA_PATH, self.name + '.txt')
        with open(playlist_data_file, 'w') as file:
            yaml.dump(self.playlist_data, file)
        with open(playlist_data_archive, 'w') as file:
            for url in self.playlist_data['downloaded']:
                file.write("youtube " + url.replace("https://youtube.com/watch?v=", "") + "\n")
    
    @property
    def info(self):
        return self.playlist_data['info']

    @property
    def downloaded(self):
        return self.playlist_data['downloaded']

    def add(self, result):
        """Add the result to the data file"""
        self.playlist_data['downloaded'].add(result['url'])
        self.playlist_data['info'][result['url']] = {
            'title': result['title'],
            'location': result['location'],
            'file': result['file']
        }

class ItemDownloader:
    def __init__(self, item, pbar, path):
        self.opts = BASE_OPTIONS.copy()
        self.name = item['name']
        if 'channel' in item and item['channel']: item['url'] = item['url'] + '/videos'
        self.url = item['url']
        self.item = item
        self.pbar = pbar
        self.playlist_data = PlaylistData(self.name)
        self.location = os.path.join(item['location'], item['name']) if 'location' in item else item['name']
        self.outputdir = os.path.join(path, self.location)
        self._set_formatting(item, pbar)
        self.stats = Stats()
    
    def finalize(self):
        self.stats.output(self.pbar)
        self.playlist_data.safe()
        return self.stats

    def _set_formatting(self, item, pbar):
        self.opts['logger'] = TQDMLogger(pbar)
        if 'mp3' in item and item['mp3']: self.opts['format'] = FORMAT_AUDIO
        outtmpl = self.outputdir
        if 'add_channel' in item and item['add_channel']: outtmpl = outtmpl + OUTTMPL_CHANNEL
        if 'count' in item and item['count']: outtmpl = outtmpl + OUTTMPL_COUNT
        self.opts['outtmpl'] = outtmpl + OUTTMPL_DEFAULT

    def _check_special_files(self):
        self.existing_files = []
        if os.path.exists(self.outputdir):
            self.existing_files = [name for name in os.listdir(self.outputdir) if os.path.isfile(os.path.join(self.outputdir, name)) and (name.endswith('.mp4') or name.endswith(".m4a"))]
            if os.name == 'posix': self.existing_files = [unicodedata.normalize('NFC', name) for name in self.existing_files]
            filesnames = [elem['file'] for elem in self.playlist_data.info.values()]
            if os.name == 'posix': filesnames = [unicodedata.normalize('NFC', name) for name in filesnames]
            self.stats.add_special_files([file for file in self.existing_files if file not in filesnames])
        if 'other' in self.playlist_data.playlist_data:
            self.pbar.write(f"    Other files: {len(self.playlist_data.playlist_data['other'])}")

    def _download_video(self, ydl, url, title):
        """Download the video"""
        pbar_video = trange(100, leave=False, desc='Starting', ascii=True)
        def tqdm_hook(d):
            if d['status'] == 'downloading':
                if 'total_bytes' not in d: curr_prog = d['fragment_index'] / d['fragment_count']
                elif d['total_bytes'] is None: curr_prog = 0
                else: curr_prog = d['downloaded_bytes'] / d['total_bytes']
                pbar_video.update(curr_prog - pbar_video.n)
            elif d['status'] == 'error':
                self.pbar.write(f"    Error downloading: {d['filename']} with error {d['error']}")
            elif d['status'] == 'finished':
                self.pbar.update(1)
        ydl.add_progress_hook(tqdm_hook)
        result = ydl.download([url])
        if result == 0:
            self.playlist_data.add({
                'url': url,
                'title': title,
                'location': self.location,
                'file': existing_file
            })
            self.playlist_data.add(result)
        else: 
            self.stats.add_failed(result)
        pbar_video.close()

    def _check_stats(self, url, title, item):
        safe_chars = {'/': '', ':': '', '*': '', '"': '_', '<': '', '>': '', '|': '', '?': ''}
        existing_file = next((f for f in self.existing_files if safe_filename(title.translate(str.maketrans(safe_chars))) in f), None)
        url = url.replace("https://www.", "https://")
        in_playlist = url in self.playlist_data.downloaded
        record = item.copy()
        record['url'] = url
        record['title'] = title
        record.pop('channel', None)
        if not existing_file and not in_playlist: self.stats.add_submitted(record)
        if existing_file and not in_playlist:
            self.stats.add_data_missing(record)
            self.playlist_data.add({
                'url': url,
                'title': title,
                'location': self.location,
                'file': existing_file
            })
        if in_playlist and not existing_file:
            item = self.playlist_data.info[url]['file']
            if os.name == 'posix': item = unicodedata.normalize('NFC', item)
            filesnames = [elem['file'] for elem in self.playlist_data.info.values()]
            if os.name == 'posix': filesnames = [unicodedata.normalize('NFC', name) for name in filesnames]
            filename = os.path.join(self.outputdir, item)
            if not os.path.exists(filename) and item not in filesnames:
                self.stats.add_missing(record)
        if existing_file and in_playlist: self.stats.add_skipped(record)

    def progress(self, download=False, stat_checker=False):
        if download: 
            self.opts.update(DOWNLOAD_OPTIONS)
            self.opts['download_archive'] = self.playlist_data.archive
        if stat_checker: 
            self.opts.update(STATS_OPTIONS)
            self._check_special_files()
        with YoutubeDL(self.opts) as ydl:
            info = ydl.sanitize_info(ydl.extract_info(self.url, download=False))
            pbar_playlist = trange(100, leave=False, desc="Start", ascii=True)
            self.pbar.write(f"Downloading {self.name} with {info['playlist_count']} videos")
            for entry in info['entries']:
                if entry['view_count'] is None: continue
                if stat_checker: self._check_stats(entry['url'], entry['title'], self.item)
                if download: self._download_video(ydl, entry['url'])
                pbar_playlist.update(1)
            pbar_playlist.close()

def downloader(data_file, path, download, stats):
    """Download the videos from the data file"""
    ssl._create_default_https_context = ssl._create_unverified_context
    with open(data_file, 'r') as file:
        data = yaml.safe_load(file)
        print(f"Downloading {len(data)} channels or playlists")
    pbar = tqdm(data, desc='Total', ascii=True)
    stats = Stats()
    for item in pbar:
        item_downloader = ItemDownloader(item, pbar, path)
        item_downloader.progress(download=download, stat_checker=stats)
        stats.add_category(item['name'], item_downloader.finalize())
    stats.calculate_globals(pbar)
    pbar.close()

if __name__ == '__main__': 
    parser=argparse.ArgumentParser(description="Youtube Downloader", allow_abbrev=False)
    parser.add_argument("--nas", help="Whether to use the NAS storage", default=False, action='store_true')
    parser.add_argument("--data", help="The data file to use", default=DATA_FILE)
    parser.add_argument("--path", help="The path to download to", default=DEFAULT_PATH)
    parser.add_argument("--stats", help="Create a list of files to be downloaded", default=False, action='store_true')
    parser.add_argument("--download", help="Download the files", default=False, action='store_true')
    args=parser.parse_args()
    path = NAS_PATH if args.nas else args.path
    # downloader(args.data, path, download=args.download, stats=args.stats)
    downloader(args.data, path, download=False, stats=args.stats)

    # TODO
    # desc = "Title: "+yt.title+"\nAuthor: "+author+"\nPublished: "+str(yt.publish_date)+"\nTags: "+", ".join(yt.keywords)+"\nAge Restricted: "+str(yt.age_restricted)
    # desc = desc + "\nSubtitles: " + str(subtitles) + "\nThumbnail: " + str(thumbnail) + "\nChapters: " + str(chapters)+"\nDescription: "+description