from tqdm.auto import tqdm, trange # type: ignore
import yaml, ssl, os, argparse, re, shutil # type: ignore
from yt_dlp import YoutubeDL, postprocessor # type: ignore

DATA_FILE = 'data.yml'
SPECIAL_FILE = 'special.yml'
STATS_FILE = 'stats.yml'
FORMAT_VIDEO = 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best'
FORMAT_AUDIO = 'bestaudio[ext=m4a]/bestaudio'

DATA_PATH = "data"

if os.name == 'posix':
    DEFAULT_PATH, NAS_PATH = "~/Downloads/youtube", "/Volumes/media/youtube"
    TMP_DIR = "~/Downloads/_tmp/"
    import unicodedata
else:
    DEFAULT_PATH, NAS_PATH = "X:\\youtube", "X:\\youtube"
    TMP_DIR = os.path.join(os.environ['USERPROFILE'], "Downloads\\_tmp")
    
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

VIDEO_OPTIONS = {
    'postprocessors': [
        {'actions': [(postprocessor.metadataparser.MetadataParserPP.interpretter,
                                  'description',
                                  '(?s)(?P<meta_comment>.+)')],
                     'key': 'MetadataParser',
                     'when': 'pre_process'},
        {'add_chapters': True, 'add_infojson': 'if_exists', 'add_metadata': True, 'key': 'FFmpegMetadata'},
        {'already_have_thumbnail': False, 'key': 'EmbedThumbnail'},
        {'already_have_subtitle': False, 'key': 'FFmpegEmbedSubtitle'},
        {'key': 'FFmpegConcat', 'only_multi_video': True, 'when': 'playlist'}
    ],
    'subtitleslangs': ['en', 'a.en'],
    'writesubtitles': True,
    'writethumbnail': True,
}

AUDIO_OPTIONS = {
    'postprocessors': [
        {'actions': [(postprocessor.metadataparser.MetadataParserPP.interpretter,
                                  'description',
                                  '(?s)(?P<meta_comment>.+)')],
                     'key': 'MetadataParser',
                     'when': 'pre_process'},
        {'add_chapters': True, 'add_infojson': 'if_exists', 'add_metadata': True, 'key': 'FFmpegMetadata'},
    ],
}

DOWNLOAD_OPTIONS = {
    'fragment_retries': 10,
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
        self.pbar.write(f"    {msg}")

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
        self._add_key('skipped', skipped, discard=True)

    def add_failed(self, failed):
        self._add_key('failed', failed, discard=True)
    
    def add_submitted(self, submitted):
        self._add_key('submitted', submitted, discard=True)

    def calculate_globals(self, pbar, special_file, stats_file, console, file_output):
        self.stats['global'] = {}
        for key in ['submitted', 'data_missing', 'missing', 'failed']:
            total = sum([elem[key] for elem in self.stats.values() if key in elem])
            if total > 0: self.stats['global'][key] = total
        if 'submitted' in self.stats['global'] and console:
            pbar.write(f"Submitted {self.stats['global']['submitted']} videos in total")
        if not file_output: return
        if self.special_files:
            with open(special_file, 'w') as file:
                yaml.dump(self.special_files, file)
        with open(stats_file, 'w') as file:
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

    def has_submitted(self):
        return 'submitted' in self.stats

    def get_skipped(self):
        if 'skipped' in self.stats: return self.stats['skipped']
        return 0

    def output(self, pbar, console):
        if not console: return
        if 'submitted' in self.stats: pbar.write(f"    Submitted {self.stats['submitted']} videos")
        if 'data_missing' in self.stats: pbar.write(f"    Data missing {self.stats['data_missing']} videos")
        if 'missing' in self.stats: pbar.write(f"    Missing {self.stats['missing']} videos")
        if 'failed' in self.stats: pbar.write(f"    Failed {self.stats['failed']} videos")

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
    
    def save(self, archive=True):
        playlist_data_file = os.path.join(DATA_PATH, self.name + '.yml')
        with open(playlist_data_file, 'w') as file:
            yaml.dump(self.playlist_data, file)
        if not archive: return
        playlist_data_archive = os.path.join(DATA_PATH, self.name + '.txt')
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
        self.tempdir = TMP_DIR
        self._set_formatting(item, pbar)
        self.stats = Stats()
    
    def finalize(self, update=False, console=True, file_output=True):
        self.stats.output(self.pbar, console=console)
        if update and file_output: self.playlist_data.save()
        return self.stats

    def _set_formatting(self, item, pbar):
        self.opts['logger'] = TQDMLogger(pbar)
        if 'mp3' in item and item['mp3']: 
            self.opts['format'] = FORMAT_AUDIO
            self.opts.update(AUDIO_OPTIONS)
        else:
            self.opts['format'] = FORMAT_VIDEO
            self.opts.update(VIDEO_OPTIONS)
        self.opts['paths'] = {
            'home' : self.outputdir + '/',
            'temp' : self.tempdir + '/'
        }
        outtmpl = ''
        if 'add_channel' in item and item['add_channel']: outtmpl = outtmpl + OUTTMPL_CHANNEL
        if 'count' in item and item['count']: outtmpl = outtmpl + OUTTMPL_COUNT
        self.opts['outtmpl'] = outtmpl + OUTTMPL_DEFAULT

    def _check_special_files(self, console):
        self.existing_files = []
        if os.path.exists(self.outputdir):
            self.existing_files = [name for name in os.listdir(self.outputdir) if os.path.isfile(os.path.join(self.outputdir, name)) and (name.endswith('.mp4') or name.endswith(".m4a"))]
            if os.name == 'posix': self.existing_files = [unicodedata.normalize('NFC', name) for name in self.existing_files]
            filesnames = [elem['file'] for elem in self.playlist_data.info.values()]
            if os.name == 'posix': filesnames = [unicodedata.normalize('NFC', name) for name in filesnames]
            self.stats.add_special_files([file for file in self.existing_files if file not in filesnames])
        if 'other' in self.playlist_data.playlist_data and console:
            self.pbar.write(f"    Other files: {len(self.playlist_data.playlist_data['other'])}")

    def _download_video(self, file_output=True, console=True):
        """Download the video"""
        download_opts = self.opts.copy()
        download_opts.update(DOWNLOAD_OPTIONS)
        download_opts['download_archive'] = self.playlist_data.archive
        info_dict = {}
        with YoutubeDL(download_opts) as ydl:
            info = ydl.sanitize_info(ydl.extract_info(self.url, download=False))
            if console: self.pbar.write(f"Downloading {self.name} with {info['playlist_count']} videos...")
            pbar_playlist = trange(info['playlist_count'], leave=False, desc=self.name, ascii=True, miniters=1)
            pbar_video = trange(100, leave=False, desc='Starting', ascii=True)
            pbar_playlist.update(self.stats.get_skipped())
            pbar_playlist.refresh()
            def tqdm_hook(d):
                nonlocal info_dict
                nonlocal pbar_video
                nonlocal pbar_playlist
                nonlocal console
                if d['status'] == 'downloading':
                    pbar_video.set_description(f"Downloading {os.path.basename(d['filename'])}")
                    if 'total_bytes' not in d: curr_prog = d['fragment_index'] / d['fragment_count']
                    elif d['total_bytes'] is None: curr_prog = 0
                    else: curr_prog = d['downloaded_bytes'] / d['total_bytes']
                    pbar_video.update(int(curr_prog*100) - pbar_video.n)
                elif d['status'] == 'error':
                    if console: self.pbar.write(f"    Error: {os.path.basename(d['filename'])} with error {d['error']}")
                    self.stats.add_failed({})
                    pbar_playlist.update(1)
                elif d['status'] == 'finished':
                    pbar_video.set_description(f"Finished {os.path.basename(d['filename'])}")
                    info_dict = d['info_dict']
            def tqdm_hook_post(d):
                nonlocal pbar_video
                nonlocal info_dict
                if d['status'] == 'started':
                    pbar_video.set_description(f"Postprocessing {d['postprocessor']} for {d['info_dict']['title']}")
                elif d['status'] == 'finished':
                    pbar_video.set_description(f"Finished {d['postprocessor']} for {d['info_dict']['title']}")
                    info_dict = d['info_dict']
            def post_hook(filename):
                pbar_video.set_description(f"Finished {info_dict['title']}")
                nonlocal pbar_playlist
                nonlocal file_output
                result = {
                    'url': info_dict['webpage_url'].replace("https://www.", "https://"),
                    'title': info_dict['title'],
                    'location': self.location,
                    'file': filename
                }
                pbar_playlist.update(1)
                self.playlist_data.add(result)
                if file_output: self.playlist_data.save(archive=False)
            ydl.add_progress_hook(tqdm_hook)
            ydl.add_postprocessor_hook(tqdm_hook_post)
            ydl.add_post_hook(post_hook)
            ydl.download([self.url])
        pbar_video.close()
        pbar_playlist.close()

    def _check_stats(self, url, title, item, console=True, update=False):
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
            if console: self.pbar.write(f"    \"{title}\" already exists but not in playlist")
            self.stats.add_data_missing(record)
            if not update: return
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
                if console: self.pbar.write(f"    \"{title}\" missing from playlist")
                self.stats.add_missing(record)
            else: self.stats.add_skipped(record)
        if existing_file and in_playlist: self.stats.add_skipped(record)

    def progress(self, download=False, stat_checker=False, update=False, console=True):
        if stat_checker: 
            stat_opts = self.opts.copy()
            stat_opts.update(STATS_OPTIONS)
            self._check_special_files(console=console)
            with YoutubeDL(stat_opts) as ydl:
                info = ydl.sanitize_info(ydl.extract_info(self.url, download=False))
                pbar_playlist = trange(info['playlist_count'], leave=False, desc=self.name, ascii=True, miniters=1)
                if console: self.pbar.write(f"Checking stats of {self.name} with {info['playlist_count']} videos")
                for entry in info['entries']:
                    if entry['view_count'] is not None: 
                        self._check_stats(entry['url'], entry['title'], self.item, console=console, update=update)
                    pbar_playlist.update(1)
                    pbar_playlist.refresh()
                pbar_playlist.close()
            if not self.stats.has_submitted(): return
        if download: self._download_video(file_output=file_output, console=console)

def downloader(data_file, path, download, check_stats, update, special_file, stats_file, console, file_output):
    """Download the videos from the data file"""
    if not download and not check_stats: 
        if console: print("No action specified")
        return
    ssl._create_default_https_context = ssl._create_unverified_context
    with open(data_file, 'r') as file:
        data = yaml.safe_load(file)
        if console: 
            print(f"Downloading {len(data)} channels or playlists")
            print(f"=============================================")
    pbar = tqdm(data, desc='Total', leave=False, ascii=True)
    stats = Stats()
    for item in pbar:
        item_downloader = ItemDownloader(item, pbar, path)
        item_downloader.progress(download=download, stat_checker=check_stats, update=update, console=console)
        stats.add_category(item['name'], item_downloader.finalize(update=True if download else update, console=console, file_output=file_output))
    if check_stats: stats.calculate_globals(pbar, special_file, stats_file, console, file_output)
    shutil.rmtree(TMP_DIR, ignore_errors=True)
    pbar.close()
    if console: print(f"=============================================")

if __name__ == '__main__': 
    usage = """Youtube Downloader

    This script is used to download videos from youtube. It uses the yt-dlp library to download the videos.
    The script can be used to download videos from channels or playlists. The data file is a yaml file that
    contains the information about the channels or playlists to download. The script can also be used to
    check the stats of the videos in the channels or playlists. 

    To download files, you can use:
        python3 ytdlp.py -sd
    
    To check the stats of the files without changing any files, you can use:
        python3 ytdlp.py -sf

    A PO-token is required to download the videos. 
        # Replace `~` with `$USERPROFILE` if using Windows
        cd ~
        # Replace 0.7.3 with the latest version or the one that matches the plugin
        git clone --single-branch --branch 0.7.3 https://github.com/Brainicism/bgutil-ytdlp-pot-provider.git
        cd bgutil-ytdlp-pot-provider/server/
        yarn install --frozen-lockfile
        npx tsc
        python3 -m pip install -U bgutil-ytdlp-pot-provider"""
    parser=argparse.ArgumentParser(prog='Builder',
        description=usage,
        allow_abbrev=False,
        formatter_class=argparse.RawTextHelpFormatter,
        epilog='Used for development of youtube download scripts.')
    parser.add_argument("-e", "--nas", "--external", help="Whether to use the NAS storage", default=False, action='store_true')
    parser.add_argument("-s", "--stats", help="Create a list of files to be downloaded", default=False, action='store_true')
    parser.add_argument("-u", "--update", help="Update the data files (automatically True if downloading, still required if you want to update missing elements)", default=False, action='store_true')
    parser.add_argument("-d", "--download", help="Download the files", default=False, action='store_true')

    subparsers = parser.add_argument_group(title='File Output',
        description='Set the files to output to')
    subparsers.add_argument("-p", "--path", help="The path to download to", default=DEFAULT_PATH)
    subparsers.add_argument("-i", "--data", "--input", help="The data file to use", default=DATA_FILE)
    subparsers.add_argument("-m", "--missing", help="The special/missing file to use (for existing files that don't appear in the playlist/channel anymore)", default=SPECIAL_FILE)
    subparsers.add_argument("-o", "--output", help="The stats file to use", default=STATS_FILE)

    subparsers = parser.add_argument_group(title='Control output',
        description='Control the output of the script')
    subparsers.add_argument("-c", "--no-console", help="Dont output to the console", default=False, action='store_true')
    subparsers.add_argument("-f", "--no-file", help="Dont output to files", default=False, action='store_true')
    
    args=parser.parse_args()
    path = NAS_PATH if args.nas else args.path
    if args.download: args.update = True
    console = not args.no_console
    file_output = not args.no_file
    downloader(args.data, path, download=args.download, check_stats=args.stats, update=args.update, special_file=args.missing, stats_file=args.output, console=console, file_output=file_output)