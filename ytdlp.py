from tqdm.auto import tqdm, trange # type: ignore
import yaml, ssl, os, argparse, re, shutil # type: ignore
import ntpath # type: ignore
from yt_dlp import YoutubeDL, postprocessor # type: ignore
from yt_dlp.utils import sanitize_filename # type: ignore

DATA_FILE = 'data.yml'
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
    # 'cookiefile': 'cookies.txt',
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

DOWNLOAD_OPTIONS_BASE = {
    'fragment_retries': 10,
}

DOWNLOAD_OPTIONS_WAIT = {
    'ratelimit': 4000000,
    'sleep_interval_requests': 2,
    'sleep_interval': 20,    
    'max_sleep_interval': 120,
    'sleep_interval_subtitles': 2,
}

class DownloadErrorException(Exception):
    """Base class for other exceptions"""
    pass

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
        if 'Private video' in msg: return
        if 'Video unavailable' in msg: return
        if 'members' in msg: return
        if 'Sign in to confirm your age' in msg: return
        # if "Sign in to confirm you’re not a bot" not in msg:
        #     return
        self.pbar.write(f"    {msg}")
        # raise DownloadErrorException()

def safe_filename(s: str, max_length: int = 255) -> str:
    """Sanitize a string making it safe to use as a filename."""
    characters = [r'"', r"\*", r"\.", r"\/", r"\:", r'"', r"\<", r"\>", r"\?", r"\\", r"\|", r"\\\\"]
    regex = re.compile("|".join([chr(i) for i in range(31)] + characters), re.UNICODE)
    filename = regex.sub("", s)
    return filename[:max_length].rsplit(" ", 0)[0]

class Stats:
    def __init__(self):
        self.stats = {}

    def add_missing(self, missing):
        self._add_key('missing', missing)

    def add_data_missing(self, downloaded):
        self._add_key('data_missing', downloaded)

    def add_ignored(self, ignored):
        self._add_key('ignored', ignored)
    
    def add_deleted(self, deleted):
        self._add_key('deleted', deleted)
    
    def add_skipped(self, skipped):
        self._add_key('skipped', skipped, discard=True)

    def add_failed(self, failed):
        self._add_key('failed', failed, discard=True)
    
    def add_submitted(self, submitted):
        self._add_key('submitted', submitted, discard=False)

    def add_downloaded(self, downloaded):
        self._add_key('downloaded', downloaded, discard=True)

    def calculate_globals(self, pbar, stats_file, console, file_output):
        self.stats['global'] = {}
        for key in ['submitted', 'data_missing', 'missing', 'failed', 'downloaded']:
            total = sum([elem[key] for elem in self.stats.values() if key in elem])
            if total > 0: self.stats['global'][key] = total
        if 'submitted' in self.stats['global'] and console:
            pbar.write(f"Submitted {self.stats['global']['submitted']} videos in total")
        if 'downloaded' in self.stats['global'] and console:
            pbar.write(f"Downloaded {self.stats['global']['downloaded']} videos in total")
        if not file_output: return
        with open(stats_file, 'w') as file:
            yaml.dump(self.stats, file)

    def add_special_files(self, special_files):
        for file in special_files: self._add_key('special', file)

    def add_category(self, category, value):
        self.stats[category] = value.stats
    
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

    def get_ignored(self):
        if 'ignored' in self.stats: return self.stats['ignored']
        return 0

    def output(self, pbar, console, list_info):
        if not console: return
        if 'ignored' in self.stats: 
            pbar.write(f"    Ignored {self.stats['ignored']} videos")
        if 'ignored' in self.stats and list_info:
            for file in self.stats['ignored_file']:
                pbar.write(f"        \"{file['title']}\" - \"{file['reason']}\"")
        if 'submitted' in self.stats: pbar.write(f"    Submitted {self.stats['submitted']} videos")
        if 'downloaded' in self.stats: pbar.write(f"    Downloaded {self.stats['downloaded']} videos")
        if 'data_missing' in self.stats: pbar.write(f"    Data missing {self.stats['data_missing']} videos")
        if 'data_missing' in self.stats and list_info:
            for file in self.stats['data_missing_file']:
                pbar.write(f"        \"{file['title']}\"")
        if 'missing' in self.stats: pbar.write(f"    Missing {self.stats['missing']} videos")
        if 'missing' in self.stats and list_info:
            for file in self.stats['missing_file']:
                pbar.write(f"        \"{file['title']}\"")
        if 'deleted' in self.stats: pbar.write(f"    Deleted {self.stats['deleted']} videos")
        if 'deleted' in self.stats and list_info:
            for file in self.stats['deleted_file']:
                pbar.write(f"        \"{file['title']}\"")
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
    
    @property
    def ignore(self):
        return self.playlist_data['ignore'] if 'ignore' in self.playlist_data else {}
    
    def save(self, archive=True):
        playlist_data_file = os.path.join(DATA_PATH, self.name + '.yml')
        with open(playlist_data_file, 'w') as file:
            yaml.dump(self.playlist_data, file)
        if not archive: return
        playlist_data_archive = os.path.join(DATA_PATH, self.name + '.txt')
        with open(playlist_data_archive, 'w') as file:
            for url in self.playlist_data['downloaded']:
                file.write("youtube " + url.replace("https://youtube.com/watch?v=", "") + "\n")
            if 'ignore' in self.playlist_data:
                for url in self.playlist_data['ignore']:
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
    
    def finalize(self, update=False, console=True, file_output=True, list_info=False):
        self.stats.output(self.pbar, console=console, list_info=list_info)
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

    def _check_special_files(self):
        self.existing_files = []
        if os.path.exists(self.outputdir):
            self.existing_files = [name for name in os.listdir(self.outputdir) if os.path.isfile(os.path.join(self.outputdir, name)) and (name.endswith('.mp4') or name.endswith(".m4a"))]
            if os.name == 'posix': self.existing_files = [unicodedata.normalize('NFC', name) for name in self.existing_files]
            filesnames = [elem['file'] for elem in self.playlist_data.info.values()]
            if os.name == 'posix': filesnames = [unicodedata.normalize('NFC', name) for name in filesnames]
            filesnames = [ntpath.basename(name) for name in filesnames]
            self.stats.add_special_files([file for file in self.existing_files if file not in filesnames])
        for url in self.playlist_data.ignore:
            self.stats.add_ignored(self.playlist_data.ignore[url])

    def _download_video(self, wait=True, file_output=True, console=True):
        """Download the video"""
        download_opts = self.opts.copy()
        download_opts.update(DOWNLOAD_OPTIONS_BASE)
        if wait: download_opts.update(DOWNLOAD_OPTIONS_WAIT)
        download_opts['download_archive'] = self.playlist_data.archive
        info_dict = {}
        with YoutubeDL(download_opts) as ydl:
            info = ydl.sanitize_info(ydl.extract_info(self.url, download=False))
            if console: self.pbar.write(f"Downloading {self.name} with {info['playlist_count']} videos...")
            pbar_playlist = trange(info['playlist_count'], leave=False, desc=self.name, ascii=True, miniters=1)
            pbar_video = trange(100, leave=False, desc='Starting', ascii=True)
            pbar_playlist.update(self.stats.get_skipped())
            pbar_playlist.update(self.stats.get_ignored())
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
                    'file': ntpath.basename(filename),
                }
                self.stats.add_downloaded(result)
                pbar_playlist.update(1)
                self.playlist_data.add(result)
                if file_output: self.playlist_data.save(archive=False)
            ydl.add_progress_hook(tqdm_hook)
            ydl.add_postprocessor_hook(tqdm_hook_post)
            ydl.add_post_hook(post_hook)
            ydl.download([self.url])
        pbar_video.close()
        pbar_playlist.close()

    def _check_stats(self, url, title, channel, item, console=True, update=False, nas=False):
        safe_chars = {'/': '', ':': '', '*': '', '"': '_', '<': '', '>': '', '|': '', '?': ''}
        existing_file = next((f for f in self.existing_files if safe_filename(title.translate(str.maketrans(safe_chars))) in f), None)
        if not existing_file:
            existing_file = next((f for f in self.existing_files if sanitize_filename(title) in f), None)
        url = url.replace("https://www.", "https://")
        in_playlist = url in self.playlist_data.downloaded
        record = item.copy()
        record['url'] = url
        record['title'] = title
        record.pop('channel', None)
        record['channel'] = channel
        if 'ignore' in self.playlist_data.playlist_data and url in self.playlist_data.playlist_data['ignore']:
            return
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
            if os.name == 'posix' and not nas: 
                self.stats.add_skipped(record)
                return
            item = self.playlist_data.info[url]['file']
            if os.name == 'posix': item = unicodedata.normalize('NFC', item)
            filesnames = [elem for elem in self.existing_files]
            if os.name == 'posix': filesnames = [unicodedata.normalize('NFC', name) for name in filesnames]
            filename = os.path.join(self.outputdir, item)
            if not os.path.exists(filename) and item not in filesnames:
                self.stats.add_missing(record)
            else: self.stats.add_skipped(record)
        if existing_file and in_playlist: self.stats.add_skipped(record)

    def progress(self, download=False, stat_checker=False, update=False, wait=True, console=True, nas=False):
        if stat_checker: 
            stat_opts = self.opts.copy()
            stat_opts.update(STATS_OPTIONS)
            self._check_special_files()
            with YoutubeDL(stat_opts) as ydl:
                info = ydl.sanitize_info(ydl.extract_info(self.url, download=False))
                if info is None: 
                    if console: self.pbar.write(f"Error: {self.name} not found")
                    return
                pbar_playlist = trange(info['playlist_count'], leave=False, desc=self.name, ascii=True, miniters=1)
                if console: self.pbar.write(f"Checking stats of {self.name} with {info['playlist_count']} videos")
                for entry in info['entries']:
                    if 'view_count' in entry and entry['view_count'] is not None:
                        self._check_stats(entry['url'], entry['title'], entry['channel'], self.item, console=console, update=update, nas=nas)
                    else:
                        self.stats.add_skipped(entry)
                    pbar_playlist.update(1)
                    pbar_playlist.refresh()
                urls = [entry['url'].replace("https://www.", "https://") for entry in info['entries'] if 'view_count' in entry and entry['view_count'] is not None]
                for key in self.playlist_data.info.keys():
                    if key not in urls: self.stats.add_deleted(os.path.splitext(self.playlist_data.info[key]['file'])[0])
                pbar_playlist.close()
            if not self.stats.has_submitted(): return
        if download: self._download_video(wait=wait, file_output=file_output, console=console)

def downloader(data_file, path, download, check_stats, update, wait, stats_file, console, file_output, list_info, nas):
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
    try:
        for item in pbar:
            item_downloader = ItemDownloader(item, pbar, path)
            item_downloader.progress(download=download, stat_checker=check_stats, update=update, wait=wait, console=console, nas=nas)
            stats.add_category(item['name'], item_downloader.finalize(update=True if download else update, console=console, file_output=file_output, list_info=list_info))
    except KeyboardInterrupt as e:
        pbar.write("Interrupted by user")
    except DownloadErrorException as e:
        pbar.write(f"Encountered error while downloading.")
    except Exception as e:
        import traceback
        pbar.write(f"Error: {e}")
        traceback.print_exc()
        pbar.write("Exiting")
    if check_stats: stats.calculate_globals(pbar, stats_file, console, file_output)
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
    parser.add_argument("-e", "--nas", "--external", help="Whether to use the NAS storage (only for MacOS)", default=False, action='store_true')
    parser.add_argument("-s", "--stats", help="Create a list of files to be downloaded", default=False, action='store_true')
    parser.add_argument("-u", "--update", help="Update the data files (automatically True if downloading, still required if you want to update missing elements)", default=False, action='store_true')
    parser.add_argument("-d", "--download", help="Download the files", default=False, action='store_true')
    parser.add_argument("-w", "--no-wait", help="Don't wait between requests", default=False, action='store_true')

    subparsers = parser.add_argument_group(title='File Output',
        description='Set the files to output to')
    subparsers.add_argument("-p", "--path", help="The path to download to (default: '"+DEFAULT_PATH+"')", default=DEFAULT_PATH)
    subparsers.add_argument("-i", "--data", "--input", help="The data file to use (default: '"+DATA_FILE+"')", default=DATA_FILE)
    subparsers.add_argument("-o", "--output", help="The stats file to use (default: '"+STATS_FILE+"')", default=STATS_FILE)

    subparsers = parser.add_argument_group(title='Control output',
        description='Control the output of the script')
    subparsers.add_argument("-c", "--no-console", help="Dont output to the console", default=False, action='store_true')
    subparsers.add_argument("-f", "--no-file", help="Dont output to files", default=False, action='store_true')
    subparsers.add_argument("-l", "--list-info", help="Output the full info of the stats", default=False, action='store_true')
    
    args=parser.parse_args()
    path = NAS_PATH if args.nas else args.path
    if args.download: args.update = True
    console = not args.no_console
    file_output = not args.no_file
    downloader(args.data, path, download=args.download, check_stats=args.stats, update=args.update, wait=not args.no_wait, stats_file=args.output, console=console, file_output=file_output, list_info=args.list_info, nas=args.nas)