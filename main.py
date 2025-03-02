from pytubefix import Playlist, Channel, YouTube  # type: ignore
from pytubefix.exceptions import LoginRequired, BotDetection, AgeRestrictedError  # type: ignore
from tqdm.auto import tqdm, trange  # type: ignore
import yaml, ffmpeg, os, ssl, signal, requests, shutil, subprocess, re, time, datetime, errno, pathlib, random  # type: ignore
from concurrent.futures import ThreadPoolExecutor, as_completed
import traceback

RETRIES, MAX_RETRIES, TIMEOUT, MAX_WORKERS, MAX_SUBMITTED = 1, 1, 30, 1, 2000
SUCCESS_CODE, MUSIC_CODE, FAILED_CODE, SKIPPED_CODE, IGNORE_CODE = "Downloaded", "Music", "Failed", "Skipped", "Ignored"
DATA_FILE, IGNORE_FILE, FAILED_FILE, RESULTS_FILE, SPECIAL_FILE, STATS_FILE = 'data.yml', 'ignore.yml', 'failed.yml', 'results.yml', 'special.yml', 'stats.yml'
ON_SUCCESS_SLEEP, ON_FAIL_SLEEP, ON_SKIP_SLEEP, RANDOMNESS = 5, 5, 5, 0
AUDIO_EXT = ".m4a"

DOWNLOAD = False

# TODO
# First loop over all playlists and create list of videos to be downloaded
# Next have a function to go over that list to download the videos
# Would allow us to have several scripts
#  1. To download the videos using pytubefix
#  2. To download the videos using yt-dlp
#  3. To create the data file for the videos and do some managing of files

if os.name == 'posix':
    DEFAULT_PATH = "~/Downloads/youtube"
    MAIN_DATA_FILE = "~/Downloads/youtube/data"
    TMP_DIR = "~/Downloads/youtube"
    YML_FILE_DIR = "~/Downloads/youtube/data"
else:
    TMP_DIR, YML_FILE_DIR = "X:\\youtube", "X:\\youtube\\data"
    DEFAULT_PATH = "X:\\youtube"
    MAIN_DATA_FILE = "X:\\youtube\\data"

def getchapters(yt, filename):
    markers_map = yt.initial_data.get('playerOverlays', {}).get('playerOverlayRenderer', {}).get(
        'decoratedPlayerBarRenderer', {}).get('decoratedPlayerBarRenderer', {}).get('playerBar', {}).get(
        'multiMarkersPlayerBarRenderer', {}).get('markersMap', [])
    chapters_data = next(
        (marker['value']['chapters'] for marker in markers_map if marker['key'].upper() in {'DESCRIPTION_CHAPTERS', 'AUTO_CHAPTERS'}),
        [])
    chapters = [ {
            'start': chapter['chapterRenderer']['timeRangeStartMillis'],
            'title': chapter['chapterRenderer']['title']['simpleText']
        } for chapter in chapters_data ]
    text = ";FFMETADATA1\n" + "\n".join(
        f"[CHAPTER]\nTIMEBASE=1/1000\nSTART={ch['start']}\n\ntitle={ch['title']}\n"
        for ch in chapters)
    with open(filename, "w", encoding="utf-8") as myfile: myfile.write(text)
    return chapters

def make_title_safe(video, default_filename, old=False):
    safe_chars = {'/': '', ':': '', '*': '', '"': '_', '<': '', '>': '', '|': '', '?': ''}
    try:
        return safe_filename(video.title.translate(str.maketrans(safe_chars)), old=old)
    except KeyError:
        return default_filename

def safe_filename(s: str, max_length: int = 255, old=False) -> str:
    """Sanitize a string making it safe to use as a filename."""
    characters = [r'"', r"\*", r"\.", r"\/", r"\:", r'"', r"\<", r"\>", r"\?", r"\\", r"\|", r"\\\\"]
    if old:
        characters = [r'"', r"\#", r"\$", r"\%", r"'", r"\*", r"\,", r"\.", r"\/", 
                r"\:", r'"', r"\;", r"\<", r"\>", r"\?", r"\\", r"\^", r"\|", r"\~", r"\\\\"]
    regex = re.compile("|".join([chr(i) for i in range(31)] + characters), re.UNICODE)
    filename = regex.sub("", s)
    return filename[:max_length].rsplit(" ", 0)[0]

def ascii_text(input_text):
    return input_text.encode('ascii','ignore').decode()

def silentremove(filename):
    if filename is None: return
    try:
        os.remove(filename)
    except OSError as e:
        if e.errno != errno.ENOENT:
            raise

def merge_files(yt, output_dir, prefix, path_imagesource, path_audiosource, filename, tmp_dir, counter, mp3):
    subtitles = False
    thumbnail = False
    chapters = []
    success = True
    author = "Unknown"
    description = "Unknown"
    if 'videoSecondaryInfoRenderer' in yt.initial_data["contents"]["twoColumnWatchNextResults"][
                "results"]["results"]["contents"][1]:
        author = yt.initial_data["contents"]["twoColumnWatchNextResults"][
                    "results"]["results"]["contents"][1]["videoSecondaryInfoRenderer"][
                    "owner"]["videoOwnerRenderer"]['title']['runs'][0]['text']
        if 'attributedDescription' in yt.initial_data["contents"]["twoColumnWatchNextResults"][
                    "results"]["results"]["contents"][1]["videoSecondaryInfoRenderer"]:
            description = yt.initial_data["contents"]["twoColumnWatchNextResults"][
                    "results"]["results"]["contents"][1]["videoSecondaryInfoRenderer"][
                    "attributedDescription"]["content"]
    if not mp3:
        filename_chapter = os.path.join(tmp_dir, "tmp_" + str(counter) + ".ffmeta")
        chapters = getchapters(yt, filename_chapter)
        key = 'en'
        filename_srt = None
        if key not in yt.captions.keys(): key = "a.en"
        if key in yt.captions.keys():
            try:
                caption = yt.captions[key]
                caption.download("tmp_" + str(counter), srt=True, output_path=tmp_dir)
                filename_srt = os.path.join(tmp_dir, "tmp_" + str(counter) + f" ({key})" + ".srt")
                subtitles = True
            except AttributeError as e:
                if key == "en" and "a.en" in yt.captions.keys(): 
                    try:
                        key = "a.en"
                        caption = yt.captions[key]
                        caption.download("tmp_" + str(counter), srt=True, output_path=tmp_dir)
                        filename_srt = os.path.join(tmp_dir, "tmp_" + str(counter) + f" ({key})" + ".srt")
                        subtitles = True
                    except AttributeError as e:
                        pass
        res = requests.get(yt.thumbnail_url, stream = True)
        filename_thumb = None
        if res.status_code == 200:
            thumbnail = True
            filename_thumb = os.path.join(tmp_dir, "tmp_" + str(counter) + ".jpg")
            with open(filename_thumb,'wb') as f:
                shutil.copyfileobj(res.raw, f)
        if filename_srt or filename_thumb:
            video = ffmpeg.input(path_imagesource).video
            audio = ffmpeg.input(path_audiosource).audio
            chapter_ff = ffmpeg.input(filename_chapter)
            desc = "Title: "+yt.title+"\nAuthor: "+author+"\nPublished: "+str(yt.publish_date)+"\nTags: "+", ".join(yt.keywords)+"\nAge Restricted: "+str(yt.age_restricted)#+"\nViews: "+str(yt.views) # TODO: Fix this
            desc = desc + "\nSubtitles: " + str(subtitles) + "\nThumbnail: " + str(thumbnail) + "\nChapters: " + str(chapters)+"\nDescription: "+description
            if filename_srt and not filename_thumb:
                success = False
            elif filename_thumb and not filename_srt:
                cover = ffmpeg.input(filename_thumb)
                cmd = (
                    ffmpeg
                    .output(video, audio, chapter_ff, cover, filename, vcodec='copy', acodec='copy', **{'c:v:1': 'png'}, **{'disposition:v:1': 'attached_pic'}, 
                            **{'metadata:g:0': f"artist={author}"}, **{'metadata:g:1': f"title={yt.title}"}, **{'metadata:g:2': f"genre={yt.publish_date}"}, **{'metadata:g:3': f"comment={desc}"})
                    .global_args('-map_metadata', '2')
                    .global_args('-loglevel', 'quiet')
                    .global_args('-y')
                    .compile()
                )
                idx = cmd.index('2')
                cmd.pop(idx)
                cmd.pop(idx)
                subprocess.call(cmd)
            else:
                input_subtitle = ffmpeg.input(filename_srt)
                cover = ffmpeg.input(filename_thumb)
                cmd = (
                    ffmpeg
                    .output(video, audio, input_subtitle, chapter_ff, cover, filename, vcodec='copy', acodec='copy', **{'c:v:1': 'png'}, **{'disposition:v:1': 'attached_pic'}, **{'c:s': 'mov_text'}, 
                            **{'metadata:g:0': f"artist={author}"}, **{'metadata:g:1': f"title={yt.title}"}, **{'metadata:g:2': f"genre={yt.publish_date}"}, **{'metadata:g:3': f"comment={desc}"}
                            , **{'metadata:s:s:0': f"title=English Subs"})
                    .global_args('-map_metadata', '3')
                    .global_args('-loglevel', 'quiet')
                    .global_args('-y')
                    .compile()
                )
                idx = cmd.index('3')
                cmd.pop(idx)
                cmd.pop(idx)
                subprocess.call(cmd)
        silentremove(filename_srt)
        silentremove(filename_chapter)
        silentremove(filename_thumb)
        silentremove(path_imagesource)
        silentremove(path_audiosource)
    if filename.endswith(".mp4"): filename = filename[:-4] + ".yml"
    elif filename.endswith(AUDIO_EXT): filename = filename[:-4] + ".yml"
    filename = filename.replace("youtube", "youtube\\data")
    if os.path.isfile(filename): return success
    info = {}
    info['title'] = yt.title
    info['author'] = author
    info['description'] = description
    info['tags'] = yt.keywords
    info['age_restricted'] = yt.age_restricted
    info['publish_date'] = yt.publish_date
    # info['views'] = yt.views # TODO: Fix this
    info['subtitles'] = subtitles
    info['thumbnail'] = thumbnail
    info['chapters'] = chapters
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, 'w') as file:
        yaml.dump(info, file)
    return success

def download_video(ignore_urls, video, item, output_dir, mp3, pbar, counter, trailing, tmp_dir, channel):
    prog_bar_video = trange(100, leave=False, desc=ascii_text(f'Startup - \"\"'), ascii=True)
    try:
        prefix = ""
        if 'add_channel' in item and item['add_channel']:
            prefix = f"[{video.author}] - "
        if 'count' in item and item['count']:
            prefix = f"{str(counter).zfill(trailing)} - " + prefix
        
        video = YouTube(video.watch_url, 'WEB', use_oauth=False, allow_oauth_cache=True) 
        if video.watch_url in ignore_urls:
            return {
                'status' : IGNORE_CODE,
                "title" : video.title,
                'location': output_dir,
                'url': video.watch_url
            }
        trying = 0
        filename_vid = ""
        filename_aud = ""
        prog_bar_video.set_description_str(ascii_text(f'Video - \"{video.title}\"'))
        subtype = "mp4" if not mp3 else AUDIO_EXT[1:]
        file_path = str(pathlib.Path(output_dir) / f"{prefix}{make_title_safe(video, '')}.{subtype}")
        file_path_old = str(pathlib.Path(output_dir) / f"{prefix}{make_title_safe(video, '', True)}.{subtype}")
        if os.path.isfile(file_path_old):
            try:
                os.replace(file_path_old, file_path)
            except OSError as e:
                if e.errno != errno.ENOENT:
                    raise
        if os.path.isfile(file_path):
            prog_bar_video.set_description_str(ascii_text(f'Skipped - \"{video.title}\"'))
            time.sleep(ON_SKIP_SLEEP)
            prog_bar_video.close()
            return {
                'status' : SKIPPED_CODE,
                "title" : video.title,
                "file": os.path.basename(file_path),
                'location': os.path.join(item['location'], item['name']) if 'location' in item else item['name'],
                'url': video.watch_url
            }
        while trying < RETRIES:
            try: 
                def on_progress(stream, chunk, bytes_remaining):
                    """Callback function"""
                    total_size = stream.filesize
                    bytes_downloaded = total_size - bytes_remaining
                    pct_completed = bytes_downloaded / total_size * 100
                    prog_bar_video.update(round(pct_completed - prog_bar_video.n))
                def on_complete(stream, file_path):
                    pass
                video.register_on_progress_callback(on_progress)
                video.register_on_complete_callback(on_complete)
                if not mp3:
                    prog_bar_video.set_description_str(ascii_text(f'Video - \"{video.title}\"'))
                    prog_bar_video.reset()
                    check_vid = video.streams.filter(adaptive=True, file_extension='mp4', only_video=True).order_by('resolution').desc().first()
                    filename_vid = check_vid.get_file_path("tmp_" + str(counter), tmp_dir)
                    check_vid.download(tmp_dir, filename="tmp_" + str(counter), skip_existing=False, max_retries=MAX_RETRIES, timeout=TIMEOUT)
                    check_aud = video.streams.get_audio_only()
                    filename_aud = check_aud.get_file_path("tmp_" + str(counter) + AUDIO_EXT, tmp_dir)
                    prog_bar_video.set_description_str(ascii_text(f'Audio - \"{video.title}\"'))
                    prog_bar_video.reset()
                    check_aud.download(tmp_dir, filename="tmp_" + str(counter) + AUDIO_EXT, skip_existing=False, max_retries=MAX_RETRIES, timeout=TIMEOUT)
                    prog_bar_video.set_description_str(ascii_text(f'Merging - \"{video.title}\"'))
                    success = merge_files(video, output_dir, prefix, filename_vid, filename_aud, file_path, tmp_dir, counter, mp3)
                    prog_bar_video.set_description_str(ascii_text(f'Success - \"{video.title}\"'))
                    time.sleep(ON_SUCCESS_SLEEP + (random.random()*2*RANDOMNESS-RANDOMNESS))
                    prog_bar_video.close()
                    return {
                        'status' : SUCCESS_CODE if success else FAILED_CODE,
                        "title" : video.title,
                        "file": os.path.basename(file_path),
                        'location': os.path.join(item['location'], item['name']) if 'location' in item else item['name'],
                        'url': video.watch_url
                    }
                else:
                    prog_bar_video.set_description_str(ascii_text(f'Music - \"{video.title}\"'))
                    check = video.streams.get_audio_only()
                    check.download(output_dir, filename_prefix=prefix, filename=filename_aud, skip_existing=True, max_retries=MAX_RETRIES, timeout=TIMEOUT)
                    merge_files(video, output_dir, prefix, None, filename_aud, file_path, tmp_dir, counter, mp3)
                    prog_bar_video.set_description_str(ascii_text(f'Success - \"{video.title}\"'))
                    time.sleep(ON_SUCCESS_SLEEP + (random.random()*2*RANDOMNESS-RANDOMNESS))
                    prog_bar_video.close()
                    return {
                        'status' : MUSIC_CODE,
                        "title" : video.title,
                        "file": os.path.basename(check.get_file_path(output_path=output_dir)),
                        'location': os.path.join(item['location'], item['name']) if 'location' in item else item['name'],
                        'url': video.watch_url
                    }
            except KeyboardInterrupt:
                pbar.write(f"    Error downloading: \"{video.title}\" with KeyboardInterrupt")
                prog_bar_video.close()
                return None
            except AgeRestrictedError as e:
                pbar.write(f"    Error downloading: \"{video.title}\" with AgeRestrictedError")
                time.sleep(ON_FAIL_SLEEP)
                prog_bar_video.close()
                return {
                    'status' : FAILED_CODE,
                    'title': video.title,
                    'location': output_dir,
                    'url': video.watch_url,
                    'error': str(e),
                    'counter': counter,
                }
            except Exception as e:
                trying = trying + 1
                prog_bar_video.set_description_str(ascii_text(f'Fail - \"{video.title}\"'))
                time.sleep(ON_FAIL_SLEEP)
                if trying >= RETRIES:
                    pbar.write(f"    Error downloading: \"{video.title}\" with error {e}")
                    silentremove(filename_vid)
                    silentremove(filename_aud)
                    prog_bar_video.close()
                    return None
    except LoginRequired as e:
        pbar.write(f"    Error downloading: \"{video.title}\" with LoginRequired")
        time.sleep(ON_FAIL_SLEEP)
        prog_bar_video.close()
        return None
    except BotDetection as e:
        pbar.write(f"    Error downloading: \"{counter}\" with BotDetection")
        prog_bar_video.close()
        return None
    except Exception as e:
        pbar.write(f"    Error downloading: \"{counter}\" with {e} error")
        time.sleep(ON_FAIL_SLEEP)
        prog_bar_video.close()
        return None
    pbar.write(f"    Error downloading: \"{counter}\" with unknown")
    time.sleep(ON_FAIL_SLEEP)
    prog_bar_video.close()
    return None

def addPlaylistData(data_file, result):
    if 'downloaded' not in data_file: data_file['downloaded'] = set([])
    if 'info' not in data_file: data_file['info'] = {}
    data_file['downloaded'].add(result['url'])
    data_file['info'][result['url']] = {
        'title': result['title'],
        'location': result['location'],
        'file': result['file']
    }

def downloader(data_file, ignore_file, failed_file, results_file, tmp_dir):
    ssl._create_default_https_context = ssl._create_unverified_context
    failed = {}
    data = []
    global DOWNLOAD
    def signal_handler(sig, frame):
        exit(1)
    signal.signal(signal.SIGINT, signal_handler)
    with open(data_file, 'r') as file:
        data = yaml.safe_load(file)
        print(f"Downloading {len(data)} channels or playlists")
    with open(ignore_file, 'r') as file:
        ignore = yaml.safe_load(file)
        ignore_urls = []
        if ignore is not None:
            ignore_urls = [item['url'] for item in ignore]
        print(f"Ignoring {len(ignore_urls)} urls")
    results = {}
    special_files = {}
    stats = {}
    try:
        i = 0
        pbar = tqdm(data, desc='Total', ascii=True)
        submitted = 0
        total_fails = 0
        total_downloads = 0
        total_anomalies = 0
        total_existing = 0
        total_to_be_added = 0
        total = 0
        for item in pbar:
            if submitted > MAX_SUBMITTED: 
                pbar.write(f"Maximum downloaded videos reached. Exiting...")
                break
            pl = None
            title = item['name']
            channel = False
            location = os.path.join(DEFAULT_PATH, item['location']) if 'location' in item else DEFAULT_PATH
            output_dir = os.path.join(location, item['name'] )
            i = i + 1
            playlist_data = {
                'downloaded' : set([]),
                'info' : {}
            }
            playlist_data_file = os.path.join(MAIN_DATA_FILE, item['name'] + '.yml')
            playlist_data_archive = os.path.join(MAIN_DATA_FILE, item['name'] + '.txt')
            existing_files = []
            anomaly = 0
            if os.path.exists(playlist_data_file):
                with open(playlist_data_file, 'r') as file:
                    playlist_data = yaml.safe_load(file)
            if os.path.exists(output_dir):
                existing_files = [name for name in os.listdir(output_dir) if os.path.isfile(os.path.join(output_dir, name)) and (name.endswith('.mp4') or name.endswith(AUDIO_EXT))]

                filesnames = [elem['file'] for elem in playlist_data['info'].values()]
                for file in existing_files:
                    if file not in filesnames: 
                        if title not in special_files: special_files[title] = []
                        special_files[title].append(os.path.join(file))
                        anomaly = anomaly + 1
                total_anomalies = total_anomalies + anomaly
                total_existing = total_existing + len(existing_files)
            if 'channel' in item and item['channel']:
                pl = Channel(item['url'], 'WEB')
                desc = "Channel"
                channel = True
            else:
                pl = Playlist(item['url'], 'WEB', use_oauth=False, allow_oauth_cache=True)
                desc = "Playlist"
            mp3 = False
            if 'mp3' in item and item['mp3']: mp3 = True
            trailing = len(str(len(pl.videos)))
            results[item['name']] = []
            pbar.write(f'Downloading #{i}: {title} with {len(pl.videos)} videos and {len(existing_files)} existing files (outpath: {output_dir})')
            total = total + len(pl.videos)
            os.makedirs(output_dir, exist_ok=True)
            quit = False
            fails = 0
            downloaded = 0
            local_submitted = 0
            # pbar.write(f"{pl.initial_data}")
            titles = []
            if not channel:
                videos_data = pl.initial_data["contents"] \
                    ["twoColumnBrowseResultsRenderer"] \
                    ["tabs"][0] \
                    ["tabRenderer"]["content"] \
                    ["sectionListRenderer"]["contents"][0] \
                    ["itemSectionRenderer"]["contents"][0] \
                    ["playlistVideoListRenderer"]["contents"]
                for video in videos_data:
                    if "playlistVideoRenderer" in video:
                        # Sometimes the title is broken into runs, so join them.
                        title_runs = video["playlistVideoRenderer"]["title"]["runs"]
                        title = " ".join(run["text"] for run in title_runs)
                        titles.append(title)
            # else:
            #     # Locate the Videos tab
            #     tabs = pl.initial_data.get('contents', {}) \
            #             .get('twoColumnBrowseResultsRenderer', {}) \
            #             .get('tabs', [])
            #     for tab in tabs:
            #         tab_renderer = tab.get('tabRenderer', {})
            #         # Check for the Videos tab by its title and/or selected flag
            #         if tab_renderer.get('title') == 'Videos' and tab_renderer.get('selected', False):
            #             # Navigate into the rich grid content where videos are listed
            #             rich_grid = tab_renderer.get('content', {}) \
            #                                     .get('richGridRenderer', {})
            #             for item in rich_grid.get('contents', []):
            #                 # Each item may be wrapped in a "richItemRenderer"
            #                 video = item.get('richItemRenderer', {}) \
            #                             .get('content', {}) \
            #                             .get('videoRenderer', None)
            #                 if video:
            #                     # The video title is inside videoRenderer.title.runs
            #                     runs = video.get('title', {}).get('runs', [])
            #                     if runs:
            #                         # Extract the text from the first run
            #                         title = runs[0].get('text')
            #                         titles.append(title)
            #             break  # Exit after processing the Videos tab
            for title in titles:
                pbar.write(f"    Processing video: {title}")
                safe_chars = {'/': '', ':': '', '*': '', '"': '_', '<': '', '>': '', '|': '', '?': ''}
                existing_file = next((f for f in existing_files if safe_filename(title.translate(str.maketrans(safe_chars))) in f), None)
                in_playlist = next((f for f in playlist_data['info'].values() if title in f['title']), None)
                if existing_file and not in_playlist:
                    pbar.write(f"    Skipping existing file: {existing_file}")
                    total_to_be_added = total_to_be_added + 1
                    # addPlaylistData(playlist_data, {
                    #     'url': video.watch_url,
                    #     'title': video.title,
                    #     'location': output_dir,
                    #     'file_path': os.path.join(output_dir, existing_file)
                    # })

            with tqdm(pl.videos, position=1, leave=False, desc=ascii_text(f'{desc} - ({downloaded}D / {fails}F)'), ascii=True) as pbar_video:
                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
                    counter = 0
                    futures = []
                    for video in pl.videos:
                        counter = counter + 1
                        if submitted > MAX_SUBMITTED: break
                        if (video.watch_url in ignore_urls) or (video.watch_url in playlist_data['downloaded']):
                            if not video.watch_url in playlist_data['info']:
                                if 'other' not in playlist_data: playlist_data['other'] = set([])
                                playlist_data['other'].add(video.watch_url)
                                pbar_video.update(1)
                                continue
                            filename = playlist_data['info'][video.watch_url]['file']
                            filename = os.path.join(DEFAULT_PATH, playlist_data['info'][video.watch_url]['location'], filename)
                            if not os.path.exists(filename):
                                pbar.write(f"    Missing file {filename}")
                                playlist_data['downloaded'].remove(video.watch_url)
                            else:
                                if filename.endswith(".mp4"): filename = filename[:-4] + ".yml"
                                elif filename.endswith(AUDIO_EXT): filename = filename[:-4] + ".yml"
                                filename = filename.replace("youtube", "youtube\\data")
                                if not os.path.isfile(filename):
                                    try:

                                        characters = [r'"', r"\#", r"\$", r"\%", r"'", r"\*", r"\,", r"\.", r"\/", 
                                                    r"\:", r'"', r"\;", r"\<", r"\>", r"\?", r"\\", r"\^", r"\|", r"\~", r"\\\\"]
                                        regex = re.compile("|".join([chr(i) for i in range(31)] + characters), re.UNICODE)
                                        filename_old = regex.sub("", os.path.splitext(os.path.basename(filename))[0])
                                        filename_old = filename_old[:256].rsplit(" ", 0)[0]
                                        file_path_old = os.path.join(os.path.dirname(filename), filename_old + ".yml")
                                        os.replace(file_path_old, filename)
                                    except OSError as e:
                                        if e.errno != errno.ENOENT:
                                            raise
                                pbar_video.update(1)
                                continue
                        if DOWNLOAD:
                            futures.append(ex.submit(download_video, ignore_urls, video, item, output_dir, mp3, pbar, counter, trailing, tmp_dir, channel))
                        submitted = submitted + 1
                        local_submitted = local_submitted + 1
                    if local_submitted > 0:
                        pbar.write(f"    Submitted {local_submitted} videos for downloading")
                    for future in as_completed(futures):
                        if future.cancelled(): continue
                        try:
                            result = future.result()
                            if result is None:
                                for future in futures:
                                    if not future.done(): future.cancel()
                                quit = True
                                fails = fails + 1
                                if item['name'] not in failed:
                                    failed[item['name']] = []
                                failed[item['name']].append(result)
                            elif result['status'] == FAILED_CODE:
                                fails = fails + 1
                                if item['name'] not in failed:
                                    failed[item['name']] = []
                                failed[item['name']].append(result)
                            elif result['status'] == SUCCESS_CODE or result['status'] == MUSIC_CODE:
                                results[item['name']].append(result)
                                addPlaylistData(playlist_data, result)
                                downloaded = downloaded + 1
                            elif result['status'] == SKIPPED_CODE:
                                addPlaylistData(playlist_data, result)
                            pbar_video.update(1)
                            pbar_video.set_description_str(ascii_text(f'{desc} - ({downloaded}D / {fails}F)'))
                        except Exception as e:
                            fails = fails + 1
                            pbar_video.update(1)
                            pbar_video.set_description_str(ascii_text(f'{desc} - ({downloaded}D / {fails}F / Cathastrophic Error)'))
                            pbar.write(f"    Error downloading: \"{counter}\" with error {e}")
                            if item['name'] not in failed:
                                failed[item['name']] = []
                            failed[item['name']].append({
                                'status' : FAILED_CODE,
                                'location': output_dir,
                                'error': str(e),
                                'counter': counter,
                            })
                            for future in futures:
                                if not future.done(): future.cancel()
                            quit = True
                total_fails = total_fails + fails
                total_downloads = total_downloads + downloaded
                if fails > 0: pbar.write(f"    {fails} downloads failed. Check failed.yml for more information")
                if downloaded > 0: pbar.write(f"    {downloaded} downloads successful. Check results.yml for more information")
                stats[title] = {
                    'downloaded': downloaded,
                    'failed': fails,
                    'remaining': local_submitted-downloaded,
                    'total': len(pl.videos),
                    'existing_files': len(existing_files),
                    'anomaly': anomaly
                }
                with open(playlist_data_file, 'w') as file:
                    yaml.dump(playlist_data, file)
                with open(playlist_data_archive, 'w') as file:
                    for url in playlist_data['downloaded']:
                        file.write("youtube " + url.replace("https://youtube.com/watch?v=", "") + "\n")
                if quit: 
                    DOWNLOAD = False
                    # break
        pbar.write(f"A total of {submitted} videos were submitted for downloading with {total_downloads}/{total_fails}/{submitted-total_downloads} downloads/fails/remaining")
        stats['global'] = {
            'downloaded': total_downloads,
            'failed': total_fails,
            'remaining': submitted-total_downloads,
            'total': total,
            'existing_files': total_existing,
            'anomaly': total_anomalies,
            'total_to_be_added': total_to_be_added
        }
        pbar.close()
    except KeyboardInterrupt:
        print("Exiting...")
        exit(0)
    with open(failed_file, 'w') as file:
        yaml.dump(failed, file)
    if len(failed) > 0:
        print("Some downloads failed. Check failed.yml for more information")
    with open(results_file, 'w') as file:
        yaml.dump(results, file)
    with open(SPECIAL_FILE, 'w') as file:
        yaml.dump(special_files, file)
    with open(STATS_FILE, 'w') as file:
        yaml.dump(stats, file)
    

if __name__ == '__main__': 
    downloader(DATA_FILE, IGNORE_FILE, FAILED_FILE, RESULTS_FILE, TMP_DIR)