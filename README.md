# yt-downloader

From a given data file, download and track YouTube playlists and channels with `yt-dlp`.

Given a `data.yml` describing playlists/channels, the script downloads new videos, keeps a
per-playlist archive so nothing is fetched twice, reports stats, and supports a "watch +
triage" flow where new videos in flagged playlists are queued for review instead of being
downloaded automatically.

## Install

```bash
python3 -m pip install -r requirements.txt
```

Keep `yt-dlp` current — YouTube's bot detection changes constantly and a stale `yt-dlp` is
the most common reason downloads suddenly fail. Prefer the **nightly** channel:

```bash
python3 -m pip install -U --pre "yt-dlp[default]"
```

The script prints a warning at startup if its installed `yt-dlp` looks more than ~30 days old.

## PO-token provider (required)

As of 2026 most YouTube downloads require a Proof-of-Origin (PO) token. The recommended way
to supply one is the [bgutil-ytdlp-pot-provider](https://github.com/Brainicism/bgutil-ytdlp-pot-provider)
plugin (maintained by a yt-dlp maintainer). One-time setup:

```bash
# Replace `~` with `$USERPROFILE` on Windows.
cd ~
# Replace 0.7.3 with the latest release (or whichever matches the plugin you install).
git clone --single-branch --branch 0.7.3 https://github.com/Brainicism/bgutil-ytdlp-pot-provider.git
cd bgutil-ytdlp-pot-provider/server/
yarn install --frozen-lockfile
npx tsc
python3 -m pip install -U bgutil-ytdlp-pot-provider
```

## Cookies & account safety

This script is **cookieless by design** — it authenticates through the PO-token provider
above, not account cookies. Pointing an automated, high-volume downloader at a logged-in
account is exactly the pattern YouTube's bot detection flags, and the
[yt-dlp wiki](https://github.com/yt-dlp/yt-dlp/wiki/Extractors) warns the account can be
banned. `cookies.txt` is no longer read; you can safely delete it (it's already gitignored).

## Client selection

The script no longer hardcodes a `player_client` list. `yt-dlp` picks the best clients itself
(and drops ones that can't use your cookies), which tracks YouTube's changes far better than a
fixed list — the old `ios`/`web` pair is now a liability (`ios` ignores account cookies and
needs PO tokens). Override only if you must:

```bash
YT_PLAYER_CLIENT="tv,web_safari" python3 ytdlp.py -sd
```

## Usage

```bash
python3 ytdlp.py -sd      # check stats, then download
python3 ytdlp.py -sf      # check stats only, no changes
python3 ytdlp.py --triage # interactively triage watched-playlist videos: [d]ownload/[i]gnore/[s]kip
python3 ytdlp.py --review # write review.yml for batch triage, applied on the next run
python3 ytdlp.py -h       # full option list
```

At least one action (`-s`, `-d`, `--triage`, `--review`) is required; running with no action
now errors instead of silently doing nothing.

### Watched playlists

Add `watch: true` to a playlist in `data.yml`. New videos there are collected as **pending**
for review instead of downloaded automatically (other playlists still download normally).
Triage with `--triage` or `--review`; approved videos download on your next `-sd` run.
