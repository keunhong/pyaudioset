import tempfile

import rq
import socket
import structlog
import urllib.error
from pytube import YouTube

from pyaudioset import ffmpeg, utils

logger = structlog.get_logger(__name__)


def get_url(video_id):
    return f'https://youtube.com/watch?v={video_id}'


def get_hostname():
    return socket.gethostname().split('.')[0]


def _try_download_streams(streams, save_dir):
    path = None
    for stream in streams:
        logger.info("trying to download...", stream=stream)
        try:
            path = stream.download(save_dir)
        except urllib.error.HTTPError as e:
            logger.error("http error", exc_info=e)
            continue
        else:
            break

    return path


def _download_progressive(yt, save_dir):
    logger.info("downloading stream", video_id=yt.video_id, type='progressive')
    streams = yt.streams.filter(progressive=True, file_extension='mp4').all()

    path = _try_download_streams(streams, save_dir)

    return path, path


def _download_dash(yt, save_dir):
    log = logger.bind(video_id=yt.video_id, type='dash')
    log.info("downloading video stream")
    video_streams = yt.streams.filter(adaptive=True, only_video=True).all()
    video_path = _try_download_streams(video_streams, save_dir)

    log.info("downloading audio stream")
    audio_streams = yt.streams.filter(adaptive=True, only_audio=True).all()
    audio_path = _try_download_streams(audio_streams, save_dir)
    return video_path, audio_path


def _try_download(yt, save_dir):
    video_path = audio_path = _try_download_streams([yt.streams.get_by_itag(18)], save_dir)
    if video_path is None:
        video_path, audio_path = _download_progressive(yt, save_dir)
    # if video_path is None:
    #     video_path, audio_path = _download_dash(yt, save_dir)
    if video_path is None:
        raise RuntimeError(f"Could not download {yt.video_id}")

    return video_path, audio_path


def download(clip, width=256, height=256, fps=16, sr=16000):
    job = rq.get_current_job()
    job.meta['hostname'] = get_hostname()
    job.save_meta()

    log = logger.bind(job_id=job.id, video_id=clip.video_id, start=clip.start, end=clip.end)
    url = get_url(clip.video_id)
    yt = YouTube(url)

    with tempfile.TemporaryDirectory() as temp_dir, \
            tempfile.NamedTemporaryFile('wb', suffix='.mp4') as conv_f:
        video_path, audio_path = _try_download(yt, temp_dir)

        log.info("converting", video_path=video_path, audio_path=audio_path,
                    out_path=conv_f.name)
        ffmpeg.convert(video_path, audio_path, conv_f.name, clip.start, clip.end,
                       width=width, height=height, fps=fps, sr=sr)

        result_bytes = utils.path_to_bytes(conv_f.name)

    log.info("done", size=len(result_bytes))

    return result_bytes
