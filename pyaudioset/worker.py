import tempfile

import pytube
import rq
import structlog
from pytube import YouTube

from pyaudioset import ffmpeg, utils

logger = structlog.get_logger(__name__)


def get_url(video_id):
    return f'https://youtube.com/watch?v={video_id}'


def download(clip, width=256, height=256, fps=16, sr=16000):
    job = rq.get_current_job()
    log = logger.bind(job_id=job.id, video_id=clip.video_id, start=clip.start, end=clip.end)

    url = get_url(clip.video_id)
    yt = YouTube(url)
    stream = yt.streams.get_by_itag(18)

    with tempfile.TemporaryDirectory() as temp_dir, \
            tempfile.NamedTemporaryFile('wb', suffix='.mp4') as conv_f:
        log.info("downloading", url=url, temp_dir=temp_dir)
        raw_path = stream.download(temp_dir, clip.video_id)

        logger.info("converting", in_path=raw_path, out_path=conv_f.name)
        ffmpeg.convert(raw_path, conv_f.name, clip.start, clip.end,
                       width=width, height=height, fps=fps, sr=sr)

        result_bytes = utils.path_to_bytes(conv_f.name)

    logger.info("done", size=len(result_bytes))

    return result_bytes
