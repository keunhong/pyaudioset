import argparse
import logging
import time
from pathlib import Path

import rq
import structlog as structlog
from redis import Redis

from pyaudioset import worker
from pyaudioset import audioset

logger = structlog.get_logger(__name__)


parser = argparse.ArgumentParser()
parser.add_argument('--redis-url', default='redis://drell.cs.washington.edu:6379')
parser.add_argument('--csv', type=Path)
parser.add_argument('--ttl', type=int, default=120)
parser.add_argument('--save-dir', type=Path, required=True)
parser.add_argument('--log-path', type=Path)
args = parser.parse_args()


def get_clip_path(clip):
    return args.save_dir / f'{clip!r}.mp4'


def get_next_clip(clip_iter):
    while True:
        clip = next(clip_iter)
        if get_clip_path(clip).exists():
            continue
        return clip


def handle_finished_job(job):
    clip = job.args[0]
    log = logger.bind(job_id=job.get_id(), video_id=clip.video_id)
    payload = job.result
    assert type(payload) == bytes

    path = get_clip_path(clip)
    log.info("saving video", num_bytes=len(payload), path=path)

    with open(path, 'wb') as f:
        f.write(payload)

    job.delete()


def handle_failed_job(job):
    log = logger.bind(job_id=job.get_id())
    job.refresh()
    log.error("job failed", exc=job.exc_info)


def process_finished_jobs(jobs: rq.job.Job):
    unfinished = []
    for job in jobs:
        log = logger.bind(job_id=job.get_id())
        if job.is_finished:
            handle_finished_job(job)
        elif job.is_failed:
            handle_failed_job(job)
        else:
            unfinished.append(job)

    return unfinished


def loop(q, clips, max_jobs=10):
    clip_iter = iter(clips)

    jobs = []

    while True:
        jobs = process_finished_jobs(jobs)

        if len(jobs) < max_jobs:
            clip = get_next_clip(clip_iter)
            job = q.enqueue(worker.download,
                            args=(clip,),
                            result_ttl=args.ttl)
            jobs.append(job)
            continue

        time.sleep(2)


def get_unprocessed_clips(r, clips):
    return [c for c in clips if r.get(c.video_id) != 'done']


def main():
    if args.log_path:
        handler = logging.FileHandler(args.log_path)
        logging.getLogger('').addHandler(handler)

    clips = audioset.parse_csv(args.csv)
    logger.info("loaded csv", num_clips=len(clips))

    r = Redis.from_url(args.redis_url)
    q = rq.Queue(connection=r)

    loop(q, clips)


if __name__ == '__main__':
    main()
