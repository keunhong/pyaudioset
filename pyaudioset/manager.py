import argparse
import logging
import time
from pathlib import Path

import pytube.exceptions
import rq
import structlog as structlog
from redis import Redis
from rq.exceptions import NoSuchJobError

from pyaudioset import worker
from pyaudioset import audioset

logger = structlog.get_logger(__name__)


parser = argparse.ArgumentParser()
parser.add_argument('--redis-url', default='redis://drell.cs.washington.edu:6379')
parser.add_argument('--csv', type=Path)
parser.add_argument('--ttl', type=int, default=120)
parser.add_argument('--queue', type=str, default='default')
parser.add_argument('--save-dir', type=Path, required=True)
parser.add_argument('--log-path', type=Path)
parser.add_argument('--max-jobs', type=int, default=32)
args = parser.parse_args()


def get_clip_path(clip):
    return args.save_dir / f'{clip!r}.mp4'


def get_next_clip(clip_iter):
    num_skipped = 0
    while True:
        clip = next(clip_iter)
        if get_clip_path(clip).exists():
            num_skipped += 1
            continue
        return clip, num_skipped


def handle_finished_job(job):
    clip = job.args[0]
    log = logger.bind(job_id=job.get_id(), video_id=clip.video_id,
                      hostname=job.meta.get('hostname'))
    payload = job.result
    assert type(payload) == bytes

    path = get_clip_path(clip)
    log.info("saving video", num_bytes=len(payload), path=path)

    with open(path, 'wb') as f:
        f.write(payload)

    job.delete()


def handle_failed_job(job):
    log = logger.bind(job_id=job.get_id(), hostname=job.meta.get('hostname'))
    if job.exc_info and 'pytube.exceptions.VideoUnavailable' in job.exc_info:
        log.error("video unavailable")
    else:
        log.error("job failed", exc=job.exc_info)


def process_finished_jobs(jobs: rq.job.Job):
    num_finished = 0
    num_failed = 0
    unfinished = []
    for job in jobs:
        try:
            job.refresh()
        except NoSuchJobError:
            logger.error("no such job?", job_id=job.get_id())
            continue

        if job.is_finished:
            handle_finished_job(job)
            num_finished += 1
        elif job.is_failed:
            handle_failed_job(job)
            num_failed += 1
        else:
            unfinished.append(job)

    return unfinished, num_finished, num_failed


def loop(q, clips, max_jobs=10):
    clip_iter = iter(clips)
    total_clips = len(clips)
    total_finished = 0
    total_failed = 0

    jobs = []

    while True:
        jobs, num_finished, num_failed = process_finished_jobs(jobs)
        total_finished += num_finished
        total_failed += num_failed

        if len(jobs) < max_jobs:
            try:
                clip, num_skipped = get_next_clip(clip_iter)
            except StopIteration:
                break

            total_finished += num_skipped
            job = q.enqueue(worker.download,
                            args=(clip,),
                            result_ttl=args.ttl)
            jobs.append(job)
            logger.info("queued job", job_id=job.get_id(), video_id=clip.video_id,
                        total=total_clips,
                        finished=total_finished,
                        failed=total_failed,
                        percent_done=f'{(total_failed+total_finished)/total_clips*100:.02f}%')
            continue

        time.sleep(2)

    logger.info("done!",
                total=total_clips,
                finished=total_finished,
                failed=total_failed,
                percent_done=f'{(total_failed+total_finished)/total_clips*100:.02f}%')


def get_unprocessed_clips(r, clips):
    return [c for c in clips if r.get(c.video_id) != 'done']


def main():
    if args.log_path:
        handler = logging.FileHandler(args.log_path)
        logging.getLogger('').addHandler(handler)

    clips = audioset.parse_csv(args.csv)
    logger.info("loaded csv", num_clips=len(clips))

    r = Redis.from_url(args.redis_url)
    q = rq.Queue(name=args.queue, connection=r)

    loop(q, clips, args.max_jobs)


if __name__ == '__main__':
    main()
