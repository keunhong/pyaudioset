import csv
from dataclasses import dataclass


csv.register_dialect('audioset',
                     delimiter=',',
                     quoting=csv.QUOTE_ALL,
                     skipinitialspace=True)


@dataclass
class VideoClip(object):
    video_id: str
    start: float
    end: float
    labels: str

    def __init__(self, video_id, start, end, labels):
        self.video_id = video_id
        self.start = float(start)
        self.end = float(end)
        self.labels = labels

    def __repr__(self):
        return f'{self.video_id}_{self.start}_{self.end}'


def parse_csv(path):
    clips = []
    with open(path, 'r') as f:
        reader = csv.reader(f, dialect='audioset')
        for row in reader:
            if row[0].startswith('#'):
                continue
            clips.append(VideoClip(*row))

    return clips