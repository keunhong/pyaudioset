import subprocess
import typing
from datetime import timedelta
from numbers import Number
from pathlib import Path


def _exec(command):
    result = subprocess.run(command, capture_output=False, check=True)
    return result.stdout


def convert_command(in_path: Path, out_path: Path,
                    start: typing.Union[timedelta, Number],
                    end: typing.Union[timedelta, Number],
                    width=-2,
                    height=-2,
                    fps=16,
                    sr=16000):
    out_path = Path(out_path)
    if isinstance(start, Number):
        start = timedelta(seconds=start)
    if isinstance(end, Number):
        end = timedelta(seconds=end)

    duration = (end - start).total_seconds()

    supported_exts = {'.mp4', '.jpg'}
    if out_path.suffix not in supported_exts:
        raise ValueError(f'{out_path.suffix} not is not a supported format.')

    output_args = {
        '.mp4': ('-c:v', 'libx264', '-c:a', 'aac'),
        '.jpg': (),
    }[out_path.suffix]

    command = [
        'ffmpeg',
        '-ss', str(start),
        '-t', str(duration),
        '-i', str(in_path),
        '-r', str(fps),
        '-ar', str(sr),
        '-vf', f'fps={fps},scale={width}:{height}',
        '-crf', '18',
        '-map_metadata', '-1',
        *output_args,
        str(out_path),
        '-y',
    ]

    return command


def convert(*args, **kwargs):
    return _exec(convert_command(*args, **kwargs))
