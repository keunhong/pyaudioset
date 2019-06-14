# pyaudioset

This package allows distributed downloading and processing of YouTube videos. Currently it only 
supports the AudioSet .csv format but it's trivial to extend.

There are also a lot of hardcoded variables like the width, height, fps etc. but those are also
trivial to change. See `manager.py` and `worker.py`

## Setup

First install pipenv if you don't have it. Follow the [official instructions](https://github.com/pypa/pipenv#installation).

Then clone this repository and sync.

```bash
git clone https://github.com/keunhong/pyaudioset.git
cd pyaudioset
pipenv sync
```

If the `pipenv sync` fails it might be due to the pytube git fork dependency. In that case just do

```bash
pipenv lock
pipenv sync
```

Now you also need to run redis somewhere. I recommend using Docker since it makes it simple.

```bash
docker run -d --name my-redis -p 6379:6379 redis
```


## Running some workers

Now clone the repository and follow the setup instructions (minus the redis part) on any machine you want to run workers on.

Then all you have to do is run

```bash
rq worker -u "redis://$REDIS_HOST:$REDIS_PORT"
```


## Creating jobs

The workers actually need jobs, and that's what the manager script does.

Download the [.csv files from Google](https://research.google.com/audioset/download.html) and put them somewhere e.g., in the `resources` directory.

```bash
python -m pyaudioset.manager \
    --redis-url redis://$REDIS_HOST:$REDIS_PORT'
    --csv $CSV_PATH \
    --save-dir $SAVE_DIR/eval \
    --log-path manager_eval.log \
    --max-jobs 64
```
