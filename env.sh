#!/bin/bash

export PYTHONPATH=$(pwd):$PYTHONPATH

ulimit -n 100000

source $(dirname $(poetry run which python))/activate

