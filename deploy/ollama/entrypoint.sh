#!/bin/sh
# Start the server, wait until it answers, pull the model if it is not on the disk yet, then stay in the foreground.
ollama serve &
pid=$!
until ollama list >/dev/null 2>&1; do sleep 1; done
MODEL="${OVERTURN_MODEL:-qwen2.5:7b-instruct}"
ollama list | grep -q "${MODEL%%:*}" || ollama pull "$MODEL"
wait $pid
