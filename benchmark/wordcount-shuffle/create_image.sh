docker build --no-cache -t wc_start ~/little/faasflow/benchmark/wordcount/start
docker build --no-cache -t wc_count ~/little/faasflow/benchmark/wordcount/count
docker build --no-cache -t wc_merge ~/little/faasflow/benchmark/wordcount/merge