docker build --no-cache -t recognizer_upload ~/little/faasflow/benchmark/illgal_recognizer/upload
docker build --no-cache -t recognizer_adult ~/little/faasflow/benchmark/illgal_recognizer/adult_detector
docker build --no-cache -t recognizer_violence ~/little/faasflow/benchmark/illgal_recognizer/violence_detector
docker build --no-cache -t recognizer_mosaic ~/little/faasflow/benchmark/illgal_recognizer/mosaic
docker build --no-cache -t recognizer_extract ~/little/faasflow/benchmark/illgal_recognizer/extract
docker build --no-cache -t recognizer_translate ~/little/faasflow/benchmark/illgal_recognizer/translate
docker build --no-cache -t recognizer_word_censor ~/little/faasflow/benchmark/illgal_recognizer/word_censor