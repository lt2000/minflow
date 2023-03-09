# install docker
sudo apt-get update
sudo apt-get install -y \
    ca-certificates \
    curl \
    gnupg \
    lsb-release
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io
sudo apt-get install wondershaper
# install and initialize couchdb
docker pull couchdb
docker run -itd -p 5984:5984 -e COUCHDB_USER=openwhisk -e COUCHDB_PASSWORD=openwhisk --name couchdb couchdb
pip install -r requirements.txt
python3 couchdb_starter.py
# install redis
docker pull redis
docker run -itd -p 8002:6379 --name redis redis
# run grouping for all benchmarks
cd ../src/grouping
# python3 grouping.py video illgal_recognizer fileprocessing wordcount cycles epigenomics genome soykb
python3 grouping.py wordcount