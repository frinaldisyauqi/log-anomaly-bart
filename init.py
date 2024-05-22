import os
from urllib import request
from tqdm import tqdm


path = "datasets"
filename = {"hdfs" : "HDFS_1.tar.gz", "bgl" : "BGL.tar.gz", "tbird" : "tbird2.gz"}
url ={"hdfs" : "https://zenodo.org/record/3227177/files/" + filename["hdfs"] +"?download=1",
      "bgl" : "https://zenodo.org/record/3227177/files/" + filename["bgl"] + "?download=1",
      "tbird" : "http://0b4af6cdc2f0c5998459-c0245c5c937c5dedcca3f1764ecc9b2f.r43.cf2.rackcdn.com/hpc4/" + filename["tbird"]} 

try:
    os.mkdir(path)
except OSError as error:
    print(path + " : Directory already exist!")
    pass

class DownloadProgressBar(tqdm):
    def update_to(self, b=1, bsize=1, tsize=None):
        if tsize is not None:
            self.total = tsize
        self.update(b * bsize - self.n)

def download_url(url, output_path, file):
    with DownloadProgressBar(unit='B', unit_scale=True,
                             miniters=1, desc=file) as t:
        request.urlretrieve(url, filename=output_path + "/" + file, reporthook=t.update_to)

for key, value in filename.items():
    if os.path.exists(path+"/"+value):
        print(f"\n{value} is already exist!")
    else : 
        print(f"\ndownloading {value}...")
        download_url(url[key], path, value)
