from .sw_shared.data.make_classification import MakeClassificationImageInfo, STATIONAR_PHOTO_KIND, PARKING_PHOTO_KIND
import requests
import os
import tarfile
from io import BytesIO
import asyncio

class TarGZSaver:
    def __init__(self, tar_path, image_type):
        self.tar_path = tar_path
        self.image_type = image_type
        self.tarfile = tarfile.open(tar_path, "w:gz")
    
    def __del__(self):
        self.tarfile.close()
    
    async def save(self, db_result):
        query = requests.get(db_result.host, params=db_result.url_params, stream=True, verify=False)
        if query.status_code != 200:
            return -1
        
        tarinfo = tarfile.TarInfo(name=db_result.filename)
        tarinfo.size = len(query.content)
        self.tarfile.addfile(tarinfo, BytesIO(query.content))
        return query.elapsed.total_seconds()
    
    def process(self, data_generator):
        data_generator = filter( lambda db_result : db_result.fixation.photo_kind == self.image_type, data_generator)
        group = asyncio.gather(*[self.save(db_result) for db_result in data_generator])
        return group
