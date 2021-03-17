from .sw_shared.data.make_classification import MakeClassificationImageInfo, STATIONAR_PHOTO_KIND, PARKING_PHOTO_KIND
import requests
import os
import asyncio

class FileSaver:
    def __init__(self, root_path, image_type):
        self.root_path = root_path
        os.makedirs(root_path, exist_ok=True)
        self.image_type = image_type
        
    async def save(self, db_result):
        query = requests.get(db_result.host, params=db_result.url_params, stream=True,verify=False)
        if query.status_code != 200:
            return -1
        
        with open(os.path.join(self.root_path, db_result.filename), 'wb') as f:
            f.write(query.content)
            
        return query.elapsed.total_seconds()
    
    def process(self, data_generator):
        data_generator = filter(lambda db_result : db_result.fixation.photo_kind == self.image_type, data_generator)
        group = asyncio.gather(*[self.save(db_result) for db_result in data_generator])
        return group
