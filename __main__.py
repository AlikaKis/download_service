from .sw_shared.data.make_classification import MakeClassificationImageInfo, STATIONAR_PHOTO_KIND, PARKING_PHOTO_KIND
from .enums import PARKING_FILTER_TYPE, STATIONAR_FILTER_TYPE, DB_LOCATION_FILTER
from pymongo import MongoClient
from datetime import datetime, timedelta
import urllib3
from .stationar_db import StationarDB
from .parking_db import ParkingDB
from .file_saver import FileSaver
from .tar_gz_saver import TarGZSaver
import os
import asyncio
DEBUG = False
urllib3.disable_warnings()

DB_CONNECT='mongodb://a.kiseleva:hAEorHtoR1eN@mongo0-10g.local.traffic-view.com:27017,\
          mongo1-10g.local.traffic-view.com:27017,\
          mongo2-10g.local.traffic-view.com:27017/admin?readPreference=secondaryPreferred'
client = MongoClient(DB_CONNECT)

HOST = 'https://parking.traffic-view.com/Api/Photo/getPhoto?'


if __name__ == '__main__':
    
    STATIONAR_ENABLE = os.environ['STATIONAR_ENABLE']
    PARKING_ENABLE = os.environ['PARKING_ENABLE']
    SAVE_PERIOD = os.environ['SAVE_PERIOD']
    PARKING_FILTER_TYPE = PARKING_FILTER_TYPE[os.environ['PARKING_FILTER_TYPE']]
    STATIONAR_FILTER_TYPE = STATIONAR_FILTER_TYPE[os.environ['STATIONAR_FILTER_TYPE']]
    DB_LOCATION_FILTER = DB_LOCATION_FILTER[os.environ['DB_LOCATION_FILTER']]
    FILTER_LIST = os.environ['FILTER_LIST']
    LOCATION_FILTER_LIST = os.environ['LOCATION_FILTER_LIST']
    PARKING_SAVE_FULL_PATH = os.environ['PARKING_SAVE_FULL_PATH']
    PARKING_SAVE_CROP_PATH = os.environ['PARKING_SAVE_CROP_PATH']
    PARKING_SAVE_LP_PATH = os.environ['PARKING_SAVE_LP_PATH']
    STATIONAR_SAVE_FULL_PATH = os.environ['STATIONAR_SAVE_FULL_PATH']
    STATIONAR_SAVE_CROP_PATH = os.environ['STATIONAR_SAVE_CROP_PATH']
    
    SAVE_PATHS = [(PARKING_SAVE_FULL_PATH, PARKING_PHOTO_KIND.FULL), (PARKING_SAVE_CROP_PATH, PARKING_PHOTO_KIND.CROPPED_CAR), (PARKING_SAVE_LP_PATH, PARKING_PHOTO_KIND.LP), (STATIONAR_SAVE_FULL_PATH, STATIONAR_PHOTO_KIND.FULL), (STATIONAR_SAVE_CROP_PATH, STATIONAR_PHOTO_KIND.CROPPED_CAR)]
        
    if os.environ['REDUCE_TIMEDELTA'].find('H') > -1:
        reduce_timedelta = timedelta(hours = int(os.environ['REDUCE_TIMEDELTA'][:-1]))
    elif os.environ['REDUCE_TIMEDELTA'].find('MS') > -1:
        reduce_timedelta = timedelta(milliseconds = int(os.environ['REDUCE_TIMEDELTA'][:-2]))
    elif os.environ['REDUCE_TIMEDELTA'].find('D') > -1:
        reduce_timedelta = timedelta(days = int(os.environ['REDUCE_TIMEDELTA'][:-1]))
    elif os.environ['REDUCE_TIMEDELTA'].find('INF') > -1:
        reduce_timedelta = timedelta(seconds = 10e6)
    else:
        raise ValueError("Check params? Enter in '{number}H/MS/D or INF'")
     
    if SAVE_PERIOD.find('H') > -1:
        period_timedelta = timedelta(hours = int(SAVE_PERIOD[:-1]))  
    else:
        raise ValueError("Check params? Enter in '{number}H'")
    
    now_timedelta = timedelta(hours = 1)  
    while now_timedelta <= period_timedelta:
            start_date = datetime.now() - now_timedelta + timedelta(hours = 1)   
            end_date = start_date - timedelta(hours = 1)   
            if STATIONAR_ENABLE == 'True':
                stationar = StationarDB(HOST, client, STATIONAR_FILTER_TYPE, DB_LOCATION_FILTER, reduce_timedelta, FILTER_LIST, LOCATION_FILTER_LIST, start_date, end_date)
                stationar_save_generator  = stationar.get_results()

            if PARKING_ENABLE == 'True':
                parking = ParkingDB(HOST, client,  PARKING_FILTER_TYPE, reduce_timedelta, FILTER_LIST, start_date, end_date)
                parking_save_generator  = parking.get_results()

            if DEBUG:
                t = stationar_save_generator()
                z = parking_save_generator()
                save_stationar_generator = lambda : islice(t, 0, 40)
                save_parking_generator = lambda : islice(z, 0, 40)

            saver_list = []
            for path, photo_kind in SAVE_PATHS:   
                if path == "":
                    continue
                elif path.endswith('tar.gz'):
                    tar_path = f"{path[:-6]}{str(datetime.now())[:10]}_{str(datetime.now())[11:]}.tar.gz"
                    if not os.path.exists(tar_path):
                        saver_list.append(TarGZSaver(tar_path, photo_kind))
                    else:
                        raise FileExistsError ("This archive already exist")
                else:
                    os.makedirs(path, exist_ok=True)
                    saver_list.append(FileSaver(path, photo_kind))

            loop = asyncio.get_event_loop()
            groups = []
           
            for saver in saver_list:
                photo_kind = saver.image_type
                if photo_kind in STATIONAR_PHOTO_KIND:
                    groups.append(saver.process(stationar_save_generator()))
                elif photo_kind in PARKING_PHOTO_KIND:
                    groups.append(saver.process(parking_save_generator()))

            all_groups = asyncio.gather(*groups)

            results = loop.run_until_complete(all_groups)
            now_timedelta += timedelta(hours = 1)  
    loop.close()
