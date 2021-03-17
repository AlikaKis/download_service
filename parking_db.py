import sys
sys.path.append('/workspace/')
from .sw_shared.data.make_classification import MakeClassificationImageInfo, STATIONAR_PHOTO_KIND, PARKING_PHOTO_KIND
from .enums import PARKING_FILTER_TYPE, STATIONAR_FILTER_TYPE, DB_LOCATION_FILTER
from .db_results import DBResults
from bson.tz_util import FixedOffset
from datetime import datetime, timedelta, time, timezone
import os
import json
from tqdm.auto import tqdm
import random

class ParkingDB():
    def __init__(self, HOST, client, db_filter_type, reduce_timedelta, FILTER_LIST, start_date, end_date):
            self.host = HOST
            self.db_filter_type = db_filter_type
            self.reduce_timedelta = reduce_timedelta

            if self.db_filter_type:
                self.filter_list = FILTER_LIST
                if not os.path.exists(self.filter_list):
                    raise ValueError()
                with open(self.filter_list, 'r') as f:
                    self.lp_filter_list = list(json.load(f).keys())
                   
            self.start_date = start_date
            self.end_date = end_date  
            self.query = self.__build_query1__()
            self.cursor = client['tms-parking-RU']['FixationUnit'].find(self.query)
            
    def __build_query1__(self):
        query = {
            '_id.T' : { u"$gte": self.end_date.replace(tzinfo = FixedOffset(180, "+0300")),
            u"$lte": self.start_date.replace(tzinfo = FixedOffset(180, "+0300"))
                      }
        }
        if self.db_filter_type == PARKING_FILTER_TYPE.WHITE_LIST:
            query['VNN'] = {'$in' : self.lp_filter_list}
        elif self.db_filter_type == PARKING_FILTER_TYPE.BLACK_LIST:
            query['VNN'] = {'$nin' : self.lp_filter_list}
        return query
    
    @staticmethod
    def __get_filename__(fixation):    
        date = str(fixation.date).replace(':', '-')
        filename = f"{fixation.lp}-{date[:10]}-{date[11:23]}-D-{fixation.device_id}"\
                   f"-C-{fixation.camera_id}-Q-{fixation.fixation_id}-K-{fixation.photo_kind.value}"\
                   f"-TBLR_N-{fixation.lp_bbox['RectTop']}-"\
                   f"{fixation.lp_bbox['RectBottom']}-{fixation.lp_bbox['RectLeft']}-"\
                   f"{fixation.lp_bbox['RectRight']}-"\
                   f"TBLR_A-{fixation.car_bbox['RectTop']}-"\
                   f"{fixation. car_bbox['RectBottom']}-{fixation.car_bbox['RectLeft']}-"\
                   f"{fixation.lp_bbox['RectRight']}.jpg"
        return filename 
   
    @staticmethod
    def __get_url_params__(fixation):
        
        url_params = {
                 'deviceId' : fixation.device_id,
                 'time' : f"{str(fixation.date)[:10]}T{str(fixation.date)[11:23]}Z",
                 'cameraId' : fixation.camera_id,
                 'sequenceId' : fixation.fixation_id,
                 'photoKindId' : fixation.photo_kind.value,
                 'photoIndex' : 0
              }
        return url_params
    
    def __get_results__(self, cursor):
        data_list = []
        for doc in tqdm(cursor):
            if not "DLI" in doc:
                continue
            if not "MDJ" in doc["DLI"]["violation"]:
                continue
            value = doc["DLI"]["violation"]['MDJ']
            info = MakeClassificationImageInfo(
                                               lp_bbox = json.loads(value)['ImagesMessages'][1],
                                               car_bbox = json.loads(value)['ImagesMessages'][2],
                                               date = doc["_id"]["T"],
                                               lp = str(doc["VNN"]),
                                               camera_id = doc["_id"]["C"],
                                               device_id = str(doc["_id"]["D"]),
                                               fixation_id = str(doc["_id"]["Q"]),
                                               photo_kind = None)
                                            
            data_list.append(info)
        return data_list
    
    def __reduce__(self, data_list):
        for fixation_identifier in tqdm(set([(info.lp, info.device_id) for info in data_list])):
            identical_fixations = [info for info in data_list if (info.lp, info.device_id) == fixation_identifier]
            random.shuffle(identical_fixations)
            saved_datetimes = []
            for fixation in identical_fixations:
                if [date for date in saved_datetimes if abs(date - fixation.date) < self.reduce_timedelta]:
                    continue
                
                saved_datetimes.append(fixation.date)
                for photo_kind in PARKING_PHOTO_KIND:
                    fixation.photo_kind = photo_kind
                    
                    result = DBResults(
                                              fixation=fixation,
                                              filename=ParkingDB.__get_filename__(fixation),
                                              url_params=ParkingDB.__get_url_params__(fixation),
                                              host=self.host)
                    yield result
        
    def get_results(self):
        data_list = self.__get_results__(self.cursor)
        return lambda : (t for t in self.__reduce__(data_list))
