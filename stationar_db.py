from .db_results import DBResults
from .enums import PARKING_FILTER_TYPE, STATIONAR_FILTER_TYPE, DB_LOCATION_FILTER
from .sw_shared.data.make_classification import MakeClassificationImageInfo, STATIONAR_PHOTO_KIND, PARKING_PHOTO_KIND
from datetime import datetime, timedelta, time, timezone
import os
import json
from tqdm.auto import tqdm
import random

class StationarDB():
    def __init__(self, HOST, client, db_filter_type, db_location_filter, reduce_timedelta, FILTER_LIST, LOCATION_FILTER_LIST, start_date, end_date):
        self.host = HOST
        self.db_filter_type = db_filter_type
        self.location_filter_list = LOCATION_FILTER_LIST
        self.db_location_filter = db_location_filter
        self.reduce_timedelta = reduce_timedelta
        
        if self.db_filter_type:
            self.filter_list = FILTER_LIST
            if not os.path.exists(self.filter_list):
                raise ValueError("Uncorrect filter_list file")
            with open(self.filter_list, 'r') as f:
                self.lp_filter_dict = json.load(f)

        self.start_date = start_date
        self.end_date = end_date
        
        self.query = [self.__build_query1__(),
                      self.__build_query2__(),
                      self.__build_query3__()]
        print(self.query)
        
        self.cursor = client['tms-raw-RU-vehicles']['VbnYear'].aggregate(self.query)
    
    def __build_query1__(self):
        query = {
            '$match': {
                'Y' : datetime.today().year,
                'L' : {'$gte' : self.end_date, '$lte' : self.start_date }
            }
        }
        
        reversed_lp_list = [t[::-1] for t in self.lp_filter_dict]    
        if self.db_filter_type  == STATIONAR_FILTER_TYPE.WHITE_LIST:
            query['$match']['R'] = {'$in' : reversed_lp_list}
            
        if self.db_filter_type == STATIONAR_FILTER_TYPE.BLACK_LIST:
            query['$match']['R'] = {'$nin' : reversed_lp_list}      
        
        return query
    
    def __build_query2__(self):
        sw_timestamp_start_date  = StationarDB.__date_to_swtimestamp__(self.start_date)
        sw_timestamp_end_date = StationarDB.__date_to_swtimestamp__(self.end_date)
        if self.db_location_filter == DB_LOCATION_FILTER.GOOD_LOCATION:   
            with open(self.location_filter_list, 'r') as f:
                self.location_list = json.load(f)
            cond = [{'$gte' : ["$$item.T", sw_timestamp_end_date]},
                    {'$lte' : ["$$item.T", sw_timestamp_start_date]},
                    {'$in' : ["$$item.L", self.location_list]}]
        elif self.db_location_filter == DB_LOCATION_FILTER.NONE:
            cond = [{'$gte' : ["$$item.T", sw_timestamp_end_date]},
                   {'$lte' : ["$$item.T", sw_timestamp_start_date]}]
        query = {
            '$project': {
                'I': {
                    '$filter': {
                        'input': '$I',
                        'as' : 'item',
                        'cond' : {'$and': cond}
                   }
                },
                'R': 1
            }
        }
        return query

    def __build_query3__(self):
        query = {
            '$match': {
                "I.0": {'$exists': 'true'}
            }
        }
        return query 
    
    @staticmethod
    def __date_to_swtimestamp__(date):
        return date.timestamp() - datetime(2000, 1, 1).timestamp()
    
    @staticmethod
    def __swtimestamp_to_date__(timestamp):
        return datetime.fromtimestamp(datetime(2000, 1, 1).timestamp() + timestamp)
    
    @staticmethod
    def __get_filename__(fixation):    
        date = str(fixation.date).replace(':', '-')
        filename = f"{fixation.lp}-{date[:10]}-{date[11:23]}.000-L-{fixation.location}"\
                   f"-D-{fixation.device_id}-Q-{fixation.fixation_id}-K-{fixation.photo_kind.value}.jpg"
        return filename 
    
    
    @staticmethod
    def __get_url_params__(fixation):
        
        url_params = {
                 'locationId' : fixation.location,
                 'time' : f"{str(fixation.date)[:10]}T{str(fixation.date)[11:23]}.000Z",
                 'cameraId' : 0,
                 'sequenceId' : fixation.fixation_id,
                 'photoKindId' : fixation.photo_kind
              }
        return url_params
    
    def __get_results__(self, cursor):
        data_list = []
        for doc in tqdm(cursor):
            reversed_lp = doc['R']
            for fixation in doc['I']:
                sw_timestamp = fixation['T']
                info = MakeClassificationImageInfo(lp = reversed_lp[::-1],
                                                   date = StationarDB.__swtimestamp_to_date__(sw_timestamp),
                                                   device_id = fixation['D'],
                                                   location = fixation['L'],
                                                   fixation_id = fixation['Q'],
                                                   photo_kind = None)
                
                if not info.location in self.location_list:
                    continue
                   
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
                for photo_kind in STATIONAR_PHOTO_KIND:
                    fixation.photo_kind = photo_kind
                    
                    result = DBResults(
                                              fixation=fixation,
                                              filename=StationarDB.__get_filename__(fixation),
                                              url_params=StationarDB.__get_url_params__(fixation),
                                              host=self.host)
                    yield result
        
    def get_results(self):
        data_list = self.__get_results__(self.cursor)
        return lambda : (t for t in self.__reduce__(data_list))
