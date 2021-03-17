from dataclasses import dataclass
from .sw_shared.data.make_classification import MakeClassificationImageInfo
@dataclass
class DBResults:
    fixation : MakeClassificationImageInfo
    filename : str
    url_params : dict
    host : str
