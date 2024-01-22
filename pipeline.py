import pandas as pd
import json
import psycopg2
from function import loadInternalData,loadExternalData
from reviews import loadReviewData

#loading internal data
loadInternalData()

#loading external data
loadExternalData()
loadReviewData()

