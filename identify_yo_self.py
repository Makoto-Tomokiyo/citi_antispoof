import csv
import pandas as pd
import numpy as np

#This just creates two csv files we can use to train the ANN
'''
test_orders_1 = pd.read_csv('data1.csv',
            header=0, 
            names=['index', 'time', 'orderType', 'symbol', 'User_ID', 'price_level',
                    'side', 'size', 'action', 'L1BidVolume', 'L1AskVolume', 'L2BidVolume',
                    'L2AskVolume', 'L3BidVolume', 'L3AskVolume', 'y'])

yValues1 = test_orders_1['y']
yValues1.to_csv("yValues1", index = False)
test_orders_1 = test_orders_1.drop(['y'], axis=1)
test_orders_1.to_csv("test_orders_1", index = False)
'''