# -*- coding: utf-8 -*-
"""citi hackathon

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1iP62UVOnM68E7a2apcCWAZN9gj17ocEm

    For running on a local machine, replace the csv directory with the local treated_data_10000 
"""


import csv
import pandas as pd
import numpy as np
from pycaret.utils import enable_colab 
enable_colab()

from google.colab import drive
drive.mount('/content/drive')

train = pd.read_csv('/content/drive/My Drive/citi_antispoof/Treated Data/10000/treated_data_10000.csv', nrows=5000)
print(train.head())

from pycaret.classification import *
clf1 = setup(data = train, target = 'y', session_id = 786,  silent = True)

# Commented out IPython magic to ensure Python compatibility.
# %%time 
# compare_models() #compares all the models from initial training of the data

"""MODEL CREATION:"""

lightgbm = create_model("lightgbm")

"""Creating Test data from Data 2:"""

test = pd.read_csv('/content/drive/My Drive/citi_antispoof/Treated Data/10000/treated_data_10000.csv', skiprows=range(1, 5000), nrows=50000)

"""Predict Holdout on Set"""

pred_holdout_lightgbm = predict_model(lightgbm)

"""Generating predictions

LIGHT GBM:
"""

predictions = predict_model(lightgbm, data=test)
predictions.head()

predictions.to_csv("lightgbm_predictions")

"""Plots"""



evaluate_model(lightgbm)

final_lightgbm = finalize_model(lightgbm)

import pickle

save_model(lightgbm,"treated_lightgbm_50000")