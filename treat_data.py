import csv
import pandas as pd
import time

#Create our main dataframe
start = time.time()
totalRows = 500000
data1_treated = train = pd.read_csv('data1.csv',
            header=0, 
            names=['index', 'time', 'orderType', 'symbol', 'User_ID', 'price_level',
                    'side', 'size', 'action', 'L1BidVolume', 'L1AskVolume', 'L2BidVolume',
                    'L2AskVolume', 'L3BidVolume', 'L3AskVolume', 'y'], nrows = totalRows)

data1_treated = data1_treated.sort_values(['User_ID','symbol'],ignore_index=True)

spoof_session_len = 1000 # in milliseconds
#returns appendDataFrame with three values, VolOfFirstType,VolOfSecondType,DifOfMeans
def createAppendDataFrame(data1_treated):
        appendDataFrame = pd.DataFrame(columns = ["VolOfFirstType", "VolOfSecondType", "DifOfMeans"])
        return appendDataFrame

#adds the 3 dimension tuple to appendDataFram, for runCount many rows, returns the new appendDataFrame
def addToAppendDataFrame(thisRunsValues, appendDataFrame,runCount):
        df = appendDataFrame
        new_row = {'VolOfFirstType':thisRunsValues[0], 'VolOfSecondType':thisRunsValues[1], 'DifOfMeans':thisRunsValues[2]}
        for i in range(runCount):
                df = df.append(new_row, ignore_index=True)
        return df



#converts the strings xs,s,m,l,xl into integers
def sizeToInt(volume):
        if(volume == 'xs'): return 1
        elif(volume == 's'): return 2
        elif(volume == 'm'): return 3
        elif(volume == 'l'): return 4
        elif(volume == 'xl'): return 5

#returns true if on new Symbol, new ID, or if big jump in prevTime
# row: str[]; currentRunSymbol: str; currentRunID: str
# prevTime should be an int (milliseconds)
def isNewRun(row, currentRunSymbol, currentRunID, prevTime):
        if (row['symbol'] != currentRunSymbol):
                #print("new symbol")
                return True
        if (row['User_ID'] != currentRunID):
                #print("new User_ID")
                return True
        if (prevTime + spoof_session_len <= time_to_msecs(row['time'])):
                #print("over time")
                #print("prev time",prevTime)
                #print("current time",time_to_msecs(row['time']))
                return True
        return False


def time_to_msecs(s): # TESTED
        s = s.split(" ")[1]
        s = s.split(":") # should be [int, int, float]

        hrs = int(s[0])
        mins = int(s[1])
        msecs = int(1000 * float(s[2]))
        
        mins += hrs * 60
        msecs += (mins * 60 * 1000)

        return msecs

def appendAuxiliaryValues(data1_treated):
        #generates 3 column DataFrame that will be added later on
        #to the main dataFrame
        appendDataFrame = createAppendDataFrame(data1_treated)
        
        #Variables for performing runs
        currentRunSymbol = None
        currentRunID = None
        firstSide = None
        runCount = 0
        cancelCount = 0
        prevTime = 0 #in mseconds

        #variables for the appendDataFrame
        #run count and cancel count used as well to determine mean_times
        thisRunsValues = [0,0,0]
        runs_orders = 0
        runs_cancel_time = 0

        totalRunCount = 0
        
        #Loops through each run, and then appends the data to the appendDataFrame
        for index,row in data1_treated.iterrows():
                orderTime = time_to_msecs(row['time'])
                orderSize = sizeToInt(row['size'])
                
                if(currentRunSymbol == None): #If this is the first run
                        currentRunSymbol = row['symbol']
                        currentRunID = row['User_ID']
                        firstSide = row['side']
                        runCount = 1
                        prevTime = orderTime
                        thisRunsValues = [orderSize,0,0]
                        runs_order_time  = orderTime
                        if(row['action'] == 'C'):
                                runs_cancel_time = orderTime
                                cancelCount = 1
                        else:
                                runs_cancel_time = 0
                                cancelCount = 0
                        
                elif isNewRun(row, currentRunSymbol, currentRunID, prevTime) or totalRunCount == totalRows-1:
                        #calculate the mean_order_difference and update appendValues
                        if(cancelCount != 0 and runCount != 0):
                                mean_order_difference = runs_cancel_time/cancelCount - runs_order_time/runCount
                        else:
                                mean_order_difference == 0
                        thisRunsValues[2] = mean_order_difference
 
                        #append the thisRunsValues to the appendDataFrame for runCount iterations
                        appendDataFrame = addToAppendDataFrame(thisRunsValues, appendDataFrame,runCount)
                        #print('mean_order_difference:',mean_order_difference)
                        #print('thisRunsValues:' + str(thisRunsValues) + ',' + 'count:' + str(totalRunCount))
                        #print('****************************************************************')
                        #updates the variables to start a new run
                        currentRunSymbol = row['symbol']
                        currentRunID = row['User_ID']
                        firstSide = row['side']
                        runCount = 1
                        prevTime = orderTime
                        thisRunsValues = [orderSize,0,0]
                        runs_order_time = orderTime
                        if(row['action'] == 'C'):
                                cancelCount = 1
                                runs_cancel_time= orderTime
                        else:
                                runs_cancel_time = 0
                                cancelCount = 0

                else:
                        #if order is same side as first order, update thisRunsValues[0]
                        if (row['side'] == firstSide):
                                thisRunsValues[0] += orderSize
                        #else update thisRunsValues[1], aka volume of opposite side
                        else:
                                thisRunsValues[1] += orderSize         

                        #update runs_order_time and runs_cancel_time
                        if(row['action'] == 'C'): #if Cancel, update runs_cancel_time
                                cancelCount += 1
                                runs_cancel_time += orderTime
                        
                        #regardless if its cancel or regular, still need to update runs_order_time
                        runCount += 1
                        runs_order_time += orderTime
                        prevTime = orderTime

                #print('*' + 'thisRunsValues:' + str(thisRunsValues) + '    ' + 'totalCount:' + str(totalRunCount))
                #print('runs_cancel_time:' + str(runs_cancel_time) + '    ' + 'cancelCount:' + str(cancelCount))
                #print('runs_order_time:' + str(runs_order_time) + '    ' +'runCount:' + str(runCount))

                totalRunCount += 1


        appendDataFrame = addToAppendDataFrame(thisRunsValues, appendDataFrame,runCount)
        #print(data1_treated)
        #print(appendDataFrame)
        
        data1_treated["VolOfFirstType"] = appendDataFrame["VolOfFirstType"]
        data1_treated["VolOfSecondType"] = appendDataFrame["VolOfSecondType"]
        data1_treated["DifOfMeans"] = appendDataFrame["DifOfMeans"]
        
        return data1_treated


appendAuxiliaryValues(data1_treated)
data1_treated = data1_treated.drop(['index'], axis=1)
data1_treated.to_csv("treated_data",index = False)

print(time.time()-start)