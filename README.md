# Python_DictionaryToCSV_Writer

=====================================================

##Fire time series Python dictionaries at this module and it will periodically write them to a CSV, the module can handle multiple CSV file names and is configurable.


###Using the module

All communications take place via the function...

  *newValue(ipDict = {}, timeKey = None, directory='', CSVname = '', 
                 metaDataDict = {}, clearBuffer = False)*

The directory and name are required to uniquely identify each data stream. In this manner
multiple data streams, with multiple configurations can be handled simultaneously. If
this is left blank then time is used to construct a directory and CSV name.

The initial value sent to *newValue* should contain an empty *ipDict*, this data
input will then be treated as a configuration communication. The configuration
options are shown at the end in **Appendix 1**
        
An example of a typical data dictionaries is shown below,

*newValue(ipDict = {dataToBeEntered}, directory="Documents/OutputData", CSVname="StationB")*

the directory is the directory to write to, if this doesn't exist it will be created,
the CSV name is the name of the CSV file, if these are left blank, then
time will be used to create the name.

the *ipDict* "dataToBeEntered" can be in the form,
 
*ipDict = {'timeUTC':12345.67, 'x':7.7, 'y':8.8, timeKey = 'timeUTC'}*
 
please provide the key for time (or other ordered unique field, ideally a number as we use a delay term).
Multiple time points can be enetered as shown below,
or in the form 

*ipDict = {12345.67:{'x':7.7, 'y':8.8}, 12346:{'x':6.6, 'y':9.9}}*

the dictionary is assumed to be in this form as the timeKey defaults to False

**(if your dictionary is not like either of these, then things might go wrong)**


###In the metaDataDict the time can be incorporated into the directory  and/or the name by setting,
        
*metaDict['directoryTime'] = "%Y/%m/%d/"*

*metaDict['CSVnameTime'] = "%H-%M-%S"* 
        
N.B. 
1. CHECK FORMAT IS CORRECT
2. PLACE "/" IN THE DIRECTORY, THESE WILL BE INTERPRETED AS FOLDERS
3. time is inserted after the given name
4. most things can be left blank, if you like, might cause problems if everything is left blank, actually the name itterator would probably catch it. - not recommended

**if you want to clear the temporal buffer, then set this to true**

##Appendix 1

The metaDataDict is used to update the dictionary below e.g.

metaDataDict = {'directoryTime' = "", 
                'CSVnameTime' = "%Y-%m-%d/_%H-%M-%S",
                'writeEvery' = 30,
                closeFileAfter = 86400}
                
all times are in seconds

    def initialiseDicts(self, directory, CSVname):
        self.makeIfNotDir(directory)
                    
        directory = self.checkDirEnding(directory)
        CSVname = self.insertTimeInCSVname(CSVname, T='', index = '')
            
        self.writeDict[self.activeDict] = {}
        self.metaDict[self.activeDict] = {}
        
        self.metaDict[self.activeDict]['directory'] = directory 
        self.metaDict[self.activeDict]['CSVname'] = CSVname 
        #
        self.metaDict[self.activeDict]['directoryTime'] = "%Y/%m/%d/"   #this will create a direcory structure, remove "/" to remove directories
        self.metaDict[self.activeDict]['CSVnameTime'] = "%H-%M-%S"      #anything that conforms to datetime.datetime.utcfromtimestamp(writeTime).strftime(nameTimeFormat), https://docs.python.org/3/library/datetime.html 
        
        self.metaDict[self.activeDict]['writeEvery'] = self.WriteEvery      #this is how often the data in memory is written to file
        self.metaDict[self.activeDict]['closeFileAfter'] = self.CloseFileAfter  #This is now often a new file is created (scheduled)
        self.metaDict[self.activeDict]['bufferDelay'] = self.bufferDelay    # this is the minimum amount of data held in memory, allows multiple data streams to be merged, i.e. for network latency
        
        self.metaDict[self.activeDict]['shortenFloat'] = self.shortenFloat  # Boolean, floats will be turned into strings, the number of significant figures can be adjusted
        self.metaDict[self.activeDict]['dataSF'] = self.dataSF   # data significant figures, often 7 is enough, requires knowledge of data
        self.metaDict[self.activeDict]['timeSF'] = self.timeSF   # if time is in UTC then you will require 10 for second resolution, 13 for millisecond resolution   
        
        self.metaDict[self.activeDict]['threadWrite'] = self.threadWrite #generally not used, writing a thread in Pyton doesn't seem to add much in the way of speed (often the contrary)
        
        #self.metaDict[self.activeDict]['timeFormat'] = self.timeFormat # not used
        
        self.metaDict[self.activeDict]['ignoreList'] = self.ignoreList # put the keys of values you wish to ignore or drop in here
        
        #################################################################
        #
        #       NOT CONFIGURABLE
        #
        #################################################################
        
        self.metaDict[self.activeDict]['writeName'] = CSVname 
        self.metaDict[self.activeDict]['writeTime'] = 0

        self.metaDict[self.activeDict]['holdDict'] = {}
        self.metaDict[self.activeDict]['lastWrite'] = 0
        self.metaDict[self.activeDict]['currentTime'] = 0

        self.metaDict[self.activeDict]['oldMod'] = 9999*9999
        self.metaDict[self.activeDict]['timeList'] = []
        self.metaDict[self.activeDict]['holdDict'] = {}
        
        self.metaDict[self.activeDict]['headersList'] = []  
        self.metaDict[self.activeDict]['headersOP'] = []  
        self.metaDict[self.activeDict]['headersFloat'] = []  
        self.metaDict[self.activeDict]['headersString'] = []  
        
        self.metaDict[self.activeDict]['writeTimeList'] = []
        self.metaDict[self.activeDict]['writeError'] = False
        self.metaDict[self.activeDict]['newHeaders'] = False
        self.metaDict[self.activeDict]['newFile'] = True
        self.metaDict[self.activeDict]['newTime'] = False



other stuff

"""
Created on Tue Jul 11 11:28:46 2017

@author: pbrogan

This receives Dictionaries in the form {'uniqueValue':123.4, 'x':2.3, 'y':3.4}
or JSONs in the form {123.4:{'x':123.4, 'y':3.4}, 123.5{'x':126.4, 'y':4.4}}

if JSONs are written like this, then they will be written to the default 
self.CSVlabel, or whatever this label is set to. This type of action is fine
for a single data source.

If you have a number of different dictionaries that you want written to different
file names or directories, then incluide these in the arguments past to the function
the arguments are 'directory' and 'CSVname'. This needs to be sent with every
dictionary, how else would the data be identified. 

Directories are created if they do not exist. CSV names are changed if conflicts
arise e.g. file already in directory is flagged as xxx_existing1.csv, if new
headers arise mid file you get xxx_newHeaders.csv, or if the file was opened 
while being written to xxx_conflict1.csv. The num,ber itterates up if it happens
multiple times. 

The meta data for each file can also be changed by providing a dictionary with
the appropriate key:value pairs 

Other factors can be changed, but these only need to be done once.

'closeFileAfter' = xxx      this is the number of seconds the file should last
'bufferDelay' = xxx         the number of seconds to wait before writing for a 
                            uniqueValue, can accomodate network latency
'writeEvery' = xxx          how often to write to the csv file, you dont want
                            to write constantly, but you dont want to lose 
                            everything in the event of failure
'timeFormat' = "%Y-%m-%d_%H-%M-%S" or "%Y-%m-%d_%H" anything that conforms with
                            the datetime module
'threadWrite' = True/False If you aren't running this is a thread, then you can
                            de the CSV writing in a thread
                            
full list

    CSVname1 = 'felix'
    directory1 = 'badDir'       #self corrects
    
    d1 = {}
    d1['directory'] = directory1
    d1['CSVname'] = CSVname1
    d1['writeEvery'] = 100000
    d1['closeFileAfter'] = 100000
    d1['bufferDelay'] = 100000
    d1['shortenFloat'] = True
    d1['dataSF'] = 3
    d1['timeSF'] = 10
    d1['threadWrite'] = True
    d1['timeFormat'] = "%Y-%m-%d_%H-%M-%S"
    d1['ignoreList'] = [None]
    
e.g. send

DW.newValue(d1, CSVname = CSVname1, directory = directory1)
"""