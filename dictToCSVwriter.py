# -*- coding: utf-8 -*-
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



import csv, datetime, threading, os, ast#, sys
from pathlib import Path

class CSVupoloader():
    def __init__(self):
        self.initialise()
        
    def initialise(self):
        self.WriteEvery = 15
        self.CloseFileAfter = 60 * 60
        self.bufferDelay = 30
        self.shortenFloat = False #this is busted, time and data mixed
        self.dataSF = 6
        self.timeSF = 15
        self.threadWrite = False
        #self.timeFormat = "%Y-%m-%d_%H-%M-%S"
        self.OPpath = ''
        #self.CSVlabel = 'CSVtester'
        self.ignoreList = []#[False, '0000', None]
        #declarations
        self.writeDict = {}
        self.metaDict = {}
        self.activeDict = ''
        self.lastWrite = 0
        self.CurrentTime = 0
        self.OldMod = 9999*9999
        self.timeList = []
        self.holdDict = {}
        self.headersList = []
        self.writeTimeList = []
        self.WriteDict = {}
        self.WriteError = False
        self.NewHeaders = None
        self.NewFile = True
        
#        self.timeFormat = "%Y-%m-%d_%H-%m-%s"
#        if self.CloseFileAfter >= (24*60*60):
#            self.timeFormat = "%Y-%m-%d"
#        elif self.CloseFileAfter >= (60*60):
#            self.timeFormat = "%Y-%m-%d_%H"
#        elif self.CloseFileAfter >= (60):
#            self.timeFormat = "%Y-%m-%d_%H-%m"

    def checkDirEnding(self, name):
        if name[-1] not in ['/', '\\']: name += '/'
        return(name)

    def insertTimeInCSVname(self, name, T, index = ''):
        if len(name) > 3:
            if name[-4] == '.':
                name = name[:-4] + '_' + T + index  + name[-4:]
            else:
                name = name + '_' + T + '.csv'
        else:
            name = T + '.csv'
        return(name)
        
    def makeIfNotDir(self, directory):
        if len(directory) > 0:
            #print(directory)
            if os.path.isdir(directory) != True:
                os.makedirs(directory)        

    def uniqueName(self, i = 1, tag = ''):
        dirTimeFormat = self.metaDict[self.activeDict]['directoryTime']
        nameTimeFormat = self.metaDict[self.activeDict]['CSVnameTime']
        
        writeTime = self.metaDict[self.activeDict]['writeTime']
        
        dirTime = self.checkDirEnding(datetime.datetime.utcfromtimestamp(writeTime).strftime(dirTimeFormat))
        nameTime = datetime.datetime.utcfromtimestamp(writeTime).strftime(nameTimeFormat)
        
        
        path = self.checkDirEnding(self.metaDict[self.activeDict]['directory'])
        
        path += dirTime
        self.makeIfNotDir(path)
        
        NameCSV = self.insertTimeInCSVname(self.metaDict[self.activeDict]['CSVname'], nameTime)
        
        NameCSVfull = path + NameCSV
        
        P = Path(NameCSVfull)        
        NameCSVfullhold = NameCSVfull
        
        while P.is_file() == True:
            """This checks if a file exists (we want a unique name) if so an
            alternative name is chosen, just through indexes"""
            NameCSVfullhold = NameCSVfull[:-4] + '_' + str(tag) + str(i) + NameCSVfull[-4:]
            i += 1
            P = Path(NameCSVfullhold)  
            
        return(NameCSVfullhold)
        
    def Write(self, Array, zipFile):
        #print(Array)
        #if self.metaDict[self.activeDict]['newFile'] == True:
        if self.metaDict[self.activeDict]['newHeaders'] == True:
            if self.metaDict[self.activeDict]['newTime'] != True:
                self.metaDict[self.activeDict]['writeName'] = self.uniqueName(i = 1, tag = 'headers')
                headersOP = self.metaDict[self.activeDict]['headersOP'] 
                Array = headersOP + Array
            else:
                self.metaDict[self.activeDict]['writeName'] = self.uniqueName(i = 1, tag = 'existing')
            self.metaDict[self.activeDict]['newHeaders'] = False
                
                #self.metaDict[self.activeDict]['writeName'] = self.uniqueName(i = 1, tag = 'existing') #tag is only implemented if...
                #self.metaDict[self.activeDict]['newHeaders'] = False
            #else:
        #print(Array)        
        i = 0
        while i < 20:
            try:
                with open(self.metaDict[self.activeDict]['writeName'], 'a', newline = '') as f:
                    writer = csv.writer(f)
                    writer.writerows(Array)
        #           logging.debug('writing to ' + NameCSV)
        #            
        #        if zipFile == True:
        #            NameZIP = Name + '.zip'
        #            with open(NameCSV, 'r') as f:
        #                zipfile.ZipFile(f, NameZIP)
                break
            except:
                if i == 0:
                    headersOP = self.metaDict[self.activeDict]['headersOP'] 
                    Array = headersOP + Array
                i += 1
                self.metaDict[self.activeDict]['writeName'] = self.uniqueName(i = i, tag = 'locked_')
                #Array = self.metaDict[self.activeDict]['headersOP'] + Array
                #print(sys.exc_info()[0:2])
            
    def ThreadWrite(self, Array, zipFile = False):  
        """the Array is the data to be written, the zip file is not used yet""" 
        if self.metaDict[self.activeDict]['threadWrite'] == False:
            self.Write(Array, zipFile)    
        else:
            t = threading.Thread(target = self.Write, args = (Array, zipFile))
            t.start()

    #these functions apply the concatronation of the figures, to significant figures

    fix2 = lambda self, Value, DecimalP, SF : str(float(int(float(Value) * 10**(DecimalP) + 0.5))/10**(DecimalP))[:SF+1]
    fix = lambda self, Value, SF : str(int(Value + 0.5)) if SF <= str(Value).find('.') else self.fix2(Value, (SF - str(Value).find('.')), SF)
    #   fix() if the the value is larger than 10^SF, then this is turned into 
    #   an integer and returned, the magnitude of the value is inferred from 
    #   the decimal point position
    #   else the data is passed to fix2, the multiplication necessary is 
    #   inferred from the difference between the decimal point and the 
    #   significant figures, this is then cut by the SF value 
    #  

    testNull = lambda self, value : '' if value in self.ignoreList else value
        
    def ArrayMe(self, Dict, TimeList):
        WriteArray = []
        TimeList.sort()
        #print(TimeList)#, Dict)
        for Time in TimeList:    
            opTime = Time
            if self.metaDict[self.activeDict]['shortenFloat'] == True:
                opTime = self.fix(opTime, self.metaDict[self.activeDict]['timeSF'])
                
            TempList = [opTime]
            LitlDict = Dict[Time]
            
            if self.metaDict[self.activeDict]['shortenFloat'] == True:
                for key in self.metaDict[self.activeDict]['headersList']:
                    try:
                        value = LitlDict[key]
                    except:
                        value = ''
                    if key in self.metaDict[self.activeDict]['headersFloat'] and type(value) == float:
                        TempList.append(self.fix(value, self.metaDict[self.activeDict]['dataSF']))
                    else:
                        TempList.append(self.testNull(value))
            else:
                for key in self.metaDict[self.activeDict]['headersList']:
                    try:
                        value = LitlDict[key]
                    except:
                        value = ''
                    TempList.append(self.testNull(value))
                    
            WriteArray.append(TempList)
        return(WriteArray)

    def updateHeaders(self): 
        ipDict = self.metaDict[self.activeDict]['holdDict']
        headers = []
        curTime = list(ipDict.keys())[0]
        for key in ipDict[curTime].keys():
            headers.append(key)
            
        self.metaDict[self.activeDict]['headersList'] = headers
        self.sortHeaders(curTime)
        self.parseHeaders()
        #print(self.metaDict[self.activeDict]['headersList'])
        

    def sortHeaders(self, curTime):
        """This needs to be scanitised for the human reader, eg. angles preceeding
        magnitudes is hard for some people, but in the mean time!!!"""
        #headers = [str(h) for h in headers]
        headers = self.metaDict[self.activeDict]['headersList']
        ipDict = self.metaDict[self.activeDict]['holdDict']
        headers.sort()
        self.metaDict[self.activeDict]['headersFloat'] = []
        self.metaDict[self.activeDict]['headersString'] = []
        #print(headers)
        #print(ipDict[time].keys())
        for key in headers:
            try:
                if type(ipDict[curTime][key]) == float:
                    self.metaDict[self.activeDict]['headersFloat'].append(key)                
                else:
                    self.metaDict[self.activeDict]['headersString'].append(key)
            except:
                self.metaDict[self.activeDict]['headersString'].append(key)

    def parseHeaders(self):
        #headers =  [ str(data[4:]) for data in self.metaDict[self.activeDict]['headersList'] ]
        self.metaDict[self.activeDict]['headersList'].sort()
        headers = self.metaDict[self.activeDict]['headersList']
        self.metaDict[self.activeDict]['headersOP'] = [['Time UTC'] + headers]
        #return(headers)
        
    def findCut(self):  
        """This should find the point where the time ticks over the cut point"""
        CutIndex = 1
        Hit = False#
        oldMod = self.metaDict[self.activeDict]['writeTimeList'][0] % self.metaDict[self.activeDict]['closeFileAfter']
        for Time in self.metaDict[self.activeDict]['writeTimeList'][1:]:
            newMod = Time % self.metaDict[self.activeDict]['closeFileAfter']
            if oldMod > newMod:
                Hit = True
                break
            else:
                CutIndex += 1  
        return(CutIndex, Hit)
        
    def WriteData(self):
        #print('wd', self.writeDict[self.activeDict])
        #try:
        if len(self.writeDict[self.activeDict]) > 0:
            """There is information in the dictionary to write"""
            if len(self.metaDict[self.activeDict]['writeTimeList']) > 0:
                """There are times to be loaded"""
          #      if self.metaDict[self.activeDict]['newFile'] == True:
                    
                if self.metaDict[self.activeDict]['newTime'] == True:
                    """The Time period has elapsed, time for the new CSV, this
                    is triggered in the main loop self.newValue()"""   
                    #print(self.metaDict[self.activeDict]['newTime'])                    
                    
                    
                    CutIndex, Hit = self.findCut()
                    headersOP = self.metaDict[self.activeDict]['headersOP']  
                    if Hit == True :
                        
                        TimeList1 = self.metaDict[self.activeDict]['writeTimeList'][:CutIndex]
                        self.ThreadWrite(self.ArrayMe(self.writeDict[self.activeDict], TimeList1))
                        
                        self.updateHeaders()
                        TimeList2 = self.metaDict[self.activeDict]['writeTimeList'][CutIndex:]
                        self.metaDict[self.activeDict]['writeTime'] = int(TimeList2[0] - TimeList2[0] % self.metaDict[self.activeDict]['closeFileAfter'])
                        self.metaDict[self.activeDict]['writeName'] = self.uniqueName(tag = 'newTime')
                        Array = headersOP + self.ArrayMe(self.writeDict[self.activeDict], TimeList2)                            
                        self.ThreadWrite(Array)
                        
                        #logging.info('New File ' + str(self.metaDict[self.activeDict]['writeTime']) + ' | Headers = ' + str(headersOP))  
                    else:
                        Time = self.metaDict[self.activeDict]['currentTime']
                        self.metaDict[self.activeDict]['writeTime'] = int(Time - Time % self.metaDict[self.activeDict]['closeFileAfter'])
                        self.metaDict[self.activeDict]['writeName'] = self.uniqueName()
                        #self.metaDict[self.activeDict]['newHeaders'] = False
                        #logging.info('Start File ' + str(self.metaDict[self.activeDict]['writeTime']) + ' | Headers = ' + str(headersOP))                    
                        Array = headersOP + self.ArrayMe(self.writeDict[self.activeDict], self.metaDict[self.activeDict]['writeTimeList'])
                        self.ThreadWrite(Array, self.metaDict[self.activeDict]['writeTime'])
                            
                        #Add function to check size of folder and decimate if necessary
                    self.metaDict[self.activeDict]['newTime'] = False
                        
                else:
                    """This is the normal append write"""
                    self.ThreadWrite(self.ArrayMe(self.writeDict[self.activeDict], self.metaDict[self.activeDict]['writeTimeList']))
                    
            self.writeDict[self.activeDict] = {}
            if self.WriteError == True:
                self.WriteError = False
                #logging.critical('Back writing again')
#        except:
#            if self.WriteError == False:
#                self.WriteError = True
#                print('write error', sys.exc_info()[0:2])
#                #logging.critical('Error in writing to file, is it open in something other than NotePad++??? You will be notified if service resumes...')
#                #logging.critical(str(sys.exc_info()[0:2]))

    def initialiseDicts(self, directory, CSVname):
        self.makeIfNotDir(directory)
                    
        directory = self.checkDirEnding(directory)
        CSVname = self.insertTimeInCSVname(CSVname, T='', index = '')
            
        self.writeDict[self.activeDict] = {}
        self.metaDict[self.activeDict] = {}
        
        self.metaDict[self.activeDict]['directory'] = directory 
        self.metaDict[self.activeDict]['CSVname'] = CSVname 
        #
        self.metaDict[self.activeDict]['directoryTime'] = "%Y/%m/%d/" 
        self.metaDict[self.activeDict]['CSVnameTime'] = "%H-%M-%S" 
        
        self.metaDict[self.activeDict]['writeEvery'] = self.WriteEvery
        self.metaDict[self.activeDict]['closeFileAfter'] = self.CloseFileAfter
        self.metaDict[self.activeDict]['bufferDelay'] = self.bufferDelay
        
        self.metaDict[self.activeDict]['shortenFloat'] = self.shortenFloat
        self.metaDict[self.activeDict]['dataSF'] = self.dataSF
        self.metaDict[self.activeDict]['timeSF'] = self.timeSF     
        
        self.metaDict[self.activeDict]['threadWrite'] = self.threadWrite
        
        #self.metaDict[self.activeDict]['timeFormat'] = self.timeFormat
        
        self.metaDict[self.activeDict]['ignoreList'] = self.ignoreList
        
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

    def updateMetaDict(self, metaDict):
        for key, value in metaDict.items():
            self.metaDict[self.activeDict][key] = value  
            
#        if 'timeFormat' not in metaDict and 'currentTime' in metaDict:
#            self.timeFormat = "%Y-%m-%d_%H-%m-%s"
#            if self.CloseFileAfter >= (24*60*60):
#                self.timeFormat = "%Y-%m-%d"
#            elif self.CloseFileAfter >= (60*60):
#                self.timeFormat = "%Y-%m-%d_%H"
#            elif self.CloseFileAfter >= (60):
#                self.timeFormat = "%Y-%m-%d_%H-%m"
#            self.metaDict[self.activeDict]['timeFormat'] = self.timeFormat

    def testVal(self, val):
        if type(val) == str:
            #print(type(val), val)
            try:
                val = ast.literal_eval(val)
            except:
                val = str(val)
        return(val)

    def flattenDict(self, IPdict):

        fullOPdict = {}
        for key0 in IPdict:
            keyList0 = [key0]
            tempDict1 = IPdict[key0]
            key0 = self.testVal(key0)                 
            if type(tempDict1) != dict:
                fullOPdict[tuple([key0])] = self.testVal(tempDict1)    
            else:                     
                for key1 in tempDict1:
                    keyList1 = keyList0 + [key1]
                    tempDict2 = tempDict1[key1]
                    if type(tempDict2) != dict:
                        fullOPdict[tuple(keyList1)] = self.testVal(tempDict2)
                    else:
                        for key2 in tempDict2:
                            keyList2 = keyList1 + [key2]
                            tempDict3 = tempDict2[key2]
                            if type(tempDict3) != dict:
                                fullOPdict[tuple(keyList2)] = self.testVal(tempDict3)
                            else:
                                for key3 in tempDict3:
                                    keyList3 = keyList2 + [key3]
                                    tempDict4 = tempDict3[key3]
                                    if type(tempDict4) != dict:
                                        fullOPdict[tuple(keyList3)] = self.testVal(tempDict4)
                                    else:
                                        for key4 in tempDict4:
                                            keyList4 = keyList3 + [key4]
                                            tempDict5 = tempDict4[key4]
                                            if type(tempDict5) != dict:
                                                fullOPdict[tuple(keyList4)] = self.testVal(tempDict5)
                                            else:
                                                for key5 in tempDict5:
                                                    keyList5 = keyList4 + [key5]
                                                    tempDict6 = tempDict5[key5]
                                                    if type(tempDict6) != dict:
                                                        fullOPdict[tuple(keyList5)] = self.testVal(tempDict6)
                                                    else:
                                                        for key6 in tempDict6:
                                                            keyList6 = keyList5 + [key6]
                                                            tempDict7 = tempDict6[key6]
                                                            if type(tempDict7) != dict:
                                                                fullOPdict[tuple(keyList6)] = self.testVal(tempDict7)
                                                            else:
                                                                for key7 in tempDict7:
                                                                    keyList7 = keyList6 + [key7]
                                                                    tempDict8 = tempDict7[key7]
                                                                    if type(tempDict8) != dict:
                                                                        fullOPdict[tuple(keyList7)] = self.testVal(tempDict8)
                                                                    else:
                                                                        print(tempDict8)
                                                                        print('You nedd to make this silly thing bigger')
                                                                        
        return(fullOPdict)

    def mergeDictionaries(self, updateDict):
        for key in updateDict:
            if key in self.metaDict[self.activeDict]['holdDict']:
                self.metaDict[self.activeDict]['holdDict'][key].update(updateDict[key])
            else:
                self.metaDict[self.activeDict]['holdDict'][key] = updateDict[key]
                

    def newValue(self, ipDict = {}, timeKey = None, directory='', CSVname = '', 
                 metaDataDict = {}, clearBuffer = False):
        
        """
        The directory and name are required to uniquely identify each data stream.
        
        Dictionary can be in the form {'time':12345.67, 'x':7.7, 'y':8.8}, in which
        case please provide the key for time (or other ordered unique field, 
        ideally a number as we use a delay term)
        
        or in the form {12345.67:{'x':7.7, 'y':8.8}, 12346:{'x':6.6, 'y':9.9}}
        if it's not like either of these, then things might go wrong
        
        the directory is the directory to write to, if this doesn't exist, it will be created
        the CSV name is the name of the CSV file, if these are left blank, then
        time will be used to create the name.
        
        in the metaDataDict the time can be incorporated into the directory 
        and/or the name by setting,
        
        metaDict['directoryTime'] = "%Y/%m/%d/" 
        metaDict['CSVnameTime'] = "%H-%M-%S"  
        
        N.B. 1> CHECK FORMAT IS CORRECT
            2> PLACE "/" IN THE DIRECTORY, THESE WILL BE INTERPRETED AS FOLDERS
            3> time is inserted after the given name
            4> most things can be left blank, if you like, might cause problems
            if everything is left blank, actually the name itterator would 
            probably catch it. - not recommended
        
        if you want to clear the buffer, then set this to true
        
        ['closeFileAfter', 'bufferDelay', 'writeEvery', 'threadWrite']"""
         
        if directory == '':
            directory = self.OPpath
#        if CSVname == '': 
#            CSVname = self.CSVlabel 
        
        self.activeDict = directory + CSVname
        if self.activeDict not in self.writeDict:
            """writeDict can handle multiple data streams, each uniquely ID'ed
            by their directory and csv name"""
            self.initialiseDicts(directory, CSVname)
            
        if metaDataDict != {}:
            """for now the most important thing is the time format"""
            self.updateMetaDict(metaDataDict)                
        
        if ipDict != {}:
            """ipDict can be left blank if metadata is being updated"""
            #print('ip', ipDict)
            if timeKey == None:
                updateDict = {}
                #print('holda', self.metaDict[self.activeDict]['holdDict'])
                for key in ipDict:
                    updateDict[float(key)] = self.flattenDict(ipDict[key])                    
                self.mergeDictionaries(updateDict)
                #print('holdb', self.metaDict[self.activeDict]['holdDict'])
                self.metaDict[self.activeDict]['timeList'] += [ float(t) for t in list(updateDict.keys())]
                self.metaDict[self.activeDict]['timeList'] = list(set(self.metaDict[self.activeDict]['timeList']))      
                
    
            else:
                time = float(ipDict.pop(timeKey))
                ipDict = self.flattenDict(ipDict)
                #print(time, ipDict)
                updateDict = {time: ipDict}
                #self.metaDict[self.activeDict]['holdDict'].update(updateDict)
                self.mergeDictionaries(updateDict)
                if time not in self.metaDict[self.activeDict]['timeList']:
                    self.metaDict[self.activeDict]['timeList'].append(time)   
#                except:
#                    print(time, ipDict)
#                    dgffg
                
            self.metaDict[self.activeDict]['timeList'].sort()
            maxTime = self.metaDict[self.activeDict]['timeList'][-1]
            #print('here', maxTime, metaDict['bufferDelay'])
            #print(maxTime, self.metaDict[self.activeDict]['bufferDelay']  )
            maxWriteTime = maxTime - self.metaDict[self.activeDict]['bufferDelay']        
            if clearBuffer == True:
                maxWriteTime = maxTime  
    #            cut = -1
    #            print('clearingBuffer')
    #        else:
            cut = next((i for i, t in enumerate(self.metaDict[self.activeDict]['timeList']) if t >= maxWriteTime), 0) 
    
            if cut > 0:
                #print(self.metaDict[self.activeDict]['newTime']) 
                if set(self.metaDict[self.activeDict]['holdDict'][self.metaDict[self.activeDict]['timeList'][cut]].keys()) <= set(self.metaDict[self.activeDict]['headersList']):
                    """if the number of headers at the file contains no headers that
                    are missing from the headers file, then proceed"""              
                    pass
                else:
                    """New headers at the end of the file"""
                    if set(self.metaDict[self.activeDict]['holdDict'][self.metaDict[self.activeDict]['timeList'][0]].keys()) <= set(self.metaDict[self.activeDict]['headersList']):
                        """This means the initial dictionary contains the known headers"""
                        cut = 0
                        for time in self.metaDict[self.activeDict]['timeList']:
                            if time >= maxWriteTime:
                                break
                            else:
                                if set(self.metaDict[self.activeDict]['holdDict'][time].keys()) <= set(self.metaDict[self.activeDict]['headersList']):
                                    """This should ID when the new headers start"""
                                    cut += 1
                                else:
                                    break
                    else:
                        """This is the new file being initiated, with the new headers"""
                        for time in self.metaDict[self.activeDict]['timeList'][:cut]:
                            self.updateHeaders()
                            self.metaDict[self.activeDict]['newHeaders'] = True
                        
                
                self.metaDict[self.activeDict]['currentTime'] = self.metaDict[self.activeDict]['timeList'][cut-1]
                self.metaDict[self.activeDict]['writeTimeList'] += self.metaDict[self.activeDict]['timeList'][:cut]
                self.metaDict[self.activeDict]['timeList'] = self.metaDict[self.activeDict]['timeList'][cut:]
    

                #print(metaDict['writeTimeList'])
                if (self.metaDict[self.activeDict]['currentTime'] - self.metaDict[self.activeDict]['lastWrite']) >= self.WriteEvery or clearBuffer == True:
                    #print('hold2', self.metaDict[self.activeDict]['holdDict'])
                    self.metaDict[self.activeDict]['writeTimeList'] = list(set(self.metaDict[self.activeDict]['writeTimeList']))
                    for time in self.metaDict[self.activeDict]['writeTimeList']:
                        #print(time, self.metaDict[self.activeDict]['holdDict'])
                        self.writeDict[self.activeDict][time] = self.metaDict[self.activeDict]['holdDict'].pop(time)
                    #print('writing', self.metaDict[self.activeDict]['writeTimeList'])
                    #print(self.writeDict[self.activeDict])
                    self.metaDict[self.activeDict]['lastWrite'] = self.metaDict[self.activeDict]['currentTime']
                    NewMod = int(self.metaDict[self.activeDict]['currentTime']) % self.metaDict[self.activeDict]['closeFileAfter']
                    if NewMod < self.metaDict[self.activeDict]['oldMod']: #86400 IS ONE DAY IN SECONDS
                        """This means time has ticked over, new file"""
                        #self.metaDict[self.activeDict]['newFile'] = True   
                        self.metaDict[self.activeDict]['newTime'] = True  
                        #self.metaDict[self.activeDict]['newHeaders'] = True
                        #metaDict['writeTime'] = int(metaDict['currentTime'])     
                        #print('make new file, bottom', metaDict['currentTime'])
                    
                    self.metaDict[self.activeDict]['oldMod'] = NewMod                                
                    #logging.debug('writing')
                    self.WriteData()
                    #perge any data
                    self.metaDict[self.activeDict]['writeTimeList'] = []
                    self.writeDict[self.activeDict] = {}
                    
            if clearBuffer == True:
                self.ThreadWrite(self.ArrayMe(self.metaDict[self.activeDict]['holdDict'], self.metaDict[self.activeDict]['timeList']))
                #print(self.metaDict[self.activeDict]['holdDict'])
                self.metaDict[self.activeDict]['timeList'] = []
                self.metaDict[self.activeDict]['holdDict'] = {}
                    
