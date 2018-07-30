# Python_DictionaryToCSV_Writer
Fire time series Python dictionaries at this module and it will periodically write them to a CSV, can handle multiple CSV file names and is configurable.

Using the module

All communications take place via the function...

  newValue(ipDict = {}, timeKey = None, directory='', CSVname = '', 
                 metaDataDict = {}, clearBuffer = False)

The directory and name are required to uniquely identify each data stream. In this manner
multiple data streams, with multiple configurations can be handled simultaneously. If
this is left blank then time is used to construct a directory and CSV name.

The initial value sent to newValue should contain an empty ipDict, this data
input will then be treated as a configuration communication. The configuration
optimons are shown at the end in Appendix 1
        
Data dictionaries should be sent as,
newValue(ipDict = {dataToBeEntered}, directory="Documents/OutputData", CSVname="StationB") 
and the ipDict can be in the form 
ipDioct = {'timeUTC':12345.67, 'x':7.7, 'y':8.8, timeKey = 'timeUTC'}, 
please provide the key for time (or other ordered unique field, 
ideally a number as we use a delay term).
Multiple time points can be enetered as shown below,
or in the form {12345.67:{'x':7.7, 'y':8.8}, 12346:{'x':6.6, 'y':9.9}}
the dictionary will default to this form as timeKey defaults to False
(if your dictionary is not like either of these, then things might go wrong)
        
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

Appendix 1




