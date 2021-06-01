# dendra-tools-dat2datastream

**PURPOSE**   
dat2datastream converts a LoggerNet .dat file into a Dendra (https://dendra.science) station and interprets the fields in the .dat file into a set of datastreams.  This is a command line utility.

**DEPENDENCIES**   
Node.js must be installed.  

**USAGE**    
`node dat2datastream.js organization exact/guess table/tablename`  

**REQUIRED FOLDERS**     
* tables: .dat files are placed here.  'tables/' must be included in argument in path to .dat file.      
* stations:  the station and datastream JSON files are output into a subdirectory within the station folder.      

