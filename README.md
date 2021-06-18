# dendra-tools-dat2datastream

**PURPOSE**   
dat2datastream converts a LoggerNet .dat file into a Dendra (https://dendra.science) station and interprets the fields in the .dat file into a set of datastreams.  This is a command line utility.

**USAGE**    
`node dat-get-unique-fields organization table-directory`
`node dat-update-unique-fields unique-fields new-fields`

`node dat2station organization table-file`
`node dat2datastream manifest-file`  

**DEPENDENCIES**   
Node.js must be installed.  

**REQUIRED FOLDERS**     
* tables: .dat files are placed here.  'tables/' can be included in argument in path to .dat file, but is not required. The script will look there first.      
* stations:  the station and datastream JSON files are output into a subdirectory within the station folder.      
* vocabulary:  this folder contains vocabulary and unique fields lists.  These must match current lists in Dendra metadata.     

**INSTRUCTIONS**     
**A. Prepare an organization's field parsing list**.   
1. Make sure your organization org_slug and organization_id exist in dat2datastream.js and dat2station.js.     
2. Place all .dat files for an organization into the tables directory, possibly under a subdirectory. 
3. Run dat-unique-fields-finder.  This script parses all the fields in the .dat file, compares them to an existing list for that organization, and appends new unique fields to the list.  This is a major change from the previous way dat2datastream guessed measurements from fieldnames.  Each org tends to have a particular way of naming and listing them together reduces the chance of guessing correctly.     
4. Run merge-field-names.  This script takes the new fields found and folds them into the existing unique fields vocabulary list for the organization.       
5. Edit unique fields vocabulary list for the organization.  Requires a measurement be added. _This is a manual process_.    
**B. Set up a new station.**      
1. Run dat2stations. This will create a subdirectory for the station in the stations directory, the station json file, and a manifest file in the tables directory next to the .dat file.    
2. Edit station directory, manifest, and json file to customize.  _This is a manual process_.    
3. Follow the CLI instructions for committing the station file to Dendra, using the `den` command.  This will validate the station and add a Mongo ID.    
4. Edit the station and the manifest file as needed.    
5. Run dat2datstreams.  All fields will now generate datastream files in the stations directory.  
6. Follow the CLI instructions for comitting the datastreams to Dendra using the `den` command.







