/* 
* LoggerNet .dat file to datastream exporter
* @author=collin bode
* @date=2021-05-18
* @source_date=2018-09-27
*  - dat2datastream.js was the original code. dat2tation.js is half of the original script.
*  - dat2datastreams.js now only exports datastreams.
*  - assumes Pacific Standard Time (UTC-8hours) This should be changed. From Row 5 Parser.
*/
fs = require("fs")
//ObjectID = require('mongodb').ObjectID

// Argument processing of paths
args = process.argv.slice(2)
org_slug = args[0]
datfile = args[1]
boo_write = true

if(args.length != 2) {
	console.log("usage: node dat2station <org> <dat file>")
	process.exit()
}

console.log("dat2station: creating station from "+datfile+" for "+org_slug)

// Organization ID and slug
// NOTE: this should eventually be an online query to dendra via api. Problem is everything else is offline.
if(org_slug == "erczo") {
	orgid = "58db17e824dc720001671379"	
} else if(org_slug == "ucnrs") {
	orgid = "58db17c424dc720001671378"
} else if(org_slug == "chi") {
	orgid = "5b032fe439e58256b546331c"
} else if(org_slug == "ucanr") {
	orgid = "5ea32bc5fafb456f97153c39"
} else if(org_slug == "pepperwood") {
	orgid = "5eb41fe55b25755183a25319"
} else if(org_slug == "prep") {
	orgid = "5ef15567f205a1b6875419dd"
} else if(org_slug == "tnc") {
	orgid = "5eb41ff0883adf89568569d0"	
} else if(org_slug == "cdfw") {
	orgid = "6092b070492ae15e05876ed8"
} else {
	console.log("organization not recognized. quitting.")
	console.log("usage: node dat2station.js <org_slug> <datfile_name>")
	process.exit(1)
}

// DAT File
// Look for the DAT file
if (!fs.existsSync(datfile)){
	console.log("Didn't find",datfile)
	if (fs.existsSync("tables/"+datfile)){
		datfile = "tables/"+datfile
		console.log("Found in table directory. Modified:",datfile)
	} else {
		console.log("DATFile NOT FOUND. exiting.",datfile)
		process.exit()
	}
} else {
	console.log("Found",datfile)
}
// Parse the header rows of the .dat file
datfile_content = fs.readFileSync(datfile).toString().split("\r")

// Row 1 Station Identification 
//"TOA5","L6_WSSS","CR1000","84249","CR1000.Std.31","CPU:Level 6_2017-09-27.CR1","16291","WeatherStationSS"
row1 = datfile_content[0].split(',')
station_name = row1[1].replace('"','').replace('"','')
station = station_name.toLowerCase()
station_slug = station.replace(/_/g,'-')
station_title = station_name.replace(/_/g,' ').replace(/-/g,' ')
loggernet_table = row1[7].replace('"','').replace('"','')
table = loggernet_table.toLowerCase()
console.log("Organization:",org_slug,"Station:",station,"Table:",table)

// Row 2 Header Fields
//"TIMESTAMP","RECORD","BattV","PTemp_C_Avg","BP_mmHg_Avg","AirTC_WS_Avg","RH_WS","PAR_Den_Avg","PAR_Tot_Tot","LWmV_Avg","LWMDry_Tot","LWMCon_Tot","LWMWet_Tot","VWC_WS1_Avg","EC_WS1_Avg","T_WS1_Avg","P_WS1_Avg","PA_WS1_Avg","VR_WS1_Avg","VWC_WS2_Avg","EC_WS2_Avg","T_WS2_Avg","P_WS2_Avg","PA_WS2_Avg","VR_WS2_Avg","VWC_WS3_Avg","EC_WS3_Avg","T_WS3_Avg","P_WS3_Avg","PA_WS3_Avg","VR_WS3_Avg","Rain_mm_Tot"
headers = datfile_content[1].split(',')
console.log(headers)

// Row 3 Units 
units = datfile_content[2].split(',')

// Row 4 Aggregation 
aggs = datfile_content[3].split(',')

// Row 5 First Date 
daterow = datfile_content[4].split(',')
str_first_date = daterow[0].replace('"','').replace('"','')
str_first_date = str_first_date+"+08:00" // Add PST timezone
first_date = new Date(str_first_date)  // autoconverts to UTC
console.log('First Date:',first_date)
console.log("str_first_date: "+str_first_date)

//--------------------------------------------------------------------
// STATION 
ds_path = "stations/"+station+"/"
st_json_path = (ds_path+station_slug+".station.json").toLowerCase()
boo_station_exists = false

// Create station directory
if (!fs.existsSync(ds_path)){
	fs.mkdirSync(ds_path)
	console.log("Creating Dir:",ds_path)
} else {
	console.log("Dir already exists.",ds_path)
	if(fs.existsSync(st_json_path)) {
		console.log("Station JSON already exists. Quiting.")
		boo_station_exists = true
	}
}

//
// Create a station JSON file, unless one already exists
if(boo_station_exists == false) {
	st_json = {  
	  "enabled": true,
	  "is_active": true,
	  "is_stationary": true,
	  "name": station_name,
	  "organization_id": orgid,
	  "slug": station_slug,
	  "station_type": "weather",
	  "time_zone": "PST",
	  "utc_offset": -28800,
  	"external_refs": [
	    {
	    	"identifier":station_name,
	    	"type":"loggernet_station"
	    },
	    {
	    	"identifier": loggernet_table,
	    	"type":"loggernet_table"
	    },
	    {
	      "identifier": "station_"+station,
	      "type": "influx_db"
	    },
	    {
	      "identifier": "source_"+table,
	      "type": "influx_table_fc"
	    },
	    {
	      "identifier": first_date,
	      "type": "start_date_utc"
	    },
	    {
      	"identifier": "/influx/select",
      	"type": "path"
    	}
  	]
	}

	// Customizations by organization
	if(org_slug == 'cdfw') {
		ww = {
			"identifier": "WesternWeatherGroup",
    	"type": "Maintainer"	
		}
		st_json['external_refs'].push(ww)
	}

	// for testing only
	if(station_slug == "test") {
		st_json.station_id = new ObjectID()
	}
	console.log(st_json)

	// Write JSON station file
	st_json_string = JSON.stringify(st_json,null,2)
	fs.writeFileSync(st_json_path,st_json_string,'utf-8')
	console.log("Station JSON exported.  Please upload use the dendra tool to upload station.")
	console.log("den meta push-stations --filespec="+st_json_path+" --save")
}

// Write JSON manifest file in the same location as the DAT file
// if there is already a mnaifest file, do not overwrite
//datfile_parent = os.get_parent(datfile) // FIND JAVASCRIPT FOR THIS
//datfile_parent = "tables/"
manifest_path = ds_path+station_slug+".manifest.json"
if(fs.existsSync(manifest_path)) {
	console.log("A manifest already exists for ",station_slug,"at",manifest_path)
	console.log("Quitting.")
} else {
	manifest = {
		"org_slug": org_slug,
		"out_path": ds_path,
		"station": station_slug,
		"datfile": datfile,
		"fields_list": "fields/"+org_slug+".field_names_unique.json"
	}

	// Write JSON station file
	str_manifest = JSON.stringify(manifest,null,2)
	fs.writeFileSync(manifest_path,str_manifest,'utf-8')
	console.log("Manifest file exported.",manifest_path)
	console.log("Please use dat2datastream.js to create datastreams using the manifest file.")
	console.log("node dat2datastream",manifest_path)
}
