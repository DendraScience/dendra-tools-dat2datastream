/* 
* LoggerNet .dat file to datastream exporter
* @author=collin bode
* @date=2018-09-27
* @update=2021-05-10
*  - now uses exact matches to fieldnames instead of guessing at fieldname
*    by default.  
*/
fs = require("fs")
//ObjectID = require('mongodb').ObjectID

// Argument processing of paths
args = process.argv.slice(2)
org_slug = args[0]
match_type = args[1]
datfile = args[2]
boo_write = true

if(args.length != 3) {
	console.log("usage: node dat2datastream <org> <exact,guess> <table>")
	process.exit()
}

console.log("dat2datastreams: converting "+datfile+" for "+org_slug+" match type is "+match_type)

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
	console.log("usage: node dat2datastreams.js <org_slug> <datfile_name>")
	process.exit(1)
}

// Load config files 
conv_units = JSON.parse(fs.readFileSync('./vocabulary/conv_units.json'))
conv_aggs = JSON.parse(fs.readFileSync('./vocabulary/conv_aggregates.json'))
conv_measurements = JSON.parse(fs.readFileSync('./vocabulary/conv_measurement_full.json'))
//conv_variables = JSON.parse(fs.readFileSync('./vocabulary/conv_variables.json'))
//conv_mediums = JSON.parse(fs.readFileSync('./vocabulary/conv_mediums.json'))
conv_vocab = JSON.parse(fs.readFileSync('./vocabulary/conv_meta_vocab_unique.json'))

// Create a blank measurement set
blank_mmeasurement = {
	"measurement": "UNKNOWN",
	"variable": "UNKNOWN",
	"medium": "UNKNOWN",
	"purpose": "UNKNOWN",
	"name": "UNKNOWN"
}

// Standardize units returns a Dendra-approved unit type
function standardize_units(ns_unit) {
	boo_match = false
	s_unit = "UNKNOWN" //ns_unit
	ns_unit = ns_unit.toLowerCase().replace('\n',"")
	for (let term of conv_units.terms) {
		s_label = term.label		
		for (var k=0;k<term.abbreviations.length;k++) {
			abbrev = term.abbreviations[k].toLowerCase()
			//console.log("\t",ns_unit,"=?",abbrev)
			if(ns_unit === abbrev) {
				boo_match = true
				s_unit = 'dt_Unit_'+s_label
				//console.log("\tMATCH!",ns_unit,"==",abbrev,"-->",s_label)
			}
		}
		if(boo_match == true) { break }
	}
	if(boo_match == false) {
		console.log("\t",ns_unit,s_unit)
	}
	return s_unit
}

// Standardize aggregates returns a Dendra-approved aggregate type
function standardize_aggregates(ns_agg) {
	boo_match = false
	s_agg = "UNKNOWN" //ns_unit
	ns_agg = ns_agg.toLowerCase().replace('\n',"")
	for (let term of conv_aggs.terms) {
		s_label = term.label		
		for (var k=0;k<term.abbreviations.length;k++) {
			abbrev = term.abbreviations[k].toLowerCase()
			//console.log("\t",ns_agg,"=?",abbrev)
			if(ns_agg === "" || ns_agg === "smp") {
				boo_match = true
				s_agg = ""
			} else if(ns_agg === abbrev) {
				boo_match = true
				s_agg = 'ds_Aggregate_'+s_label
				//console.log("\t!",ns_agg,"==",abbrev,"-->",s_agg)
			}
		}
		if(boo_match == true) { break }
	}
	return s_agg
}

/*
// Parses header, returns a Dendra-approved medium or variable 
// if possible, Unknown otherwise
function guess_at_header(header,header_part) {                                                                                                                                                                                                                                   
	boo_match = false
	if(header_part == "medium") {
		prefix = "ds_Medium_"
		terms = conv_mediums.terms
	} else if(header_part == "variable") {
		prefix = "ds_Variable_"
		terms = conv_variables.terms
	} else {
		console.log("guess_at_header: header_part not recognized:",header_part)
		return ""
	}
	s_val =  header_part.toUpperCase()+" UNKNOWN" //prefix+"Unknown"
	header = header.toLowerCase().replace('\n',"")
	for (let term of terms) {
		s_label = term.label	
		if(typeof(term.abbreviations) === "undefined") { continue }
		for (var k=0;k<term.abbreviations.length;k++) {
			abbrev = term.abbreviations[k].toLowerCase()
			//console.log("\t",header,"=?",abbrev)
			if(header.match(abbrev)) {
				boo_match = true
				s_val = prefix+s_label
				//console.log("\t!",header,"==",abbrev,"-->",s_val)
			}
		}
		if(boo_match == true) { break }
	}
	return s_val
}
*/


// Parses header fieldname, returns a Dendra-approved measurement, medium, variable or Unknown 
function find_measurement_guess_field(header) {
	boo_match = false
	m = {}
	m.measurement = "UNKNOWN"
	m.variable = "UNKNOWN"
	m.medium = "UNKNOWN"
	m.purpose = "UNKNOWN"
	m.name = "UNKNOWN"
 	header = header.toLowerCase().replace('\n',"").replace(" ","")
	for (let term of conv_measurements.terms) {	
		if(typeof(term.abbreviations) === "undefined") { continue }
		for (var k=0;k<term.abbreviations.length;k++) {
			abbrev = term.abbreviations[k].toLowerCase()
			//console.log("\t",header,"=?",abbrev)
			if(header.match(abbrev)) {
				boo_match = true
				//console.log("\t!",header,"==",abbrev,"-->",term.label)
			}
		}
		if(boo_match == true) { 
			m.measurement = "dq_Measurement_"+term.label
			m.variable = "ds_Variable_"+term.variable
			m.medium = "ds_Medium_"+term.medium	
			m.purpose = "dq_Purpose_"+term.data_purpose
			if(typeof(term.name) === 'undefined') {
				m.name = term.label
			} else {
				m.name = term.name
			}	
			//console.log("guess_at_measurement("+header+")",m)
			break 
		}
	}
	if(boo_match == false) {
		console.log("guess_at_measurement("+header+") NOT FOUND!")
	}
	return m
}

// Parses header, tries to match with a unique existing field
function find_measurement_unique_field(header) {                     
	boo_match = false
	f = {}
	f.measurement = "UNKNOWN"
	f.variable = "UNKNOWN"
	f.medium = "UNKNOWN"
	f.purpose = "UNKNOWN"
	header = header.toLowerCase().replace('\n',"")
	// loop through list of exact fieldnames looking for header fieldname
	for (let term of conv_vocab.terms) {	
		if(typeof(term.fields) === "undefined") { continue }
		for (var l=0;l<term.fields.length;l++) {
			field = term.fields[l].toLowerCase()
			//console.log("\t",header,"=?",abbrev)
			if(header == field) {
				boo_match = true
				f.measurement_label = term.measurement
				break	
			}
		}
		/*
		if(boo_match == true) { 
			f.measurement = "dq_Measurement_"+term.measurement
			f.variable = "ds_Variable_"+term.variable
			f.medium = "ds_Medium_"+term.medium	
			f.purpose = "dq_Purpose_"+term.data_purpose	
			break 
		}
		*/
	}
	if(boo_match == false) {
		console.log("guess_at_measurement_with_field("+header+") NOT FOUND!")
		return f
	}

	// Get the rest of the measurement values from the measurements table
	for (let term of conv_measurements.terms) {
		if(term.label == f.measurement_label) {
			f.measurement = "dq_Measurement_"+term.label
			f.variable = "ds_Variable_"+term.variable
			f.medium = "ds_Medium_"+term.medium	
			f.purpose = "dq_Purpose_"+term.data_purpose	
			if(typeof(term.name) === 'undefined') {
				f.name = term.label
			} else {
				f.name = term.name
			}	
			break
		}
	}
	return f
}

// DAT File
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


//--------------------------------------------------------------------
// STATION 
ds_path = "./stations/"+station
st_json_path = (ds_path+"/"+station_slug+".station.json").toLowerCase()
boo_station_exists = false

// Create station directory
if (!fs.existsSync(ds_path)){
	fs.mkdirSync(ds_path)
	console.log("Creating Dir:",ds_path)
} else {
	console.log("Dir already exists.",ds_path)
	if(fs.existsSync(st_json_path)) {
		console.log("Station JSON already exists, skipping to datastreams.")
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
	    }
  	]
	}
	// for testing only
	if(station_slug == "test") {
		st_json.station_id = new ObjectID()
	}
	console.log(st_json)

	// Write JSON file
	st_json_string = JSON.stringify(st_json,null,2)
	fs.writeFileSync(st_json_path,st_json_string,'utf-8')
	console.log("Station JSON exported.  Please upload use the dendra tool to upload, then restart this script.")
	console.log("den meta push-stations --filespec="+st_json_path+" --save")
	process.exit()
}

// Get Station ID from updated JSON file
st_json = JSON.parse(fs.readFileSync(st_json_path))
station_id = st_json._id


//--------------------------------------------------------------------
// DATASTREAMS
// Counts of datastreams from header
header_identified = 0
header_ignored = 0
header_unknown = 0
header_validation_fail = 0
header_validation_pass = 0

// Loop through header field names and generate datastreams
// ns_ prefix means 'non standard', s_ prefix means 'standardized' 
for (j in headers) {
	ds_json = {}
	passfail = 'FAIL'
	header = headers[j].replace('"','').replace('"','').replace("\n","")
	ns_unit = units[j].replace('"','').replace('"','')
	s_unit = standardize_units(ns_unit)
	ns_agg = aggs[j].replace('"','').replace('"','')
	s_agg = standardize_aggregates(ns_agg)
	s_function = ""
	tag_list = []

	// Measurements are the most problematic part of dat2datastream 
	// Exact matches are best, but do not always work. Guesses can often go wrong. 
	g = find_measurement_guess_field(header)  	// Search for measurement using a guess based on fieldname
	u = find_measurement_unique_field(header)  	// Search for measurement using the exact fieldname

	if(match_type == 'guess') {
		s = g
		v = u
	} else {
		s = u
		v = g
	}

	//console.log("s:",s.measurement,"v:",v.measurement)
	if(s.measurement == v.measurement && s.medium == v.medium && s.variable == v.variable) {
		passfail = 'MATCH'
		header_validation_pass++
	} else {
		passfail = 'no match'
		header_validation_fail++
	}
	
	if(s.measurement == "dq_Measurement_IGNORE") {
		header_ignored++
		console.log(header,"is in ignore list, skipping")
		continue
	} 

	if(s.measurement == "dq_Measurement_UNKNOWN") {
		header_unknown++
		console.log(header,"Measurement is UNKNOWN")
	} else {
		header_identified++
	}

	// Customizations based on specific Measurement Types
	if(s.measurement == 'dq_Measurement_BatteryVoltage') {
		if(s_agg == "" || s_agg == "ds_Aggregate_Average") {
			s_function = "ds_Function_Status"
		}
	}

	// Create an old skool V1 Tag list, let the dendra worker translate back into json
	for (let tag of [s.measurement,s.purpose,s_unit,s_agg,s.medium,s.variable,s_function])  {
		//console.log(header,s)
		if(tag != "") {
			tag_list.push(tag)
		}
	}
	//console.log(header,"tags:",tag_list)
	
	// Name builder (experimental)
	name_medium = s.medium.replace('ds_Medium_','')
	name_variable = s.variable.replace('ds_Variable_','')
	name_agg = s_agg.replace('ds_Aggregate_','')
	name_unit = s_unit.replace('dt_Unit_','')
	name_purpose = s.purpose.replace('dq_Purpose_','')
	name_built = s.name+' '+name_agg+' '+header //.replace(/_/g,'')
	//console.log("name:",name_built)

	// Create datastream JSON
	ds_json[j] = {
	  "is_enabled": true,
	  "name": name_built,
	  "organization_id": orgid,
	  "source_type": "sensor",
	  "state": "ready",
	  "station_id": station_id,
	  "datapoints_config": [
	    {
	      "params": {
	        "query": {
	          "api": org_slug,
	          "db": "station_"+station,
	          "fc": "source_"+table,
	          "sc": "\"time\", \""+header+"\"",
	          "utc_offset": -28800,
	          "coalesce": false
	        }
	      },
	      "begins_at": first_date,
	      "path": "/influx/select"
	    }
	  ],
	  "tags": tag_list
	}

	if(s.measurement == "dq_Measurement_SolarRadiation" && s_unit == "dt_Unit_KiloWattPerSquareMeter") {
		ds_json[j].datapoints_config.actions = { "evaluate": "v = number(1000 * v)" }
		s_unit = "dt_Unit_WattPerSquareMeter"
		name_unit = s_unit.replace('dt_Unit_','')
	}

	// Report the results before writing
	console.log(passfail+":",name_built,name_agg,name_unit)
	if(passfail != 'MATCH') {
		console.log("\t  HEADER:",header," PARTS:",s.measurement,s.medium,s.variable,s.purpose)
		console.log("\t HEADERV:",header,"PARTSV:",v.measurement,v.medium,v.variable,v.purpose)
	}
	// Write JSON file
	if(boo_write == true) {
		ds_json_string = JSON.stringify(ds_json[j],null,2)
		header_fixed = header.replace("(","").replace(")","").replace(" ","")
		ds_json_path = (ds_path+"/"+station+"_"+header_fixed+".datastream.json").toLowerCase()
		fs.writeFileSync(ds_json_path,ds_json_string,'utf-8')
	}
}
//console.log(ds_json)

// FINISH and upload
console.log("DONE! Datastreams created for",st_json.name,"\n")
console.log("Headers total:",headers.length," identified:",header_identified," unknown:",header_unknown," ignored:",header_ignored)
console.log("VALIDATION PASS:",header_validation_pass,"FAIL:",header_validation_fail,"\n")
console.log("use Dendra Tool to upload:")
console.log("den meta push-datastreams --filespec="+ds_path+"'/*.datastream.json' --save")

