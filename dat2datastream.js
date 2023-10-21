/* 
* LoggerNet .dat file to datastream converter
* @author=collin bode
* @date=2021-05-18
* @source_date=2018-09-27
*  - dat2datastream-original.js is the original code. 
#  - current version is half of the original script.
*  - dat2station.js only exports station.
*  - changed "guessing" of measurements to requiring an org-specific header list
*  - adds a manfifest file with arguments
*/
fs = require("fs")

// Argument processing of paths
args = process.argv.slice(1)
manifest_path = args[1]
boo_write = 'update'
//console.log("args.length:",args.length)
//console.log(args)

man = {
	"org_slug": "test",
	"out_path": "./stations/example_station/",
	"station": "example_station.station.json",
	"datfile": "./tables/Example_Station_Table.dat",
	"fields_list": "./vocabulary/test.field_names.json"
}

// Validate args for manifest and spit out an example manifest if needed
if(args[1] == "example") {		
	man_string = JSON.stringify(man,null,2)
	fs.writeFileSync("example.manifest.json",man_string,'utf-8')
	console.log("dat2datastream has exported an 'example.manifest.json'")
	process.exit()
} else if(args.length != 2) {
	console.log("usage: node dat2datastream <manifest_path> (optional, replace path with 'example' to spit out an example manifest and quit)")
	process.exit()
}

/* Manifest 
 Please use meta-list-new-unique-fields.js to create an organization-specific list of fields.
 That list should go under "fields-by-org".
*/
//man = fs.readFileSync(args[1]).toString().split("\r")
//console.log("dat2datastreams: converting "+man.datfile+" for "+man.org_slug+" using "+man.fields_list)
// Manifest File find and load
if (!fs.existsSync(manifest_path)){
	console.log("Didn't find",datfile)
	if (fs.existsSync("tables/"+manifest_path)){
		manifest_path = "tables/"+manifest_path
		console.log("Found in table directory. Modified:",manifest_path)
	} else {
		console.log("Manifest NOT FOUND. exiting.",manifest_path)
		process.exit()
	}
} 
man = JSON.parse(fs.readFileSync(manifest_path))
console.log("Manifest loaded",manifest_path)
console.log(man)

// Organization ID and slug
// NOTE: this should eventually be an online query to dendra via api. Problem is everything else is offline.
org_slug = man.org_slug
console.log("Organization Slug:",org_slug)
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
} else if(org_slug == "lter") {
	orgid = "63c1a0173e9c2780ea16389b"
} else {
	console.log("organization not recognized. quitting.")
	process.exit(1)
}

// Load config files 
vocab_units = JSON.parse(fs.readFileSync('vocabulary/units.vocabulary.json'))
vocab_aggs = JSON.parse(fs.readFileSync('vocabulary/aggregates.vocabulary.json'))
vocab_measurements = JSON.parse(fs.readFileSync('vocabulary/measurement_full.vocabulary.json'))
fields_list = JSON.parse(fs.readFileSync(man.fields_list))

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
	ns_unit = ns_unit.toLowerCase().replace('\n',"").replace(' ','')
	for (let term of vocab_units.terms) {
		s_label = term.label		
		for (var k=0;k<term.abbreviations.length;k++) {
			abbrev = term.abbreviations[k].toLowerCase()
			//console.log("\t",ns_unit,"=?",abbrev)
			if(ns_unit === abbrev) {
				boo_match = true
				s_unit = 'dt_Unit_'+s_label
				console.log("\t MATCH! unit:",ns_unit,"==",abbrev,"-->",s_label)
			}
		}
		if(boo_match == true) { break }
	}
	if(boo_match == false) {
		console.log("\tstandardize_units(",ns_unit,"): not found",s_unit)
	}
	return s_unit
}

// Standardize aggregates returns a Dendra-approved aggregate type
function standardize_aggregates(ns_agg) {
	boo_match = false
	s_agg = "UNKNOWN" //ns_unit
	ns_agg = ns_agg.toLowerCase().replace('\n',"")
	for (let term of vocab_aggs.terms) {
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

// Parses header, tries to match with a unique existing field
function find_measurement_unique_field(header) {                     
	boo_match = false
	measurement_label = "UNKNOWN"
	f = {}
	f.measurement = "UNKNOWN"
	f.variable = "UNKNOWN"
	f.medium = "UNKNOWN"
	f.purpose = "UNKNOWN"
	header = header.toLowerCase().replace('\n',"")
	// loop through list of exact fieldnames looking for header fieldname
	for (let term of fields_list.terms) {	
		if(typeof(term.fields) === "undefined") { continue }
		for (var l=0;l<term.fields.length;l++) {
			field = term.fields[l].toLowerCase()
			//console.log("\t",header,"=?",field)
			if(header == field) {
				boo_match = true
				measurement_label = term.label
				console.log("\t MATCH! measurement:",header,"==",field,"measurement:",measurement_label)
				break	
			}
		}
	}
	if(boo_match == false) {
		console.log("guess_at_measurement_with_field("+header+") NOT FOUND!")
		return f
	}

	// Get the rest of the measurement values from the measurements table
	boo_measurement_label_found = false
	for (let term of vocab_measurements.terms) {
		if(term.label == measurement_label) {
			boo_measurement_label_found = true
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
	if(boo_measurement_label_found == false) {
		console.log("find_measurement_unique_field("+header+") CANNOT FIND measurement.label:",measurement_label)
		process.exit()
	}
	return f
}

//--------------------------------------------------------------------
// DAT File
// Parse the header rows of the .dat file
datfile_content = fs.readFileSync(man.datfile).toString().split("\r")

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


// STATION
// Get Station ID from JSON file
st_json = JSON.parse(fs.readFileSync(man.out_path+man.station+".station.json"))
station_id = st_json._id
console.log("station:",st_json.name,station_id,"manifest:",man.station,"dat:",station_name)
if(typeof st_json._id === 'undefined') {
	console.log("dat2datastream: station_id is UNDEFINED. Please create station in Dendra before processing datastreams. Exiting.")
	process.exit()
}

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
console.log(units)
for (j in headers) {
	ds_json = {}
	//passfail = 'FAIL'
	header = headers[j].replace('"','').replace('"','').replace("\n","")
	ns_unit = units[j].replace('"','').replace('"','').replace("\n","")
	s_unit = standardize_units(ns_unit)
	//console.log(j,header,"ns_unit:",ns_unit,"s_unit:",s_unit)
	ns_agg = aggs[j].replace('"','').replace('"','').replace("\n","")
	s_agg = standardize_aggregates(ns_agg)
	s_function = ""
	tag_list = []

	// Measurements are the most problematic part of dat2datastream 
	// Exact matches are best, but do not always work. Guesses can often go wrong. 
	//g = find_measurement_guess_field(header)  	// Search for measurement using a guess based on fieldname
	s = find_measurement_unique_field(header)  	// Search for measurement using the exact fieldname
	/*
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
	*/
	
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
	name_built = s.name+' '+name_agg  //+' ('+header+')' //.replace(/_/g,'')
	//console.log("name:",name_built)
 	header_fixed_for_influx = header.replace("(","_").replace(")","_").replace(" ","")

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
	          "sc": "\"time\", \""+header_fixed_for_influx+"\"",
	          "utc_offset": -28800,
	          "coalesce": false
	        }
	      },
	      "begins_at": first_date,
	      "path": "/influx/select"
	    }
	  ],
	  "external_refs": [
	  	{
	      "identifier": header,
  	    "type": "field_name"
	  	}
	  ],
	  "tags": tag_list
	}

	// Custom Adjustments to Datastreams
	if(s.measurement == "dq_Measurement_SolarRadiation" && s_unit == "dt_Unit_KiloWattPerSquareMeter") {
		ds_json[j].datapoints_config.actions = { "evaluate": "v = number(1000 * v)" }
		s_unit = "dt_Unit_WattPerSquareMeter"
		name_unit = s_unit.replace('dt_Unit_','')
	}

	/* Report the results before writing
	console.log(passfail+":",name_built,name_agg,name_unit)
	if(passfail != 'MATCH') {
		console.log("\t  HEADER:",header," PARTS:",s.measurement,s.medium,s.variable,s.purpose)
		console.log("\t HEADERV:",header,"PARTSV:",v.measurement,v.medium,v.variable,v.purpose)
	}
	*/

	/*
 	 Update Datastream: if the datastream has already been uploaded
 	 but you want to redo it, set can_overwrite = 'update'.  Then the
 	 datastream_id will be retrieved before the file is overwritten.
 	*/
 	ds_json_path = (man.out_path+header_fixed_for_influx+".datastream.json").toLowerCase()
 	if(boo_write == 'update' && fs.existsSync(ds_json_path)) {
 		ds_temp = JSON.parse(fs.readFileSync(ds_json_path))
 		ds_json[j]._id = ds_temp._id
 	}
	// Write JSON file
	ds_json_string = JSON.stringify(ds_json[j],null,2)
	console.log(header_fixed_for_influx+".datastream.json") // "datastream:",ds_json_path)
	if(boo_write == 'update') {
		fs.writeFileSync(ds_json_path,ds_json_string,'utf-8')
	}
}
//console.log(ds_json)

// FINISH and upload
console.log("DONE! Datastreams created for",st_json.name,"\n")
console.log("Headers total:",headers.length," identified:",header_identified," unknown:",header_unknown," ignored:",header_ignored)
//console.log("VALIDATION PASS:",header_validation_pass,"FAIL:",header_validation_fail,"\n")
console.log("use Dendra Tool to upload:")
console.log("den meta push-datastreams --filespec='"+man.out_path+"*.datastream.json' --save")

