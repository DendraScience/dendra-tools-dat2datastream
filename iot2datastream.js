/* 
* SWARM & IoT device to datastream converter
* @author=collin bode
* @date=2022-11-08
* @source_date=2018-09-27
*  - dat2datastream-original.js is the original code. 
*  - Takes a station file and a CSV table as input
*  - Manifest file lists the organization, station file, and CSV file
*  - Creates datastreams for each channel - Measurement pair in the table
*/
const fs = require("fs")
const { parse } = require('csv-parse/sync')
boo_write = 'update'  // 'update'

// Argument processing of paths
args = process.argv.slice(1)
manifest_path = args[1]

man = {
	"org_slug": "test",
	"out_path": "./stations/example_station/",
	"station_file": "example_station.station.json",
	"measurements_file": "example.field-table.csv"
}

// Validate args for manifest and spit out an example manifest if needed
if(args[1] == "example") {		
	man_string = JSON.stringify(man,null,2)
	fs.writeFileSync("example.manifest.json",man_string,'utf-8')
	console.log("iot2datastream has exported an 'example.manifest.json'")
	process.exit()
} else if(args.length != 2) {
	console.log("usage: node iot2datastream <manifest_path> (optional, replace path with 'example' to spit out an example manifest and quit)")
	process.exit()
}

/* Manifest 
 Please use meta-list-new-unique-fields.js to create an organization-specific list of fields.
 That list should go under "fields-by-org".
*/
// Manifest File find and load
if (!fs.existsSync(manifest_path)){
	console.log("Manifest NOT FOUND. exiting.",manifest_path)
	process.exit()
} 
man = JSON.parse(fs.readFileSync(manifest_path))
console.log("Manifest loaded",manifest_path)
console.log(man)

// STATION
// Load Station JSON and pull values from general_config 
st = JSON.parse(fs.readFileSync(man.out_path+man.station_file))
console.log("station:",st.name,st._id)
if(typeof st._id === 'undefined') {
	console.log("iot2datastream: station_id is UNDEFINED. Please create station in Dendra before processing datastreams. Exiting.")
	process.exit()
}
influx_table = st.general_config.table_name
influx_station = "swarm_device_"+st.general_config.swarm_device_id.toString()

// Quit if station doesn't have an org
if(typeof st.organization_id === "undefined") {
	console.log("organization is undefined. quitting.")
	process.exit(1)
}

// Load config files 
vocab_units = JSON.parse(fs.readFileSync('vocabulary/units.vocabulary.json'))
vocab_aggs = JSON.parse(fs.readFileSync('vocabulary/aggregates.vocabulary.json'))
vocab_measurements = JSON.parse(fs.readFileSync('vocabulary/measurement_full.vocabulary.json'))

// Functions
function get_measurement_terms(measurement,terms) { 
	if(measurement == 'Unknown') {
		console.log("Measurement is ",measurement,"creating stub")
		return terms
	}                    
	for (v in vocab_measurements.terms) {
		if(vocab_measurements.terms[v].label == measurement) {
			terms.dq.Measurement = measurement
			terms.ds.Medium = vocab_measurements.terms[v].medium
			terms.ds.Variable = vocab_measurements.terms[v].variable
			if(typeof vocab_measurements.terms[v].data_purpose !== 'undefined') {
				terms.dq.Purpose = vocab_measurements.terms[v].data_purpose
			}
			return terms
		}
	}
	console.log("NO MATCH! STOPPING",measurement)
	process.exit()
}


//--------------------------------------------------------------------
// Measurements List CSV
// Load IoT Sensor Table (CSV)
console.log(man.out_path+man.measurements_file)
csv = fs.readFileSync(man.out_path+man.measurements_file)
const measurements_list = parse(csv, { columns: true, relax_quotes: true, skip_empty_lines: true, ltrim: true, rtrim: true })

// Loop through Measurements and Channels list
// ns_ prefix means 'non standard', s_ prefix means 'standardized' 
for (r in measurements_list) {
	m = measurements_list[r]
	console.log(m.description)
	//console.log(m)
	ds = {}
	
	// Construct basic metadata
	terms = {
	    "dq": {
	      "Measurement": "Unknown"
	    },
	    "ds": {
	      "Medium": "Unknown",
	      "Variable": "Unknown"
	    }
	}
	if(typeof m.unit !== 'undefined' && m.unit != '') {
		terms.dt = {
			"Unit": m.unit
		}
	}
	if(typeof m.aggregate !== 'undefined' && m.aggregate != '') {
		terms.ds.Aggregate = m.aggregate
	}
	terms = get_measurement_terms(m.measurement,terms)
	
	// Create datastream JSON
	ds = {
	  "is_enabled": true,
	  "name": m.description,
	  "organization_id": st.organization_id,
	  "source_type": "sensor",
	  "state": "ready",
	  "station_id": st._id,
	  "datapoints_config": [
	    {
	      "params": {
	        "query": {
	          "api": man.org_slug,
	          "db": "swarm_device_"+st.general_config.swarm_device_id,
	          "fc": st.general_config.table_name,
	          "sc": "\"time\", \""+m.channel+"\"",
	          "utc_offset": st.utc_offset,
	          "coalesce": false
	        }
	      },
	      "begins_at": st.general_config.begins_at,
	      "path": "/influx/select"
	    }
	  ],
	  "terms": terms,
	  "external_refs": [
	  	{
	    	"identifier": m.channel,
  	    	"type": "swarm_sdi_channel"
	  	}
	  ]
	}
	// Equipment: if you have a thing_type_id, add it
	if(typeof m.thing_type !== 'undefined' && m.thing_type != "") {
		ds.thing_type_id = m.thing_type 
	}
	
	/*
 	 Update Datastream: if the datastream has already been uploaded
 	 but you want to redo it, set can_overwrite = 'update'.  Then the
 	 datastream_id will be retrieved before the file is overwritten.
 	*/
 	ds_path = man.out_path+(m.channel+"."+m.measurement+".datastream.json").toLowerCase().replace(" ","-")
 	if(boo_write == 'update' && fs.existsSync(ds_path)) {
 		ds_temp = JSON.parse(fs.readFileSync(ds_path))
 		ds._id = ds_temp._id
 	}
	// Write JSON file
	ds_string = JSON.stringify(ds,null,2)
	console.log("datastream:",ds_path)
	if(boo_write == 'update') {
		fs.writeFileSync(ds_path,ds_string,'utf-8')
	}
}
//console.log(ds)

// FINISH and upload
console.log("DONE! Datastreams created for",st.name,"\n")
//console.log("Headers total:",headers.length," identified:",header_identified," unknown:",header_unknown," ignored:",header_ignored)
//console.log("VALIDATION PASS:",header_validation_pass,"FAIL:",header_validation_fail,"\n")
console.log("use Dendra Tool to upload:")
console.log("den meta push-datastreams --filespec='"+man.out_path+"*.datastream.json' --save")

