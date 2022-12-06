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
path = require("path")

// Argument processing of paths
args = process.argv.slice(1)
rain_path = args[1]
boo_write = 'update'
//console.log("args.length:",args.length)
//console.log(args)

if(args.length != 2) {
	console.log("usage: node create-cumulative-rainfall <rainfall_datastream_path>")
	process.exit()
}

// Try to load Rainfall datastream
if (!fs.existsSync(rain_path)){
	console.log("Didn't find",rain_path)
	process.exit()
} 
rds = JSON.parse(fs.readFileSync(rain_path))
console.log("rainfall datastream loaded",rain_path)

// Check to make sure the Datastream is Correct
if(typeof(rds._id) === 'undefined') { 
	console.log("Please commit the datastreams using the 'den' CLI first.")
	process.exit()
}
if(rds.terms.dq.Measurement != 'Rainfall' || rds.terms.ds.Aggregate != 'Sum') {
	console.log("Incorrect metadata terms. Quitting. dq.Measurement: Rainfall?",rds.terms.dq.Measurement,"ds.Aggregate: Sum?",rds.terms.ds.Aggregate)
	process.exit()	
}

// Copy the datastream then modify
cds = rds
cds.name = "Rainfall Cumulative"
cds.terms.ds.Aggregate = "Cumulative"
cds.source_type = "deriver"
cds.datapoints_config[0].params.query = {}
cds.derivation_description = "Cumulative rainfall for a water year (Oct 1 to Sep 30"
cds.derivation_method = "wyCumulative"
cds.derived_from_datastream_ids = [rds._id]
delete cds._id 

// Write JSON file
cds_string = JSON.stringify(cds,null,2)
station_path = path.dirname(rain_path)
cumrain_path = station_path+"/rainfall_cumulative.datastream.json"
if(boo_write == 'update') {
	fs.writeFileSync(cumrain_path,cds_string,'utf-8')
	console.log("cumulative rainfall writen:",cumrain_path)
}
console.log(rds)
console.log("Please use command line utility to upload the datastream. Copy and paste the following:")
console.log("den meta create-datastream --file=",cumrain_path,"--save")
