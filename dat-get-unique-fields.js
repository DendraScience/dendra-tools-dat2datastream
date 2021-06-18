/*
dat-get-unique-fields.js
Purpose: For a particular organization, develop a list pairing measurements with their fields.
Steps:
1. run through a list of .dat files and collect all the fieldnames in the list
2. If there a list that already exists, check to see which ones are already on this list
3. Remove redundant fields.  
4. Save list of new unique fields
5. Run each field through the guessing algorithm and pair the field with a measurement
6. Insert the pair into "terms" of the org.unique_fields.json file.  
		-- if the measurement already exists, then append the field to the fields list
*/
fs = require("fs")

// Argument processing of paths
args = process.argv.slice(1)
org_slug = args[1]
datdir = args[2]
boo_write = true
new_fields = []
if(args.length != 3) {
	console.log("usage: node dat-get-unique-fields <organization> <dat file directory>)")
	process.exit()
}
console.log("dat-unique-fields: organization:",org_slug,", checking directory:",datdir)

// Load existing lists, create new if one doesn't exist
vocab_measurements = JSON.parse(fs.readFileSync('vocabulary/measurement_full.vocabulary.json'))
org_unique_fields_path = "fields/"+org_slug+".field_names_unique.json"
if(fs.existsSync(org_unique_fields_path)) {
	unique_fields = JSON.parse(fs.readFileSync(org_unique_fields_path))
} else {
	unique_fields = {
		"org_slug": org_slug,
		"terms": [
			{
	      "fields": ["timestamp","record","day_of_year","_day_of_year","hourmin","sta_id","stationnum","cvmeta"],
	      "label": "IGNORE"
	    }
    ]
	}
	unique_field_string = JSON.stringify(unique_fields,null,2)
	fs.writeFileSync("fields/"+org_slug+".field_names_unique.json",unique_field_string,'utf-8')
}

// Compare a header field to the organization's list of known fields.
// If it is not there, register it as new
function is_field_unique(header) {                     
	is_unique = true
	header = header.toLowerCase().replace('\n',"")
	for (let term of unique_fields.terms) {	
		if(typeof(term.fields) === "undefined") { 
			console.log("ERROR. NO FIELDS IN TERM")
			process.exit() 
		}
		if(is_unique == false) { break }
		for (let existing_field of term.fields) {
			existing_field = existing_field.toLowerCase()
			//console.log("\t",header,"=?",existing_field)
			if(header == existing_field) {
				is_unique = false
				//console.log("\t!",header,"==",existing_field)
			break
			}
		}
	}
	if(is_unique == true) {
		//console.log("is_field_unique("+header+") NOT FOUND! Assuming unique.")
	}
	return is_unique
}

// Parses header fieldname, returns a Dendra-approved measurement, medium, variable or Unknown 
function find_measurement_guess_field(header) {
 	header = header.toLowerCase().replace('\n',"").replace(" ","")
	for (let term of vocab_measurements.terms) {	
		if(typeof(term.abbreviations) === "undefined") { continue }
		for (let abbrev of term.abbreviations) {
			abbrev = abbrev.toLowerCase()
			//console.log("\t",header,"=?",abbrev)
			if(header.match(abbrev)) {
				console.log("\t!",header,"==",abbrev,"-->",term.label)
				return term.label
			}
		}
	}
	console.log("find_measurement_guess_field("+header+") NOT FOUND!")
	return "UNKNOWN"
}


//-------------------------------------------------------

// Check each DAT file in directory for unique fields
fs.readdirSync(datdir).forEach(datfile => {
	if(datfile.match(".dat")) {
		console.log(datfile)
		// Parse the header rows of the .dat file
		datfile_content = fs.readFileSync(datdir+datfile).toString().split("\r")
		headers = datfile_content[1].split(',')
		console.log(datfile,headers.length)
		//console.log("\t headers:")
		for (var i=0;i<headers.length;i++) {
			field = headers[i].replace(/"/g,'').replace(/ /g,'').replace('\n',"")
			is_new_field = is_field_unique(field)
			console.log("\t header:",field,"is_new:",is_new_field)
			if(is_new_field == true) {
				new_fields.push(field)
				//console.log("\t header:",field)
			}
		}
	}
})

// Remove duplicates from list
// new_fields will have duplicates of a new field
new_unique_fields = Array.from(new Set(new_fields))
//-------------------------------------------------------

// Now that we have a unique list of new fields
// Guess what measurement is correct for them
new_terms = {
	"org_slug": org_slug,
	"terms": [
		{
      "fields": [],
      "label": "UNKNOWN"
    }
  ]
}

for (let nufield of new_unique_fields) {
	measurement = find_measurement_guess_field(nufield)
	// Loop through new_terms and add the new field if it finds a measurement match
	measurement_already_exists = false
	for (let term of new_terms.terms) {
		if(term.label == measurement) {
			term.fields.push(nufield)
			measurement_already_exists = true
			break
		}
	}
	// Add new term if it doesn't yet exist for this organization
	if(measurement_already_exists == false) {
		new_terms.terms.push( { "fields": [nufield], "label": measurement } )
	}
}

// Write JSON file
if(boo_write == true) {
	new_terms_string = JSON.stringify(new_terms,null,2)
	fs.writeFileSync("fields/"+org_slug+".field_names_new.json",new_terms_string,'utf-8')
}

// List new headers found
console.log("New headers found:",new_fields.length,"Unique headers found:",new_unique_fields.length)
console.log("DONE!")
