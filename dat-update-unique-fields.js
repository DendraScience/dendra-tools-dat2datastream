/*
dat-update-unique-fields.js
Purpose: Take two JSON lists of measurements & fieldnames associated with that measurement 
and merge them into a single document.
*/

fs = require("fs")
boo_write = true

// Argument processing of paths
args = process.argv.slice(1)
fieldname_path = args[1]
merge_path = args[2]
if(args.length != 3) {
	console.log("usage: node dat-update-unique-fields <organization field name list> <new field names>)")
	process.exit()
}
//console.log("field name path:",fieldname_path)
//console.log("merge list path:",merge_path)

function does_field_already_exist(newfield,fields) {
	if(typeof(fields) === 'undefined') { return false }
	for (let field of fields) {
		if(newfield == field) {
			return true
		}
	}
	return false
}

// Find and load Fieldnames
if (!fs.existsSync(fieldname_path)){
	if (fs.existsSync("fields/"+fieldname_path)){
		fieldname_path = "fields/"+fieldname_path
		console.log("Found in fields directory. Modified:",fieldname_path)
	} else {
		console.log("field name list NOT FOUND. exiting.",fieldname_path)
		process.exit()
	}
} 
fields_list = JSON.parse(fs.readFileSync(fieldname_path))
console.log("Loaded field names:",fieldname_path,"with count:",fields_list.terms.length)

// Find and load Merge List
if (!fs.existsSync(merge_path)){
	if (fs.existsSync("vocabulary/"+merge_path)){
		merge_path = "vocabulary/"+merge_path
		console.log("Found in table directory. Modified:",merge_path)
	} else {
		console.log("field name list NOT FOUND. exiting.",merge_path)
		process.exit()
	}
} 
merge_list = JSON.parse(fs.readFileSync(merge_path))
console.log("Loaded merge list:",merge_path, "with count:",merge_list.terms.length)


// loop through the merge list
// if a measurement exists, append its abbreviations
// if it doesn't, add a new term
for (let merge of merge_list.terms) {
	// Loop through new_terms and add the new field if it finds a measurement match
	measurement_already_exists = false
	for (let term of fields_list.terms) {
		if(term.label == merge.label) {
			for (let field of merge.fields) {
				if(does_field_already_exist(field) == false) {
					term.fields.push(field)
				}
			}
			measurement_already_exists = true
			break
		}
	}
	// Add new term if it doesn't yet exist for this organization
	if(measurement_already_exists == false) {
		fields_list.terms.push( merge )
	}
}

console.log("DONE. Fields list has count:",fields_list.terms.length)

// Write JSON file
if(boo_write == true) {
	fields_list_string = JSON.stringify(fields_list,null,2)
	fs.writeFileSync(fieldname_path,fields_list_string,'utf-8')
}
