/*	
Functions for Dendra Metadata
@author: collin bode
@email: collin@berkeley.edu
original function set: transform_functions.js
*/
axios = require("axios")
fs = require("fs")
path = require("path")


// Text parsing and uniquing
function onlyUnique(value, index, self) { 
    return self.indexOf(value) === index;
}

exports.getUniqueList = function(list) {
  unique_list = list.filter(onlyUnique)
  return unique_list
}

exports.capitalize = function(name) {
  capped = name.charAt(0).toUpperCase() + name.slice(1)
  return capped
}

exports.recurseDir = function(dir,filepath_list,regex) {
  // example use: tr.recurse_dir(start_path,filepath_list,RegExp(/^Precip((?!seasonal).)*$/i))
  // regex starts with precip and excludes seasonal.
  // filepath_list is a paired array of parent path and filename that is the returned values.
  fs.readdirSync(dir).forEach(file => {
    let full_path = path.join(dir, file)
    if (fs.lstatSync(full_path).isDirectory()) {
      exports.recurseDir(full_path+path.sep,filepath_list,regex)
    } else {
      if(regex == "") {
        filepath_list.push([dir,file])
      } else if(regex.test(file)) {
        filepath_list.push([dir,file])
      }
    }  
  })
}

exports.getOrgSlug = function(organization_id) {
  // assumes specific file structure
  opath = "../data/common/organization/"
  o_files = fs.readdirSync(opath)
  for(var o=0;o<o_files.length;o++) {
    o_file = o_files[o]
    if(o_file.match(/.json$/)) {
      o_json = JSON.parse(fs.readFileSync(opath+o_file))
      if(organization_id == o_json._id) {
        return o_json.slug
      }
    } // else {
    //  console.log(o_file,"not json!")
    //}
  }
}

exports.getExternalRef = function(internal_refs,ref_type) {
  if(typeof(internal_refs) === 'undefined') {
    return ""
  }
  for(var k=0;k<internal_refs.length;k++) {
    //console.log("external_refs[",k,"]:",de_json.external_refs[k])
    if(internal_refs[k].type == ref_type) {
      //console.log("MATCH!",ref_type,"==",de_json.external_refs[k].type,"returning identifier:",de_json.external_refs[k].identifier)
      return internal_refs[k].identifier
    }
  }
}

exports.getODMExternalRef = function(table,term) {
  vocab_ext_refs = JSON.parse(fs.readFileSync('vocabulary_odm/odm2external_refs.vocabulary.json'))  
  for(l in vocab_ext_refs) {
    //console.log(table,term,"=?=",vocab_ext_refs[l].odm_table,vocab_ext_refs[l].odm_field)
    if(table == vocab_ext_refs[l].odm_table && term == vocab_ext_refs[l].odm_field) {
      //console.log("MATCH!",key,"==", vocab_ext_refs[l].odm_field,"-->",vocab_ext_refs[l].external_refs)
      return vocab_ext_refs[l].external_refs
    }
  }    
}

exports.buildODMExternalRefs = function(odm_list) {
  // Matches a list of ODM variables to the Dendra equivalent for the external_references
  vocab_ext_refs = JSON.parse(fs.readFileSync('vocabulary_odm/odm2external_refs.vocabulary.json'))
  internal_refs = []
  for(key in odm_list) {
    //console.log(key,odm_list[key])
    is_match = false
    for(l in vocab_ext_refs) {
      //console.log(key,"=?=", vocab_ext_refs[l].odm_field,"odm_list[key]:",odm_list[key])
      if(key == vocab_ext_refs[l].odm_field && odm_list[key] != "NULL") {
        //console.log("MATCH!",key,"==", vocab_ext_refs[l].odm_field,"-->",vocab_ext_refs[l].external_refs)
        is_match = true
        internal_refs.push({
          "identifier": odm_list[key].toString(),
          "type": vocab_ext_refs[l].external_refs
        })
      }    
    }
    if(is_match == false) {
      console.log("\t NO MATCH FOR",key)
    }  
  }
  return internal_refs
}


exports.buildODMExtRefFromTable = function(internal_refs,input_path,odm_ref_type) {
  /*
  External References
  After datastream is created, append to the external references the rest of the ODM tables
  All table values are then added using IDs from SeriesCatalog 
  Each value is checked against the existing externeral_refs in case it already exists 

  odm_ref_type is the converted variable name for external references. 
  It is structured similar to an inverted web address: his.odm.<tablename>.<FieldName> 
  */
  odmid = exports.getExternalRef(internal_refs,odm_ref_type)  // ODM Primary Key 
  table = odm_ref_type.split(".")[2]          // ODM Database table, exported
  odm_field = odm_ref_type.split(".")[3]      // ODM Field Name within the table
  console.log("\n\n internal_refs adding ODM tables:",odm_field,"odm_ref_type:",odm_ref_type,"ODM ID:",odmid)

  // Look up the ODM Table extract one row of fields using primary key
  field_list = []
  odm_fields = JSON.parse(fs.readFileSync(input_path+table+".json"))
  for(i in odm_fields) {
    //console.log(odmid,odm_ref_type,odm_field,odm_fields[i][odm_field])
    if(odmid == odm_fields[i][odm_field]) {
      //console.log("MATCH! target ID",odmid,"==",odm_fields[i][odm_field])
      field_list = odm_fields[i]
      break
    }
  }
  console.log("\t ODM vocabulary to add:",field_list)
  
  // Combine the odm_table values with the field_list to update external_refs
  for(odm_field in field_list) {
    field_value = field_list[odm_field]
    //console.log(odm_field,field_list[odm_field])
    exref = exports.getODMExternalRef(table,odm_field)
    console.log("\t exref:",exref,"odm_field:",odm_field,"value:", field_value)

    // Some fields exist in SeriesCatalog are not in the export tables - they are from controlled vocabulary
    // VariableUnitsID, TimeUnitsID,
    if(typeof(exref) === 'undefined') {
      console.log("\t\t WARNING! UNRECOGNIZED FIELD:",odm_field,field_value,exref)
      continue
    }
    // Skip NULL values
    if(field_list[odm_field] == 'NULL') {
      console.log("\t\t value is NULL. Skipping")
      continue
    }
    // If not in external_refs already, add it
    result = exports.getExternalRef(internal_refs,exref)
    //console.log(exref,"in external_refs?",result)
    if(typeof result === 'undefined') {
      new_exref = { "identifier": field_list[odm_field].toString(), "type": exref }
      internal_refs.push(new_exref)
      console.log("\t\t pushed:",new_exref)
    } else {
      console.log("\t\t already exists. skipping.")
    }
  }
  return internal_refs
}


exports.getContVocabLabel = function(name,filename) {
  // Dendra controlled vocabulary have a label, which has not punctuation or spaces
  // and a name, which is human readable.  ODM terms match name, but Dendra uses label 
  // for queries.
  vocab_variables = JSON.parse(fs.readFileSync('vocabulary_odm/'+filename))
  for(i in vocab_variables.terms) {
    if(name == vocab_variables.terms[i].name) {
      //console.log("MATCH!",name,"==",vocab_variables.terms[i].name)
      return vocab_variables.terms[i].label
    }
    if(name == vocab_variables.terms[i].label && filename == "odm-qualitycontrollevels.vocabulary.json") {
      return vocab_variables.terms[i].name
    }
  }
}

/*
  Networked Functions: API Access to Dendra via axios
*/
exports.getOrganizations = async function() {
  const baseUrl = 'http://api.dendra-dev-ins1.cuahsi.dendra.science/v2'

  // Authenticate
  const auth = await axios({
    method: 'POST',
    url: `${baseUrl}/authentication`,
    data: {
      email: 'metahuman@dendra.science',
      strategy: 'local',
      password: process.env.DENDRA_DEV
    }
  })

  // Pull from Dendra
  const r = await axios({
    method: 'GET',
    url: `${baseUrl}/organizations`,
    params: {},
    headers: { Authorization: auth.data.accessToken }
  })
  result = r.data.data
  //return result
  //console.log(result)
  internal_org_list = []
  for(i in result) {
    internal_org_list.push({
      "name": result[i].name,
      "_id": result[i]._id,
      "slug": result[i].slug
    })
  }
  return internal_org_list
}

exports.findOrg = async function(orgslug) {
  const org_list = await exports.getOrganizations()
  //console.log(org_list)
  for(j in org_list) {
    //console.log(orgslug,"=?=",org_list[j].slug)
    if(orgslug == org_list[j].slug) {
      console.log("MATCH! ORGSLUG FOUND:",orgslug,"==",org_list[j].slug)
      return org_list[j]
    }
  }
  return "NO MATCH!"
}