var prefix;
var day;
var Users = [];
var Groups = [];
var Teams = [];
var Origin = new Date();
const oneDay = 86400000;   // 24 * 60 * 60 * 1000 One day in miliseconds 
var maxControlObjects = 30; //Maximum number of checksums to preserve
var maxDaysToProcess = 10; 
var s3Bucket ="log-uploader";
var s3Region = "us-east-2";
const s3Service = 'S3Bucket' 
const fName = "archiveEvents";
const username = "Not Set";
const apiKey = "Not set";

function ArchiveException(message) {
	this.message = message;
	this.name = 'ArchiveException'
}

async function cloudAuthenticatedRequest(url,prefix){


    if (typeof(username) == 'undefined' || typeof(apiKey) =='undefined')
    	throw `cloudAuthenticateRequest:  "${prefix}-pub" or "${prefix}-sec" are not defined.`;
    return context.http
      .get({ url:url})
      .then(resp => {
        const authHeader = resp.headers['Www-Authenticate'].toString();

        const realm = authHeader.match(/realm="(.*?)"/)[1];
        const nonce = authHeader.match(/nonce="(.*?)"/)[1];
        const qop = authHeader.match(/qop="(.*?)"/)[1];

        const ha1 = utils.crypto.hash('md5', `${username}:${realm}:${apiKey}`).toHex();

        const path = url.match(/:\/\/.*?(\/.*)/)[1];

        const ha2 = utils.crypto.hash('md5', `GET:${path}`).toHex();
        const cnonce = Math.random().toString().substr(2, 14);
        const nc = '00000001';

        const response = utils.crypto.hash('md5', `${ha1}:${nonce}:${nc}:${cnonce}:${qop}:${ha2}`).toHex();

        const digestHeader = `Digest username="${username}", realm="${realm}", nonce="${nonce}", uri="${path}", qop=${qop}, nc=${nc}, cnonce="${cnonce}", response="${response}", algorithm=MD5`;

        return context.http.get({ url: url, headers: { 'Authorization': [ digestHeader ] } })
          .then(({ body }) => {
        	  result = body.text ? JSON.parse(body.text()) : { links: [], results: [] }
        	  if (typeof result.error == 'number') {
        		  console.error(`Error: ${result.error} "${result.detail}" fetching ${url}.`);
        		  return(null);
        	  }
        	  return(result);
          })
          .catch(err => {return Promise.reject(`cloudRequest: Failed from ${url}: \n-->${err}`);});
      })
      .catch(err => {
      	return Promise.reject(`cloudRequest: Failed from ${url}: \n-->${err}`);
      });

    
};

async function writeS3Object(key,value){

	
	  const s3Service = context.services.get(s3Service).s3(s3Region);
	  var payload = "";
	  for (i=0;i<value.length;i++)
	    payload += EJSON.stringify(value[i]);
	  const s2 = utils.crypto.hash('md5',payload).toBase64();
	  //console.log(`MD5 is ${s2}`);
	  return s3Service.PutObject({
	    'Bucket': s3Bucket,
	    'Key': key,
	    'ContentType': "application/json",
	    'ContentMD5': s2,
	    'Body': payload
	  })
	  .then(putObjectOutput => {
		  let objectData = { "Day": null,
		  			"Etag": putObjectOutput.ETag,
		  			"Events": value.length,
		  			"KEY": key,
		  			"MD5": s2 }
	    return objectData;
	  })
	  //.catch(error => {throw new Error(error.message)}); 
	  .catch(error => {
	    var parts = error.message.split("\n");
	    throw new Error(parts[0]);
	    
	  });
	};

setQueryDate = function(day) {
  //console.log(`QueryDate ${day}`);	
  var queryDate = {"minDate": 0,"maxDate": 0};
  var minDate = day.getTime() - (day.getTime() % 86400000);  // Make it midnight UTC
  var maxDate = minDate + 86400000 - 1;  // Range is one day
  queryDate.minDate = new Date(minDate).toISOString();
  queryDate.maxDate = new Date(maxDate).toISOString();
  return(queryDate);
};

async function getFirstDate(item) {
  let earliest = new Date();
  let thisURL = `${item}/events?eventType=GROUP_CREATED`;
  //console.log(thisURL);
  return cloudAuthenticatedRequest(thisURL, prefix)
    .then(response => {
      if (response == null) {
    	  let result = earliest.getTime()+oneDay;
    	  return(result); //tomorrow
      }
      for(let i=0;i<response.results.length;i++){
        //console.log(`Found ${i} ${response.results[i].created} earliest ${earliest}`);
        if (new Date(response.results[i].created) < earliest) earliest = new Date(response.results[i].created);
      }
      //console.log(earliest.toString()+thisURL);
      return(earliest);
    })
    .catch(err => {
    	return Promise.reject(`getFirstDate: Failed to get first event from ${thisURL}: \n-->${err}`);
    });
}

async function getEarliestDate(urls) {
    const getPromises = urls.map(url => getFirstDate(url));
    return Promise.all(getPromises)
    .then(response => {
    	let earliest = response[0];
    	for (i=1;i<response.length;i++) {
    		if (earliest > response[i]) earliest = response[i];
    		console.log(`Getting: ${earliest} ${response[i]}`);
    	}
    	let now = new Date()
    	if (now < earliest) return(Promise.reject("No Projects found"));
     	return(earliest);
    })
    .catch(err => {
    	return Promise.reject(`getEarliestDate: Failed to get earliest date: \n-->${err}`);
    });
}

async function getUsers(item) {
  let users = [];
  let thisURL = `${item}/users`;
  //console.log(thisURL);
  return cloudAuthenticatedRequest(thisURL, prefix)
    .then(response => {
      if (response == null) return(Promise.reject(`getUsers: ${thisURL}`));
      for(let i=0;i<response.results.length;i++){
        users[`${response.results[i].id}`] = {Name: `${response.results[i].firstName} ${response.results[i].lastName}`, Email: `${response.results[i].emailAddress}`};
      }
      //console.log(Object.keys(users));
      return(users);
    })
    .catch(err => {
    	return Promise.reject(`getUsers: Failed to get users info from ${thisURL}: \n-->${err}`);
    });
}

async function getGroups(item) {
  let groups = [];
  let thisURL = `${item}/groups`;
  //console.log(thisURL);
  return cloudAuthenticatedRequest(thisURL, prefix)
    .then(response => {
      if (result == null) Promise.reject(`getGroups: ${thisURL}`);
      for(let i=0;i<response.results.length;i++){
        groups[`${response.results[i].id}`] = {Name: response.results[i].name, clusterCount: response.results[i].clusterCount, created:response.results[i].created};
      }
      return(groups);
    })
    .catch(err => {
    	return Promise.reject(`getGroups: Failed to get groups from ${thisURL}: \n-->${err}`);
    });
}

async function getTeams(item) {
  let teams = [];
  let thisURL = `${item}/teams`;
  //console.log(thisURL);
  return cloudAuthenticatedRequest(thisURL, prefix)
    .then(response => {
      if (result == null) Promise.reject(`getTeams: ${thisURL}`);
      for(let i=0;i<response.results.length;i++){
        teams[`${response.results[i].id}`] = {Name: response.results[i].name};
      }
      return(teams);
    })
    .catch(err => {
    	return Promise.reject(`getTeams: Failed to get teams from ${thisURL}: \n-->${err}`);
    });
}

async function getAllMeta(org){
  
  let users = getUsers(org);
  let groups = getGroups(org);
  let teams = getTeams(org);
  return Promise.all([users,groups,teams])
  .then(response => {
    Users = response[0];
    Groups = response[1];
    Teams = response[2];
    Origin = response[3];
    //console.log(response[3].toString());
    return(true);
  })
  .catch(err => {
	  //console.log(`GetMat: ${err}`);
  	  return Promise.reject(`getAllMeta: Error collecting Meta Data: \n-->${err}`);
  });

}

dataDogEvent = function(title,message,tag,severity){
  let url = "https://api.datadoghq.com/api/v1/events?api_key="+context.values.get("DataDogAPIKey");
  let method = "POST";
  let alertType = severity || "info";
  let DDmessage = message.replace(/\n/g,"\\n");
  DDmessage = DDmessage.replace(/"/g,"\\\"");
  let postData = `{ "title": "${title}","text":"${DDmessage}","priority": "normal", "tags": ["Atlas:${tag}"],"alert_type": "${alertType}" }`;
  //console.log(postData);
  
  return context.http.post({
    url: url,
    body: postData,
    headers: { "Content-Type": [ "application/json" ] }
  })
  .then(response => {
    const json_body = EJSON.parse(response.body.text());
    if (typeof(json_body.status) == 'string' && json_body.status == "ok")
    	return json_body;
    Promise.reject(`Datadog: ${json_body.errors}`);
  })
  .catch(err => {
	return Promise.reject(`DataDog: ${err}`);
  });
};

getMyOrg = function() {
  
  const url = `https://cloud.mongodb.com/api/atlas/v1.0/orgs`; 
  return cloudAuthenticatedRequest( url, prefix)
    .then(response => {
      if (response.totalCount == 1) 
        return response.results[0].links[0].href.toString();
    })
    .catch(err => {
    	  return Promise.reject(`getMyOrg: Error getting Org info: \n-->${err}`);
    });
};

getMyProjects = function() {
  content = [];
  const url = `https://cloud.mongodb.com/api/atlas/v1.0/groups`; 
  return cloudAuthenticatedRequest( url, prefix)
    .then(response => {
      for(var i=0;i<response.results.length;i++){
        content.push(response.results[i].links[0].href.toString());
      }
      return(content);
    })
    .catch(err => {
  	  return Promise.reject(`getMyProjects: Error getting Project info: \n-->${err}`);
  });
};

async function getURLS() {
    return Promise.all([
    getMyOrg(prefix),
    getMyProjects(prefix)
  ]).then(results => {
    var [orgurl,projurl] = results;
    var result = [];
    result.push(orgurl);
    return(result.concat(projurl));
  })
  .catch(err => {
    return Promise.reject(`getURLS: Error getting endpoints: \n-->${err}`);
  });
};

async function getEvents(item,dateRange) {
  let content = [];
  let thisURL = `${item}/events?minDate=${dateRange.minDate}&maxDate=${dateRange.maxDate}`;
  //console.log(thisURL);
  return cloudAuthenticatedRequest( thisURL, prefix)
    .then(response => {
      //console.log(`Got ${response.results.length}, totoal count ${response.totalCount}`)
      for(var i=0;i<response.results.length;i++){
    	let oneEvent = response.results[i];
    	if ('groupId' in oneEvent) {
    		oneEvent.group = Groups[oneEvent.groupId];
    	}
    	if ('userId' in oneEvent) {
    		oneEvent.user = Users[oneEvent.userId];
    	}
    	if ('teamId' in oneEvent) {
    		oneEvent.team = Users[oneEvent.teamId];
    	}
        content.push(response.results[i]);
      }
      //console.log(`${item} returning ${content.length}`)
      return(content);
    })
    .catch(err => {return Promise.reject(`Failed to get events from $(item}: ${err}`)});
};

async function getLastDate(s3Service,objName) {
    return s3Service.GetObject({
         'Bucket': s3Bucket,
         'Key': `${objName}`
    })
    .then(getObjectOutput => {
      const objectDate = getObjectOutput.Body.text();
      let cntrl = EJSON.parse(objectDate);
      return(cntrl);
    })
    .catch(console.error);
    
};


async function getControlObj(prefix){

	  const s3Service = context.services.get('S3Bucket').s3(s3Region);
	  return  s3Service.ListObjectsV2({
	    'Bucket': s3Bucket,
	    'MaxKeys': 10,
	    'Prefix': `Control-${prefix}`
	  })
	  .then(ListObjectsV2Output => {
	    if (ListObjectsV2Output.KeyCount != 1)
	      return Promise.reject(`${ListObjectsV2Output.KeyCount} matching control files.`);
	    else {
	      return getLastDate(s3Service,ListObjectsV2Output.Contents[0].Key)
	      .then(statusObj => {
	    	  //console.log(`Got ${ListObjectsV2Output.Contents[0].Key} Last Update ${statusObj.lastRun}.`);
	    	  return statusObj;
	      })
	      .catch(console.error);
	    }
	  })
	  .catch(console.error);  
	};
	
getOneDay = function(urls,day){
    
	let queryDate = setQueryDate(day);
	let timer = new Date();
    console.log(`Atlas Event Archiver starting to process ${prefix} for ${queryDate.minDate}`);
	const getPromises = urls.map(url => getEvents(url,queryDate));
    return Promise.all(getPromises)
    .then(response => {
        let events = [];
        for (i=0;i<response.length;i++) {
        	events = events.concat(response[i]);
        	
        }
        //console.log(`${events.length} in events`);
        let objectName = queryDate.minDate.substring(0,4)+"/"+queryDate.minDate.substring(5,7)+"/"+queryDate.minDate.substring(8,10)+"-"+prefix;
        //console.log(objectName);
        return writeS3Object(objectName,events)
        .then(response => {
              let duration = new Date();
              response.Day = queryDate.minDate;
              //console.log(EJSON.stringify(response));
              duration = (duration - timer)/1000;
              //console.log(`Atlas Event Archiver finished: Processing ${prefix} for ${queryDate.minDate} in ${duration}s`);
             dataDogEvent("Atlas Event Archiver completed one day.",`Processing ${prefix} for ${queryDate.minDate} in ${duration}s`,prefix);
            return(response);
        })
        .catch(err => dataDogEvent("Atlas Event Archiver failed!",`Processing ${prefix} for ${queryDate.minDate} Status: ${err}`,prefix,"error"));
    });
}
	
async function doit(prefix_in){
	  try {
		  prefix = prefix_in;
		  var timer = new Date();
		  var cntrlFile = null;
		  dataDogEvent("Atlas Event Archiver starting",`Processing ${prefix}.`,prefix);
		  
		  //const eventURL = `https://cloud.mongodb.com/api/atlas/v1.0/${objectID}/events?minDate=${day.minDate}&maxDate=${day.maxDate}`; 
		  return Promise.all([getURLS(prefix),getControlObj(prefix)])
		  .then( response => {
			let urls = response[0];
			let dataPromises = [getControlObj(prefix),getAllMeta(urls[0])];
			let day = new Date();  // If everything else fails start from today
			if (typeof(response[1]) === "object") {
				//console.log("Object....");
				cntrlFile = response[1];
				day = new Date(cntrlFile.lastRun);
				day = new Date(day.getTime() + oneDay);  //start of next day to process
				if (day.getTime()+oneDay > new Date()) {   // End of next day is in the future
					dataDogEvent("Atlas Event Archiver finished ",`Nothing to do`,prefix);
					console.log('Up to date');
					return(true);
				}
			}
			else
				dataPromises.push(getEarliestDate(urls));
			//console.log(`Date=${day}, URLS=${urls}`);
			return Promise.all(dataPromises)
			.then(response => {
				if (response.length == 3) {
					day=response[2];
					//console.log(`Got Dates ${day}`);
				}
					
				daysProcessed = [];
				dt = day;
			    //for (dt=day;dt < timer;dt = (new Date(dt.getTime()+oneDay))) {
				for(i=0;i<maxDaysToProcess;i++) {
			    	//console.log(`Loop ${dt} to ${timer} (${new Date(dt.getTime()+oneDay)})`);
			    	daysProcessed.push(getOneDay(urls,new Date(dt)));
			    	dt = new Date(dt.getTime()+oneDay);
			    	if (dt > new Date()) break;
			    }
			    return Promise.all(daysProcessed)
			    .then(response => {
			    	let itemCount = response.length;
			    	console.log(`Processed ${itemCount}.`);
			    	//console.log(JSON.stringify(response[0]));
			    	if (cntrlFile != null) {
			    		cntrlFile.lastRun = response[response.length-1].Day;
			    	}
			    	else 
			    		cntrlFile = { lastRun: response[response.length-1].Day, Objects: []};
			    	//console.log(JSON.stringify(cntrlFile));
			    	// Need to create a new array as Objects.push() gives a Type Error
			    	let newobj = [];
			    	let startPos = 0;
			    	let endPos = cntrlFile.Objects.length;
			    	let newCount = response.length;
			    	if (endPos+newCount > maxControlObjects) 
			    		startPos = endPos+newCount - maxControlObjects - 1;
	
			    	for (i=startPos;i< endPos;i++)
			    		newobj.push(cntrlFile.Objects[i]);
			    	for (i=0;i< response.length;i++) {
			    		newobj.push(response[i]);
			    	}
			    	cntrlFile.Objects = newobj;
			    	//console.log(`Now ${newobj.length} Objects`);
			    	//console.log(EJSON.stringify(cntrlFile));
			    	let controlData = [];
			    	controlData.push(cntrlFile);
			        return writeS3Object(`Control-${prefix}`,controlData)
			        .then(response => {
			        	let duration = new Date();
			             duration = (duration - timer)/1000;
			             console.log(`Processed ${itemCount} days for ${prefix} through ${cntrlFile.lastRun} in ${duration}s`);
			             dataDogEvent("Atlas Event Archiver finished ",`Processed ${itemCount} days for ${prefix} through ${cntrlFile.lastRun} in ${duration}s`,prefix);
			            return(response);	
			        });
			    })
			})
			.catch(err => {
				    //console.log("doit");
					if (err instanceof StitchError) {
						let obj = EJSON.stringify(err.message);
						let isMine = (err.message instanceof ArchiveException);
						console.log(`${obj}, ${isMine}, ${err.name}, ${err.statck}`);
					}
		    		//dataDogEvent("Atlas Event Archiver failed ",`Processing ${prefix}. Status: ${err}`,prefix,"error");
		    		return Promise.reject(`doit: Error is:\n-->${err}`);
		    });
		  })
		  .catch(err => {
				if (err instanceof StitchError) {
					let obj = EJSON.stringify(err.message);
					console.log(`${obj}, ${isMine}, ${err.name}, ${err.statck}`);
				}
	    		dataDogEvent("Atlas Event Archiver failed ",`Processing ${prefix}. Status: ${err}`,prefix,"error");
	    		console.error(`archiveEvents: Error is:\n-->${err}`);
	    		return false
		
		  });
	  }
	  catch(error) {
			if (error instanceof StitchError) 
				console.log(EJSON.stringify(error));
			else
				console.log(`Outer Catch: ${error}`);
			return(false); 
	  }
}

exports = function(prefix_in){
	try {
	    username = context.values.get(`${prefix}-pub`);
	    apiKey = context.values.get(`${prefix}-sec`);
		let mco = context.values.get('maxControlObjects');
		if (typeof(mco) == 'number') maxControlObjects = mco;
		let mdp = context.values.get('maxDaysToProcess');
		if (typeof(mdp) == 'number') maxDaysToProcess = mdp;
		let s3b = context.values.get('s3Bucket');
		if (typeof(s3b) == 'string') s3Bucket = s3b;
		let s3r = context.values.get('s3Region');
		if (typeof(s3r) == 'string') s3Region = s3r;
		console.log(`Settings: MCO=${maxControlObjects}, MDP=${maxDaysToProcess}, S3B=${s3Bucket}, S3R=${s3Region}, APK=${username}.`);
		let result = doit(prefix_in);
		return (result);
	}
	catch(error) {
		if (error instanceof StitchError) 
		console.log(EJSON.stringify(error));
		return(false);
	}
};
