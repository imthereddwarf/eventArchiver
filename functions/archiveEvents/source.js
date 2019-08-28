var prefix;
var day;
var Users = [];
var Groups = [];
var Teams = [];
var Origin = new Date();
const oneDay = 86400000;   // 24 * 60 * 60 * 1000 One day in miliseconds 
var maxControlObjects = 30; //Maximum number of checksums to preserve
var maxDaysToProcess = 10; 
var s3Bucket = "log-uploader";
var s3Region = "us-east-2";


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
  var earliest = new Date();
  var thisURL = `${item}/events?eventType=GROUP_CREATED`;
  //console.log(thisURL);
  return context.functions.execute('cloudAuthenticatedRequest', thisURL, prefix)
    .then(response => {
      for(var i=0;i<response.results.length;i++){
        //console.log(`Found ${i} ${response.results[i].created} earliest ${earliest}`);
        if (new Date(response.results[i].created) < earliest) earliest = new Date(response.results[i].created);
      }
      //console.log(earliest.toString()+thisURL);
      return(earliest);
    })
    .catch(err => console.error(`Failed to get first event from ${thisURL}: ${err}`));
}

async function getEarliestDate(urls) {
    const getPromises = urls.map(url => getFirstDate(url));
    return Promise.all(getPromises)
    .then(response => {
    	let earliest = response[0];
    	for (i=1;i<response.length;i++) {
    		if (earliest > response[i]) earliest = response[i];
    	}
    	console.log(`Found earliest ${earliest}.`);
    	return(earliest);
    })
    .catch(err => console.error(`Error getting earliest date: ${err}`));
}

async function getUsers(item) {
  let users = [];
  let thisURL = `${item}/users`;
  //console.log(thisURL);
  return context.functions.execute('cloudAuthenticatedRequest', thisURL, prefix)
    .then(response => {
      for(var i=0;i<response.results.length;i++){
        users[`${response.results[i].id}`] = {Name: `${response.results[i].firstName} ${response.results[i].lastName}`, Email: `${response.results[i].emailAddress}`};
      }
      //console.log(Object.keys(users));
      return(users);
    })
    .catch(err => console.error(`Failed to get Users from org: ${err}`));
}

async function getGroups(item) {
  let groups = [];
  let thisURL = `${item}/groups`;
  //console.log(thisURL);
  return context.functions.execute('cloudAuthenticatedRequest', thisURL, prefix)
    .then(response => {
      for(var i=0;i<response.results.length;i++){
        groups[`${response.results[i].id}`] = {Name: response.results[i].name, clusterCount: response.results[i].clusterCount, created:response.results[i].created};
      }
      return(groups);
    })
    .catch(err => console.error(`Failed to get groups from org: ${err}`));
}

async function getTeams(item) {
  let teams = [];
  let thisURL = `${item}/teams`;
  //console.log(thisURL);
  return context.functions.execute('cloudAuthenticatedRequest', thisURL, prefix)
    .then(response => {
      for(var i=0;i<response.results.length;i++){
        teams[`${response.results[i].id}`] = {Name: response.results[i].name};
      }
      return(teams);
    })
    .catch(err => console.error(`Failed to get team from org: ${err}`));
}

async function getAllMeta(org){
  
  var users = getUsers(org);
  var groups = getGroups(org);
  var teams = getTeams(org);
  var startDate = getFirstDate(org);
  return Promise.all([users,groups,teams,startDate])
  .then(response => {
    Users = response[0];
    Groups = response[1];
    Teams = response[2];
    Origin = response[3];
    //console.log(response[3].toString());
    return(true);
  })
  .catch(err => {
	  dataDogEvent("Atlas Event Archiver failed ",`Processing ${prefix} for ${queryDate.minDate} Status: ${err}`,prefix);
	  throw new FatalError(`Fatal error fetching metadata: ${err}`);
  });
  //.catch(error => {return false});
}

dataDogEvent = function(title,message,tag){
  let url = "https://api.datadoghq.com/api/v1/events?api_key="+context.values.get("DataDogAPIKey");
  let method = "POST";
  let postData = `{ "title": "${title}","text":"${message}","priority": "normal", "tags": ["Atlas:${tag}"],"alert_type": "info"}`;
  //console.log(postData);
  
  return context.http.post({
    url: url,
    body: postData,
    headers: { "Content-Type": [ "application/json" ] }
  })
  .then(response => {
    const json_body = EJSON.parse(response.body.text());
    return json_body;
  });
};

getMyOrg = function() {
  
  const url = `https://cloud.mongodb.com/api/atlas/v1.0/orgs`; 
  return context.functions.execute('cloudAuthenticatedRequest', url, prefix)
    .then(response => {
      if (response.totalCount == 1) 
        return response.results[0].links[0].href.toString();
    })
    .catch(err => console.error(`Failed to get Root Org: ${err}`));
};

getMyProjects = function() {
  content = [];
  const url = `https://cloud.mongodb.com/api/atlas/v1.0/groups`; 
  return context.functions.execute('cloudAuthenticatedRequest', url, prefix)
    .then(response => {
      for(var i=0;i<response.results.length;i++){
        content.push(response.results[i].links[0].href.toString());
      }
      return(content);
    })
    .catch(err => console.error(`Failed to get my projects: ${err}`));
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
   .catch(err => console.error(`Failed to get my endpoints: ${err}`));
};

async function getEvents(item,dateRange) {
  let content = [];
  let thisURL = `${item}/events?minDate=${dateRange.minDate}&maxDate=${dateRange.maxDate}`;
  //console.log(thisURL);
  return context.functions.execute('cloudAuthenticatedRequest', thisURL, prefix)
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
    .catch(err => console.error(`Failed to get events from $(item}: ${err}`));
};

getLastDate = function(s3Service,objName) {
    return s3Service.GetObject({
         'Bucket': s3Bucket,
         'Key': `${objName}`
    })
    .then(getObjectOutput => {
      const objectDate = getObjectOutput.Body.text();
      var cntrl = EJSON.parse(objectDate);
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
	      return `${ListObjectsV2Output.KeyCount} matching control files.`;
	    else {
	      const statusObj = getLastDate(s3Service,ListObjectsV2Output.Contents[0].Key);
	      console.log(`Got ${ListObjectsV2Output.Contents[0].Key} Last Update ${statusObj.lastRun}.`);
	      return statusObj;
	    }
	  })
	  .catch(console.error);  
	};
	
getOneDay = function(urls,day){
    
	let queryDate = setQueryDate(day);
	let timer = new Date();
    console.log(`Atlas Event Archiver starying to process ${prefix} for ${queryDate.minDate}`);
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
        return context.functions.execute('writeS3Object',objectName,events)
        .then(response => {
              let duration = new Date();
              response.Day = queryDate.minDate;
              //console.log(EJSON.stringify(response));
              duration = (duration - timer)/1000;
              //console.log(`Atlas Event Archiver finished: Processing ${prefix} for ${queryDate.minDate} in ${duration}s`);
             dataDogEvent("Atlas Event Archiver finished ",`Processing ${prefix} for ${queryDate.minDate} in ${duration}s`,prefix);
            return(response);
        })
        .catch(err => dataDogEvent("Atlas Event Archiver failed ",`Processing ${prefix} for ${queryDate.minDate} Status: ${err}`,prefix));
    });
}
	
async function doit(prefix_in){
	
	
	  prefix = prefix_in;
	  var timer = new Date();
	  var cntrlFile = null;
	  dataDogEvent("Atlas Event Archiver starting",`Processing ${prefix}.`,prefix);
	  
	  //const eventURL = `https://cloud.mongodb.com/api/atlas/v1.0/${objectID}/events?minDate=${day.minDate}&maxDate=${day.maxDate}`; 
	  return getURLS(prefix)
	  .then( urls => {
	    //console.log(urls);
	    var day = new Date();  // If everything else fails start from today
		return Promise.all([getControlObj(prefix),getEarliestDate(urls),getAllMeta(urls[0])])
		.then(response => {
			//console.log(response[0]+" | "+response[1])
			if (typeof(response[0]) === "object") {
				//console.log(`Control Date: ${EJSON.stringify(response[0])}`);
				cntrlFile = response[0];
				//console.log(`Control File: ${EJSON.stringify(cntrlFile)}`);
				day = new Date(cntrlFile.lastRun);
				//console.log(typeof(day));
				day = new Date(day.getTime() + oneDay);
				//console.log(typeof(day)+" "+day);
			}
			else {
				//console.log(`Event Date: ${response[1]}`);
				day = response[1];
			}
			console.log(`Earliest Date ${day}`);
			if (day.getTime()+oneDay > new Date()) {   // Up to date
				dataDogEvent("Atlas Event Archiver finished ",`Nothing to do`,prefix);
			}
			//setQueryDate(day);
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
		        return context.functions.execute('writeS3Object',`Control-${prefix}`,controlData)
		        .then(response => {
		        	let duration = new Date();
		             duration = (duration - timer)/1000;
		             console.log(`Processed ${itemCount} days for ${prefix} through ${cntrlFile.lastRun} in ${duration}s`);
		             dataDogEvent("Atlas Event Archiver finished ",`Processed ${itemCount} days for ${prefix} through ${cntrlFile.lastRun} in ${duration}s`,prefix);
		            return(response);	
		        });
		    })
		})
	  });
}

exports = function(prefix_in){
	let mco = context.values.get('maxControlObjects');
	if (typeof(mco) == 'number') maxControlObjects = mco;
	console.log(typeof(mco));
	let mdp = context.values.get('maxDaysToProcess');
	if (typeof(mdp) == 'number') maxDaysToProcess = mdp;
	let s3b = context.values.get('s3Bucket');
	if (typeof(s3b) == 'string') s3Bucket = s3b;
	console.log(typeof(s3b));
	let s3r = context.values.get('s3Region');
	if (typeof(s3r) == 'string') s3Region = s3r;
	console.log(`Settings: MCO=${maxControlObjects}, MDP=${maxDaysToProcess}, S3B=${s3Bucket}, S3R=${s3Region}.`);
	let result = doit(prefix_in);
	return (result);
};
