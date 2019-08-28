var prefix;
var events = [];
var day;
var queryDate = {"minDate": 0,"maxDate": 0};
var Users = [];
var Groups = [];
var Teams = [];
var Origin = new Date();

setQueryDate = function(day) {
  var minDate = day.getTime() - (day.getTime() % 86400000);  // Make it midnight UTC
  var maxDate = minDate + 86400000 - 1;  // Range is one day
  queryDate.minDate = new Date(minDate).toISOString();
  queryDate.maxDate = new Date(maxDate).toISOString();
  return;
};

async function getFirstDate(item) {
  var earliest = new Date();
  var thisURL = `${item}/events?eventType=GROUP_CREATED`;
  console.log(thisURL);
  return context.functions.execute('cloudAuthenticatedRequest', thisURL, prefix)
    .then(response => {
      for(var i=0;i<response.results.length;i++){
        console.log(`Found ${i} ${response.results[i].created} earliest ${earliest}`);
        if (new Date(response.results[i].created) < earliest) earliest = new Date(response.results[i].created);
      }
      console.log(earliest.toString());
      return(earliest);
    })
    .catch(err => console.error(`Failed to get Root Org: ${err}`));
}

async function getEarliestDate(org,projects) {
	let orgd = await getFirstDate(org);
	let projd = await getFirstDate(proj[0]);
	if (orgd < projd) return(orgd);
	else return(projd);
}

async function getUsers(item) {
  var users = [];
  var thisURL = `${item}/users`;
  console.log(thisURL);
  return context.functions.execute('cloudAuthenticatedRequest', thisURL, prefix)
    .then(response => {
      for(var i=0;i<response.results.length;i++){
        users[`${response.results[i].id}`] = {Name: `${response.results[i].firstName} ${response.results[i].lastName}`, Email: `${response.results[i].emailAddress}`};
      }
      console.log(Object.keys(users));
      return(users);
    })
    .catch(err => console.error(`Failed to get Root Org: ${err}`));
}

async function getGroups(item) {
  var groups = [];
  var thisURL = `${item}/groups`;
  console.log(thisURL);
  return context.functions.execute('cloudAuthenticatedRequest', thisURL, prefix)
    .then(response => {
      for(var i=0;i<response.results.length;i++){
        groups[`${response.results[i].id}`] = {Name: response.results[i].name, clusterCount: response.results[i].clusterCount, created:response.results[i].created};
      }
      return(groups);
    })
    .catch(err => console.error(`Failed to get Root Org: ${err}`));
}

async function getTeams(item) {
  var teams = [];
  var thisURL = `${item}/teams`;
  console.log(thisURL);
  return context.functions.execute('cloudAuthenticatedRequest', thisURL, prefix)
    .then(response => {
      for(var i=0;i<response.results.length;i++){
        teams[`${response.results[i].id}`] = {Name: response.results[i].name};
      }
      return(teams);
    })
    .catch(err => console.error(`Failed to get Root Org: ${err}`));
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
    console.log(response[3].toString());
    return(true);
  });
  //.catch(error => {return false});
}

dataDogEvent = function(title,message,tag){
  var url = "https://api.datadoghq.com/api/v1/events?api_key="+context.values.get("DataDogAPIKey");
  var method = "POST";
  var postData = `{ "title": "${title}","text":"${message}","priority": "normal", "tags": ["Atlas:${tag}"],"alert_type": "info"}`;
  console.log(postData);
  
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
    .catch(err => console.error(`Failed to get Root Org: ${err}`));
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
  });
};

async function getEvents(item) {
  content = [];
  var thisURL = `${item}/events?minDate=${queryDate.minDate}&maxDate=${queryDate.maxDate}`;
  console.log(thisURL);
  return context.functions.execute('cloudAuthenticatedRequest', thisURL, prefix)
    .then(response => {
      console.log(`Got ${response.results.length}, totoal count ${response.totalCount}`)
      for(var i=0;i<response.results.length;i++){
        content.push(response.results[i]);
      }
      console.log(`returning ${content.length}`)
      return(content);
    })
    .catch(err => console.error(`Failed to get Root Org: ${err}`));
};


exports = function(prefix_in,day_in){

  prefix = prefix_in;
  day = day_in;
  setQueryDate(day);
  var timer = new Date();
  dataDogEvent("Atlas Event Archiver starting",`Processing ${prefix} for ${queryDate.minDate} to ${queryDate.maxDate}`,prefix);
  //const eventURL = `https://cloud.mongodb.com/api/atlas/v1.0/${objectID}/events?minDate=${day.minDate}&maxDate=${day.maxDate}`; 
  return getURLS(prefix)
  .then( urls => {
    console.log(urls);
    events.length = 0;
    const getPromises = urls.map(url => getEvents(url));
    Promise.all(getPromises)
    .then(response => {
        events = response[0];
        var objectName = queryDate.minDate.substring(0,4)+"/"+queryDate.minDate.substring(5,7)+"/"+queryDate.minDate.substring(8,10)+"-"+prefix;
        console.log(objectName);
        return context.functions.execute('writeS3Object',objectName,events)
        .then(response => {
              var duration = new Date();
              duration = (duration - timer)/1000;
             dataDogEvent("Atlas Event Archiver finished ",`Processing ${prefix} for ${queryDate.minDate} in ${duration}s`,prefix);
            return(response);
        })
        .catch(err => dataDogEvent("Atlas Event Archiver failed ",`Processing ${prefix} for ${queryDate.minDate} Status: ${err}`,prefix));
    });
    return(events);
  });
};