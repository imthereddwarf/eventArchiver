var prefix = "PSPeter";
var Users = [];
var Groups = [];
var Teams = [];
var Origin = new Date();

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

exports = function(arg,userID,groupID,teamID){
  
  return getAllMeta(arg)
  .then(response => {
    if (response)
      console.log(Users[userID].Name+" "+Groups[groupID].Name+" "+Teams[teamID].Name+" "+Origin.toString());
    return(response);
  });

};