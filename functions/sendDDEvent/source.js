exports = function(title,message,tag){
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