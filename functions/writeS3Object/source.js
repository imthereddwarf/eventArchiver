exports = function(key,value){

	
  const s3Service = context.services.get('S3Bucket').s3('us-east-2');
  var payload = "";
  for (i=0;i<value.length;i++)
    payload += EJSON.stringify(value[i]);
  const s2 = utils.crypto.hash('md5',payload).toBase64();
  //console.log(`MD5 is ${s2}`);
  return s3Service.PutObject({
    'Bucket': "log-uploader",
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