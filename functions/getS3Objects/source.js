getLastDate = function(s3Service,objName) {
    return s3Service.GetObject({
         'Bucket': "log-uploader",
         'Key': `${objName}`
    })
    .then(getObjectOutput => {
      const objectDate = getObjectOutput.Body.text();
      var cntrl = EJSON.parse(objectDate);
       return(cntrl);
    })
    .catch(console.error);
    
};



exports = function(prefix){

  const s3Service = context.services.get('S3Bucket').s3('us-east-2');
  return  s3Service.ListObjectsV2({
    'Bucket': "log-uploader",
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