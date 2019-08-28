exports = function(arg){
  const mongodb = context.services.get("eventArchiver");
  const itemsCollection = mongodb.db("coinbase").collection("testapp");
  return itemsCollection.findOne({})
  .then(result => {
    console.log(EJSON.stringify(result));
    return(result);
  } );
};