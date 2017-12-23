var cassandra = require('./node_modules/cassandra-driver');
var client = new cassandra.Client({ contactPoints: ['172.31.14.84'], keyspace: 'events' });


module.exports.handler = function(eventRecord) {
  var query = 'SELECT * FROM Events.event WHERE videoId= (?) AND viewInstanceId = (?);';
  var params = [eventRecord.videoId, eventRecord.viewInstanceId];

  client.execute(query, params, { prepare: true })
    .then(function(data) {console.log('READ RECORDS: ', data.rows)})
    .then(function(rows) {
      console.log('SEND RECORDS TO QUEUE');
      // package rows array into an object to send out
      let events = {"Events": rows}
      // drop events out to the queue
      console.log(events);
    })
    .catch(function(err) { console.log('Database not connected:\n', err)});
};

// TEST READ handler function
// module.exports.handler = function(eventRecord) {
//   var params = [eventRecord.viewInstanceId];
//   var query = 'SELECT * FROM Events.event WHERE viewInstanceId = ' + params;

//   client.execute(query)
//   // client.execute(query, params, { prepare: true })
//     .then(function(data) {console.log('READ DATA: ', data)})
//     .catch(function(err) { console.log('Database not connected:\n', err)});

// };