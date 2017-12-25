var cassandra = require('./node_modules/cassandra-driver');
var client = new cassandra.Client({ contactPoints: ['172.31.14.84'], keyspace: 'events' });
var QUEUE_URL = 'https://sqs.us-east-2.amazonaws.com/331983685977/packaged-events';
var AWS = require('aws-sdk');
var sqs = new AWS.SQS({region : 'us-east-2'});

module.exports.handler = function(eventRecord) {
  var query = 'SELECT * FROM Events.event WHERE videoid= ? AND viewinstanceid = ? allow filtering;';
  var queryParams = [eventRecord.videoId, eventRecord.viewInstanceId];
  // console.log('PARAMS:', queryParams);

  client.execute(query, queryParams, { prepare: true })
    .then(data => {
      let cleanedRows = Array.from(data.rows);

      let events = cleanedRows.map(element => {
        let singleEvent = {
          "videoId": JSON.parse(element.videoid),
          "viewInstanceId": JSON.parse(element.viewinstanceid),
          "event_action": element.event_action,
          "event_timestamp": element.event_timestamp
        };

        return singleEvent;
      });

      // package rows array into an object to send out
      let packagedEvents = {"Events": events}
      // drop events out to the queue
      let sqsParams = {
        MessageBody: JSON.stringify(events),
        QueueUrl: QUEUE_URL
      };

      sqs.sendMessage(sqsParams, function(err, data) {
        if (err) { console.log('SQS Posting Error: ', err); }
        else { console.log(data); }
      })
    })
    .catch(function(err) { console.log('Cassandra database error:\n', err)});
};