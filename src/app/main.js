var moment = require('moment');
var pg = require('pg');
var QueryStream = require('pg-query-stream');
var transform = require('stream-transform');
var url = require('url');
var http = require('http');
var elasticsearch = require('elasticsearch');

module.exports = {
  process: function(shop, dbconfig, elastic_url, elastic_index, cb) {
    var configuration = {
      user: dbconfig.user,
      password: dbconfig.password,
      host: dbconfig.host,
      database: dbconfig.name
    }
    var now = moment().format('YYYYMMDDHHmmSS');
    var index = elastic_index + '_' + now;
    var pool = new pg.Pool(configuration);

    pool.on('error', function (err) {
      console.error(err)
    });

    var elasticClient = new elasticsearch.Client({
      host: elastic_url
    });

    //pipe 1,000,000 rows to stdout without blowing up your memory usage
    pool.connect(function(err, client, done) {
      if (err) {
        done();
        return cb(err);
      }
      var query = new QueryStream('SELECT proid, proname, prodescription FROM product');
      var stream = client.query(query);
      // release the client when the stream is finished
      stream.on('end', function() {
        done();
        pool.end();
      });
      var c = 0;
      var bulk = { body: [] };
      var transformer = transform(function (record, cb) {
        c++;
        bulk.body.push({index: {_index: index, _type: 'product', _id: record.proid }});
        bulk.body.push({name: record.proname, description: record.prodescription});

        // elasticClient.indices.analyze({
        //   index: 'nl-nl',
        //   field: 'name',
        //   text: record.prodescription
        // }, function (err, result) {
        //   result.tokens.map(function(item) {
        //     console.log(item.token);
        //   })
        // });

        if (c%1000===0) {
          elasticClient.bulk(bulk);
          bulk.body = [];
        }
        return cb();
      }, {parallel: 1});
      transformer.on('finish',function(){
        if (c%1000!==0) {
          elasticClient.bulk(bulk);
        }
        elasticClient.indices.updateAliases({
          body: {
            actions: [
              { remove: { index: elastic_index+'_*', alias: elastic_index } },
              { add:    { index: index, alias: elastic_index } }
            ]
          }
        }).then(function (response) {
          console.log(response);
          done();
          return cb();
        });
      });

      stream.pipe(transformer);
    });
  }
}