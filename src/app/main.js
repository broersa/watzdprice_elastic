var moment = require('moment');
var pg = require('pg');
var QueryStream = require('pg-query-stream');
var transform = require('stream-transform');
var elasticsearch = require('elasticsearch');
var MyError = require('../MyError.js');

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
      console.error(JSON.stringify(err));
    });

    var elasticClient = new elasticsearch.Client({
      host: elastic_url
    });

    //pipe 1,000,000 rows to stdout without blowing up your memory usage
    pool.connect(function(err, client, done) {
      if (err) {
        done();
        return cb(new MyError('ERROR', 'process', 'Error', {shop: shop, dbconfig_name: dbconfig.name, elastic_url: elastic_url, elastic_index: elastic_index}, err));
      }
      var query = new QueryStream('SELECT proid, proname, prodescription, proimage, proprice FROM product');
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
        bulk.body.push({name: record.proname, description: record.prodescription, image: record.proimage, price: record.proprice });

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
          elasticClient.bulk(bulk, function (err, resp) {
            if (err) {
              return cb(new MyError('ERROR', 'process', 'Error', {shop: shop, dbconfig_name: dbconfig.name, elastic_url: elastic_url, elastic_index: elastic_index}, err));
            }
            bulk.body = [];
            return cb();
          });
        } else {
          return cb();
        }
      }, {parallel: 1});
      transformer.on('error',function(err){
          done();
          return cb(new MyError('ERROR', 'process', 'Error', {shop: shop, dbconfig_name: dbconfig.name, elastic_url: elastic_url, elastic_index: elastic_index}, err));
      });
      transformer.on('finish',function(){
        if (c%1000!==0) {
          elasticClient.bulk(bulk, function (err, resp) {
            if (err) {
              done();
              return cb(new MyError('ERROR', 'process', 'Error', {shop: shop, dbconfig_name: dbconfig.name, elastic_url: elastic_url, elastic_index: elastic_index}, err));
            }
            done();
            elasticClient.indices.updateAliases({
              body: {
                actions: [
                  { remove: { index: elastic_index+'_*', alias: elastic_index } },
                  { add:    { index: index, alias: elastic_index } }
                ]
              }
            }, function (err, resp) {
              if (err) {
                return cb(new MyError('ERROR', 'process', 'Error', {shop: shop, dbconfig_name: dbconfig.name, elastic_url: elastic_url, elastic_index: elastic_index}, err));
              }
              elasticClient.cat.indices({format: 'json', index: elastic_index+'_*'}, function (err, resp) {
                if (err) {
                  return cb(new MyError('ERROR', 'process', 'Error', {shop: shop, dbconfig_name: dbconfig.name, elastic_url: elastic_url, elastic_index: elastic_index}, err));
                }
                var indices = [];
                for (var i = 0; i < resp.length; i++ ) {
                  if (resp[i].index !== index) {
                    indices.push(resp[i].index);
                  }
                }
                if (indices.length>0) {
                  elasticClient.indices.delete({index: indices}, function (err, resp) {
                    if (err) {
                      return cb(new MyError('ERROR', 'process', 'Error', {shop: shop, dbconfig_name: dbconfig.name, elastic_url: elastic_url, elastic_index: elastic_index}, err));
                    }
                    return cb();
                  });
                } else {
                  return cb();
                }
              });
            });
          });
        }
      });

      stream.pipe(transformer);
    });
  }
}
