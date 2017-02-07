'use strict';
/**
 * Connection handler for Postgres database
 */

var pg = require('pg');

var pool;

module.exports = {
 /**
  * Initializes a database connection pool
  * @param config The configuration
  */
  init: function (cfg) {
    var configuration = {
      user: cfg.dbUser,
      password: cfg.dbPassword,
      host: cfg.dbHost,
      database: cfg.dbDatabase
    }

    pool = new pg.Pool(configuration);

    pool.on('error', function (err) {
      console.error(err)
    });

    return true;
  },
 /**
  * Returns a connection from the database connection pool
  * @param callback The callback
  */
  execute: function (callback) {
    pool.connect(function (err, client, done) {
      if (err) {
        return callback(err);
      }
      client.query('START TRANSACTION', function (err) {
        if (err) {
          return callback(err);
        }
        callback(null, client, done);
      });
    });
  },
 /**
  * Commits a connection from the database connection pool
  * @param client The connection
  * @param done The function that is obtained when opening the connection
  */
  commit: function (client, done) {
    client.query('COMMIT', done);
  },
 /**
  * Rolls the connections transaction back
  * @param client The connection
  * @param done The function that is obtained when opening the connection
  */
  rollback: function (client, done) {
    client.query('ROLLBACK', function (err) {
      done(err);
    });
  }
};
