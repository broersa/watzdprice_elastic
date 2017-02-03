'use strict';

var moment = require('moment');

module.exports = {
  getProduct: function (client, productid, callback) {
    client.query('select prokey, proid, proname, proshop, probrand, proean, procategory, procreated, prolastupdate, proprice, prodescription, prourl, proimage from product where proid=$1 for update', [productid], function (err, result) {
      if (err) {
        return callback(err);
      }
      if (result.rowCount !== 1) {
        return callback(null, null);
      }
      callback(null, {
        prokey: parseInt(result.rows[0].prokey),
        proid: result.rows[0].proid,
        proname: result.rows[0].proname,
        proshop: result.rows[0].proshop,
        probrand: result.rows[0].probrand,
        proean: result.rows[0].proean,
        procategory: result.rows[0].procategory,
        procreated: moment(result.rows[0].procreated),
        prolastupdate: moment(result.rows[0].prolastupdate),
        proprice: parseFloat(result.rows[0].proprice),
        prodescription: result.rows[0].prodescription,
        prourl: result.rows[0].prourl,
        proimage: result.rows[0].proimage
      });
    });
  },
  addProduct: function (client, product, callback) {
    client.query('insert into product (proid, proname, proshop, probrand, proean, procategory, procreated, prolastupdate, proprice, prodescription, prourl, proimage) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) RETURNING prokey', [
      product.proid,
      product.proname,
      product.proshop,
      product.probrand,
      product.proean,
      product.procategory,
      product.procreated,
      product.prolastupdate,
      product.proprice,
      product.prodescription,
      product.prourl,
      product.proimage], function (err, result) {
        if (err) {
          return callback(err);
        }
        if (result.rowCount !== 1) {
          return callback(null, null);
        }
        callback(null, result.rows[0].prokey);
      });
  },
  updateProduct: function (client, product, callback) {
    client.query('update product set probrand=$1, proean=$2, procategory=$3, prolastupdate=$4, proprice=$5, prodescription=$6, proimage=$7 WHERE proid=$8', [
      product.probrand,
      product.proean,
      product.procategory,
      product.prolastupdate,
      product.proprice,
      product.prodescription,
      product.proimage,
      product.proid], function (err) {
      if (err) {
        return callback(err);
      }
      callback(null);
    });
  },
  addHistory: function (client, history, callback) {
    client.query('insert into history (hisproduct, hisupdate, hisprice) VALUES ($1, $2, $3) RETURNING hiskey', [
      history.hisproduct,
      history.hisupdate,
      history.hisprice], function (err, result) {
        if (err) {
          return callback(err);
        }
        if (result.rowCount !== 1) {
          return callback(null, null);
        }
        callback(null, result.rows[0].hiskey);
      });
  },
  addShopLoadStats: function (client, shopLoadStats, callback) {
    client.query('insert into shoploadstats (slsshop, slsstart, slsend, slsadded, slsupdated) VALUES ($1, $2, $3, $4, $5) RETURNING slskey', [
      shopLoadStats.shop,
      shopLoadStats.start,
      shopLoadStats.end,
      shopLoadStats.added,
      shopLoadStats.updated], function (err, result) {
        if (err) {
          return callback(err);
        }
        if (result.rowCount !== 1) {
          return callback(null, null);
        }
        callback(null, result.rows[0].slskey);
      });
  }
}
