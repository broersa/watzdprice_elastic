/* eslint-env node, mocha */
var chai = require('chai');
// var assert = chai.assert;
var expect = chai.expect;
var sinon = require('sinon');
var main = require('../../src/app/main.js');

describe('app/main', function () {
  var sandbox;

  beforeEach(function () {
    // Create a sandbox for the test
    sandbox = sinon.sandbox.create();
  });

  afterEach(function () {
    // Restore all the things made through the sandbox
    sandbox.restore();
  });

  it('should run', function () {
    // sandbox.stub(config, 'get', function (param) {
    //   return '';
    // });
    //main.process();
    expect(1).to.be.equal(1);
  });
});
