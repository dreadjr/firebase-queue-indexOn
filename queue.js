#!/usr/bin/env node

console.log('NODE', process.version);

var Firebase = require('firebase');
Firebase.enableLogging(true);

var winston = require('winston');
winston.level = 'debug';

var Queue = require('firebase-queue');
var _ = require('lodash');
var Promise = require('bluebird');

require('dotenv').load();

console.log('URL', process.env.FIREBASE_URL);
console.log('SECRET', process.env.FIREBASE_SECRET || 'n/a');

var fb = new Firebase(process.env.FIREBASE_URL);
var fbAuth = !!process.env.FIREBASE_SECRET ? Promise.promisify(fb.authWithCustomToken, { context: fb }) : noop;

var specs = {
  "default": {
    task_1: {
      "start_state": null,
      "in_progress_state": "task_1_in_progress",
      "finished_state": 'task_1_finished',
      "error_state": "error",
      "timeout": 30000,
      "retries": 3
    },
    task_2: {
      "start_state": 'task_1_finished',
      "in_progress_state": "task_2_in_progress",
      "finished_state": "task_2_finished",
      "error_state": "error",
      "timeout": 30000,
      "retries": 3
    },
    task_3: {
      "start_state": 'task_2_finished',
      "in_progress_state": "task_3_in_progress",
      "finished_state": null,
      "error_state": "error",
      "timeout": 30000,
      "retries": 3
    }
  }
};

console.log('SPECS', specs);

// start the queues
var qRef = fb.child('queues');
run(qRef);

// add a job every X
setInterval(function() {
//setTimeout(function() {
  addJob();
}, 10000);



function run(baseRef) {
  fbAuth(process.env.FIREBASE_SECRET)
    .then(addSpecs(baseRef))
    .then(function () {

      var spec_1_options = {
        'specId': 'task_1',
        'numWorkers': 1,
        'sanitize': true
      };

      q(baseRef.child('default'), spec_1_options);

      var spec_2_options = {
        'specId': 'task_2',
        'numWorkers': 1,
        'sanitize': true
      };

      q(baseRef.child('default'), spec_2_options);

      var spec_3_options = {
        'specId': 'task_3',
        'numWorkers': 1,
        'sanitize': true
      };

      q(baseRef.child('default'), spec_3_options);
    });
}

function q(ref, options) {
  console.log('creating queue', ref.toString(), options);

  var queue = new Queue(ref, options, function (data, progress, resolve, reject) {
    // Read and process task data
    console.log(options.specId, 'proccesing job', data);

    // Do some work
    progress(50);

    //throw new Error('error processing');

    // Finish the task asynchronously
    setTimeout(function () {
      resolve(data);
    }, 500);
  });

  return queue;
}

function addSpecs(baseRef) {
  var promises = [];

  _.each(specs, function (value, queue) {
    _.each(value, function (spec, key) {
      var tRef = baseRef.child(queue).child('tasks');
      var sRef = baseRef.child(queue).child('specs');
      var ref = sRef.child(key);
      //sRef.child(key).set(spec);

      promises.push(set(ref, spec));
    });
  });

  return Promise.all(promises);
}

function addJob() {
  var ref = qRef.child('default').child('tasks').push();
  console.log('adding job', ref.toString());

  ref.set({'foo': 'bar'}, function(err) {
    if (err) {
      console.log('error adding job', err);
    }
  });
}

function set(ref, val) {
  return new Promise(function(resolve, reject) {
    ref.set(val, function(err) {
      return err ? reject(err) : resolve(ref);
    });
  });
}

function noop() {
  return Promise.resolve();
}
