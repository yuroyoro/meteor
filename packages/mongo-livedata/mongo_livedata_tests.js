// This is a magic collection that fails its writes on the server when
// the selector (or inserted document) contains fail: true.

// XXX namespacing
Meteor._FailureTestCollection =
  new Meteor.Collection("___meteor_failure_test_collection");

testAsyncMulti("mongo-livedata - database failure reporting", [
  function (test, expect) {
    var ftc = Meteor._FailureTestCollection;

    var exception = function (err) {
      test.instanceOf(err, Error);
    };

    _.each(["insert", "remove", "update"], function (op) {
      if (Meteor.is_server) {
        test.throws(function () {
          ftc[op]({fail: true});
        });

        ftc[op]({fail: true}, expect(exception));
      }

      if (Meteor.is_client) {
        ftc[op]({fail: true}, expect(exception));

        // This would log to console in normal operation.
        Meteor._suppress_log(1);
        ftc[op]({fail: true});
      }
    });
  }
]);

// XXX namespacing
Meteor._LivedataTestCollection =
  new Meteor.Collection("livedata_test_collection");

Tinytest.addAsync("mongo-livedata - basics", function (test, onComplete) {
  var coll = Meteor._LivedataTestCollection;
  var run = test.runId();

  var log = '';
  var obs = coll.find({run: run}, {sort: ["x"]}).observe({
    added: function (doc, before_index) {
      log += 'a(' + doc.x + ',' + before_index + ')';
    },
    changed: function (new_doc, at_index, old_doc) {
      log += 'c(' + new_doc.x + ',' + at_index + ',' + old_doc.x + ')';
    },
    moved: function (doc, old_index, new_index) {
      log += 'm(' + doc.x + ',' + old_index + ',' + new_index + ')';
    },
    removed: function (doc, at_index) {
      log += 'r(' + doc.x + ',' + at_index + ')';
    }
  });

  var captureObserve = function (f) {
    if (Meteor.is_client) {
      f();
    } else {
      var fence = new Meteor._WriteFence;
      Meteor._CurrentWriteFence.withValue(fence, f);
      fence.armAndWait();
    }

    var ret = log;
    log = '';
    return ret;
  };

  var expectObserve = function (expected, f) {
    if (!(expected instanceof Array))
      expected = [expected];

    test.include(expected, captureObserve(f));
  };

  test.equal(coll.find({run: run}).count(), 0);
  test.equal(coll.findOne("abc"), undefined);
  test.equal(coll.findOne({run: run}), undefined);

  expectObserve('a(1,0)', function () {
    var id = coll.insert({run: run, x: 1});
    test.equal(id.length, 36);
    test.equal(coll.find({run: run}).count(), 1);
    test.equal(coll.findOne(id).x, 1);
    test.equal(coll.findOne({run: run}).x, 1);
  });

  expectObserve('a(4,1)', function () {
    var id2 = coll.insert({run: run, x: 4});
    test.equal(coll.find({run: run}).count(), 2);
    test.equal(coll.find({_id: id2}).count(), 1);
    test.equal(coll.findOne(id2).x, 4);
  });

  test.equal(coll.findOne({run: run}, {sort: ["x"], skip: 0}).x, 1);
  test.equal(coll.findOne({run: run}, {sort: ["x"], skip: 1}).x, 4);
  test.equal(coll.findOne({run: run}, {sort: {x: -1}, skip: 0}).x, 4);
  test.equal(coll.findOne({run: run}, {sort: {x: -1}, skip: 1}).x, 1);

  var cur = coll.find({run: run}, {sort: ["x"]});
  var total = 0;
  cur.forEach(function (doc) {
    total *= 10;
    total += doc.x;
  })
  test.equal(total, 14);

  cur.rewind();
  test.equal(cur.map(function (doc) {
    return doc.x * 2;
  }), [2, 8]);

  test.equal(_.pluck(coll.find({run: run}, {sort: {x: -1}}).fetch(), "x"),
             [4, 1]);

  expectObserve('c(3,0,1)c(6,1,4)', function () {
    coll.update({run: run}, {$inc: {x: 2}}, {multi: true});
    test.equal(_.pluck(coll.find({run: run}, {sort: {x: -1}}).fetch(), "x"),
               [6, 3]);
  });

  expectObserve(['c(13,0,3)m(13,0,1)', 'm(6,1,0)c(13,1,3)'], function () {
    coll.update({run: run, x: 3}, {$inc: {x: 10}}, {multi: true});
    test.equal(_.pluck(coll.find({run: run}, {sort: {x: -1}}).fetch(), "x"),
               [13, 6]);
  });

  expectObserve('r(13,1)', function () {
    coll.remove({run: run, x: {$gt: 10}});
    test.equal(coll.find({run: run}).count(), 1);
  });

  expectObserve('r(6,0)', function () {
    coll.remove({run: run});
    test.equal(coll.find({run: run}).count(), 0);
  });

  // Some more detailed observe() testing, to make sure the diffing
  // works. These aren't the only correct answers, but they cover what
  // our current implementations (on client and server) return.

/*
  expectObserve('a(10,0)a(15,1)a(20,2)a(25,3)a(30,4)', function () {
    coll.insert({run: run, x: 10});
    coll.insert({run: run, x: 15});
    coll.insert({run: run, x: 20});
    coll.insert({run: run, x: 25});
    coll.insert({run: run, x: 30});
  });

  expectObserve('c(0,2,20)m(0,2,0)c(35,1,10)m(35,1,4)', function () {
    coll.update({run: run, x: 20}, {$set: {x: 0}});
    coll.update({run: run, x: 10}, {$set: {x: 35}});
  });
*/

  // fuzz test of observe(), especially the server-side diffing
  var actual = [];
  var correct = [];
  var counters = {add: 0, change: 0, move: 0, remove: 0};

  var obs2 = coll.find({run: run}, {sort: ["x"]}).observe({
    added: function (doc, before_index) {
      counters.add++;
      actual.splice(before_index, 0, doc.x);
    },
    changed: function (new_doc, at_index, old_doc) {
      counters.change++;
      test.equal(actual[at_index], old_doc.x);
      actual[at_index] = new_doc.x;
    },
    moved: function (doc, old_index, new_index) {
      counters.move++;
      test.equal(actual[old_index], doc.x);
      actual.splice(old_index, 1);
      actual.splice(new_index, 0, doc.x);
    },
    removed: function (doc, at_index) {
      counters.remove++;
      test.equal(actual[at_index], doc.x);
      actual.splice(at_index, 1);
    }
  });

  // Random integer in [0,n)
  var rnd = function (n) {
    return Math.floor(Math.random()*n);
  };

  var step = 0;

  var doStep = function () {
    if (step++ === 100) {
      obs.stop();
      obs2.stop();
      onComplete();
      return;
    }

    var max_counters = _.clone(counters);

    captureObserve(function () {
      // Do a batch of 1-5 operations
      var batch_count = rnd(5) + 1;
      for (var i = 0; i < batch_count; i++) {
        // 25% add, 25% remove, 25% change in place, 25% change and move
        var op = rnd(4);
        var which = rnd(correct.length);
        if (op === 0 || step < 2 || !correct.length) {
          // Add
          var x = rnd(1000000);
          coll.insert({run: run, x: x});
          correct.push(x);
          max_counters.add++;
        } else if (op === 1 || op === 2) {
          var x = correct[which];
          if (op === 1)
            // Small change, not likely to cause a move
            var val = x + (rnd(2) ? -1 : 1);
          else
            // Large change, likely to cause a move
            var val = rnd(1000000);
          coll.update({run: run, x: x}, {$set: {x: val}});
          correct[which] = val;
          max_counters.change++;
          max_counters.move++;
        } else {
          coll.remove({run: run, x: correct[which]});
          correct.splice(which, 1);
          max_counters.remove++;
        }
      }
    });

    // Did we actually deliver messages that mutated the array in the
    // right way?
    correct.sort(function (a,b) {return a-b;});
    test.equal(actual, correct);

    // Did we limit ourselves to one 'moved' message per change,
    // rather than O(results) moved messages?
    _.each(max_counters, function (v, k) {
      test.isTrue(max_counters[k] >= counters[k]);
    });

    Meteor.defer(doStep);
  };

  doStep();
});
