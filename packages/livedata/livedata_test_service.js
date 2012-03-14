App.methods({
  echo: function (/* arguments */) {
    return _.toArray(arguments);
  },
  exception: function (where) {
    var shouldThrow =
      (Meteor.is_server && where === "server") ||
      (Meteor.is_client && where === "client") ||
      where === "both";

    if (shouldThrow) {
      e = new Error("Test method throwing an exception");
      e.expected = true;
      throw e;
    }
  }
});

/*****/

Ledger = new Meteor.Collection("ledger");

Meteor.startup(function () {
  if (Meteor.is_server)
    Ledger.remove({}); // XXX can this please be Ledger.remove()?
});

if (Meteor.is_server)
  Meteor.publish('ledger', function (world) {
    return Ledger.find({world: world}, {key: {collection: 'ledger',
                                              world: world}});
  });

App.methods({
  'ledger/transfer': function (world, from_name, to_name, amount, cheat) {
    var from = Ledger.findOne({name: from_name, world: world});
    var to = Ledger.findOne({name: to_name, world: world});

    if (Meteor.is_server)
      cheat = false;

    if (!from) {
      this.error(404, "No such account " + from_name + " in " + world);
      return;
    }

    if (!to) {
      this.error(404, "No such account " + to_name + " in " + world);
      return;
    }

    if (from.balance < amount && !cheat) {
      this.error(409, "Insufficient funds");
      return;
    }

    Ledger.update({_id: from._id}, {$inc: {balance: -amount}});
    Ledger.update({_id: to._id}, {$inc: {balance: amount}});
    Meteor.refresh({collection: 'ledger', world: world});
  }
});

/*****/

Pantry = new Meteor.Collection("pantry");

// When you put something in the panty, you must put in more than one
// of them. But you can assign them to anyone.
Pantry.allowInsert(function (doc) {
  return (doc.quantity && doc.quantity > 1);
});

// You can only update things in the pantry that belong to you, and
// you must guard against races by explicitly including a 'who' clause
// in the selector
Pantry.allowUpdate(function (selector, modifier) {
  return ('who' in selector) && this.user === selector.who;
});

// You can only remove things in the pantry that have a quantity of
// zero, and ditto
Pantry.allowRemove(function (selector) {
  return ('quantity' in selector) && this.quantity === 0;
});

Meteor.startup(function () {
  if (Meteor.is_server)
    Pantry.remove({}); // XXX can this please be Pantry.remove()?
});

if (Meteor.is_server) {
  App.methods({
    'logout': function () {
      this.setUser(null);
    },

    'login': function (who, succeed) {
      if (succeed) {
        this.setUser(who);
        return true;
      } else {
        this.error(401, "Not authorized");
      }
    },

    'relogin': function (token) {
      if (token.length > 2) {
        var who = token.substr(2);
        this.setUser(who);
        return who;
      } else {
        this.error(401, "Bad token");
      }
    }
  });

  Meteor.publish("my_pantry", function (world) {
    if (!this.user) {
      this.complete();
      this.flush();
      return;
    }
    return Pantry.find({who: this.user, world: world});
  });
}

App.methods({
  'read_auth': function () {
    return this.user;
  }
});
