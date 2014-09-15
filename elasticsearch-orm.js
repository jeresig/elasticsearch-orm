var url = require("url");
var events = require("events");

var _ = require("lodash");
var es = require("elasticsearch");

// Store the models outside of the module, this mimics Mongoose's
// behavior and makes it possible to reference the same models in
// multiple files using .model("name").
var models = {};

var Schema = function(options) {
    this.methods = {};
    this.statics = {};
};

Schema.prototype = {
    plugin: function(obj, options) {

    },

    virtual: function(name) {
        return {
            "get": function(callback) {

            },
            "set": function(callback) {

            }
        };
    }
};

var Model = function(name, options, client) {
    this.name = name;
    this.options = options;
    this.execQueue = [];
    this.client = client;
};

Model.prototype = {
    populate: function(options, callback) {
        return callback ? this.exec(callback) : this;
    },

    save: function(callback) {

    },

    update: function(data, callback) {

    },

    remove: function(callback) {

    },

    exec: function(callback) {
        return this;
    }
};

var Results = function(results) {
    this.results = results;
    this.execQueue = [];
};

Results.prototype = {
    populate: function(options, callback) {
        if (!callback) {
            this.execQueue.push("populate", [options]);
            return this;
        }

        async.eachLimit(this.results, 4, function(item, callback) {
            item.populate(options, callback);
        }, callback);

        return this;
    },

    save: function(callback) {
        if (!callback) {
            this.execQueue.push("save", []);
            return this;
        }

        async.eachLimit(this.results, 4, function(item, callback) {
            item.save(callback);
        }, callback);

        return this;
    },

    update: function(data, callback) {
        if (!callback) {
            this.execQueue.push("update", [data]);
            return this;
        }

        async.eachLimit(this.results, 4, function(item, callback) {
            item.update(data, callback);
        }, callback);

        return this;
    },

    remove: function(callback) {
        if (!callback) {
            this.execQueue.push("remove", []);
            return this;
        }

        async.eachLimit(this.results, 4, function(item, callback) {
            item.remove(callback);
        }, callback);

        return this;
    },

    exec: function(callback) {
        async.eachLimit(this.execQueue, 1, function(call, callback) {
            this[call[0]].apply(this, call[1].concat([callback]));
        }.bind(this), callback);

        return this;
    }
};

_.extend(Model, {
    // NOTE: Maybe we don't need this? Move to just find?
    search: function(query, options, callback) {
        return callback ? this.exec(callback) : this;
    },

    find: function(query, callback) {
        var q = new Query(query);
        return callback ? q.exec(callback) : q;
    },

    findOne: function(query, callback) {
        var q = new Query(query);
        q.limit(1);
        return callback ? q.exec(callback) : q;
    },

    findById: function(id, callback) {
        var q = new Query({id: id});
        return callback ? q.exec(callback) : q;
    },

    create: function(data, callback) {
        var model = new this(data);
        model.save(callback);
    },

    update: function(query, data, options, callback) {
        // Options: upsert, multi
        var processResults = function(err, results) {
            if (results.length === 0) {
                if (options.upsert) {
                    this.create(data, callback);
                } else {
                    callback(null, results);
                }
            } else {
                results.update(data, callback);
            }
        }.bind(this);

        return options.multi ?
            this.find(query, processResults) :
            this.findOne(query, processResults);
    },

    count: function(query, callback) {

    }
});

var Query = function(query) {
    // Support: find by null
    this.query = query;
    this.options = {};
};

Query.prototype = {
    sort: function(options, callback) {
        this.options.sort = options;
        return callback ? this.exec(callback) : this;
    },

    limit: function(num, callback) {
        this.options.limit = num;
        return callback ? this.exec(callback) : this;
    },

    skip: function(num, callback) {
        this.options.skip = num;
        return callback ? this.exec(callback) : this;
    },

    select: function(fields, callback) {
        this.options.fields = fields;
        return callback ? this.exec(callback) : this;
    },

    lean: function(callback) {
        this.options.lean = true;
        return callback ? this.exec(callback) : this;
    },

    populate: function(options, callback) {
        this.options.populate = options;
        return callback ? this.exec(callback) : this;
    },

    exec: function(callback) {
        return this;
    },

    stream: function() {
        // TODO: Return stream object
        // Events: data, error, close
        // Support: this.pause(), this.resume()
    }
};

module.exports = {
    connect: function(esURL) {
        var urlParts = url.parse(esURL);

        this.client = new es.Client({
            host: urlParts.host,
            apiVersion: "1.3"
        });

        // Simulate the connection object
        this.connection = new events.EventEmitter();

        // Produce an 'open' event so that modules can start running
        process.nextTick(function() {
            this.connection.emit("open");
        }.bind(this));
    },

    model: function(name, schema) {
        if (schema) {
            // TODO: Turn this into a proper model
            models[name] = new Model(name, schema, this.client);
        }

        if (!(name in models)) {
            throw "Model not registered: " + name;
        }

        return models[name];
    },

    Schema: Schema
};