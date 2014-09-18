var url = require("url");
var events = require("events");

var _ = require("lodash");
var es = require("elasticsearch");

// Store the models outside of the module, this mimics Mongoose's
// behavior and makes it possible to reference the same models in
// multiple files using .model("name").
var models = {};

// Store the client connection as well so that it's "shared"
var client;

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

var ModelPrototype = {
    populate: function(options, callback) {
        return callback ? this.exec(callback) : this;
    },

    save: function(callback) {
        // If it doesn't exist, use client.index()
    },

    update: function(data, callback) {
        // Use client.update()
    },

    remove: function(callback) {
        // Use client.delete
    },

    exec: function(callback) {
        return this;
    }
};

var ModelStatics = {
    // NOTE: Maybe we don't need this? Move to just find?
    search: function(query, options, callback) {
        return callback ? this.exec(callback) : this;
    },

    find: function(query, callback) {
        var q = new Query(query, this);
        return callback ? q.exec(callback) : q;
    },

    findOne: function(query, callback) {
        var q = new Query(query, this);
        q.limit(1);
        return callback ? q.exec(callback) : q;
    },

    findById: function(id, callback) {
        var q = new Query({id: id}, this);
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
};

var Results = function(results, model) {
    results.forEach(function(item, i) {
        this[i] = new model(item);
    }.bind(this));

    this.length = results.length;

    this.execQueue = [];
};

Results.prototype = {
    populate: function(options, callback) {
        if (!callback) {
            this.execQueue.push("populate", [options]);
            return this;
        }

        async.eachLimit(this, 4, function(item, callback) {
            item.populate(options, callback);
        }, callback);

        return this;
    },

    save: function(callback) {
        if (!callback) {
            this.execQueue.push("save", []);
            return this;
        }

        async.eachLimit(this, 4, function(item, callback) {
            item.save(callback);
        }, callback);

        return this;
    },

    update: function(data, callback) {
        if (!callback) {
            this.execQueue.push("update", [data]);
            return this;
        }

        async.eachLimit(this, 4, function(item, callback) {
            item.update(data, callback);
        }, callback);

        return this;
    },

    remove: function(callback) {
        if (!callback) {
            this.execQueue.push("remove", []);
            return this;
        }

        async.eachLimit(this, 4, function(item, callback) {
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

var Query = function(query, model) {
    // Support: find by null
    this.query = query || {};
    this.model = model;
    this.options = {
        limit: 10,
        skip: 0,
        fields: true,
        lean: false,
        populate: "",
        sort: {}
    };
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
        if (this.query.id) {
            client.get({
                // TODO: Figure out index and type
                index: "",
                type: "",
                id: this.query.id
            }, function(err, result) {
                if (err) {
                    return callback(err);
                }

                if (!this.options.lean || this.options.populate) {
                    result = new model(result);
                }

                if (this.options.populate) {
                    result.populate(this.options.populate, callback);
                } else {
                    callback(err, result);
                }
            });

            return;
        }

        var query = {
            // TODO: Figure out index and type
            index: "",
            type: "",
            from: this.options.skip,
            size: this.options.limit,
            fields: this.options.fields.split(/\s+/),
            body: {
                query: {
                   match: this.query
                },
                sort: {}
            }
        };

        Object.keys(this.options.sort).forEach(function(key) {
            var val = this.options.sort[key];
            var dir = val === "desc" || val === "descending" || val === -1 ?
                "desc" : "asc";
            query.body.sort[key] = dir;
        }.bind(this));

        // TODO: Use .get() when only _id is being used
        // and use +realtime: true

        client.search(query, function(err, response) {
            if (err) {
                return callback(err);
            }

            var results = response.hits.hits;

            if (!this.options.lean || this.options.populate) {
                results = new Results(results, model);
            }

            if (this.options.populate) {
                async.eachLimit(results, 4, function(item, callback) {
                    item.populate(this.options.populate, callback);
                }.bind(this), callback);
            } else {
                callback(err, results);
            }
        }.bind(this));

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

        client = new es.Client({
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
            var Model = function(data) {
                this._type = name;
                // TODO: Bring in properties, etc.
                _.extend(this, data);
                // TODO: Set virtuals
            };

            Model.displayName = name;

            Model.prototype = _.extend({}, ModelPrototype);

            _.extend(Model, ModelStatics);

            models[name] = Model;
        }

        if (!(name in models)) {
            throw "Model not registered: " + name;
        }

        return models[name];
    },

    Schema: Schema
};