var url = require("url");
var events = require("events");

var _ = require("lodash");
var async = require("async");
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
    this.virtuals = {};
};

Schema.prototype = {
    plugin: function(obj, options) {

    },

    virtual: function(name) {
        this.virtuals[name] = new Virtual();
        return this.virtuals[name];
    }
};

var Virtual = function() {};

Virtual.prototype = {
    getter: function() {},

    setter: function() {},

    "get": function(callback) {
        this.getter = callback;
    },

    "set": function(callback) {
        this.setter = callback;
    }
};

var ModelPrototype = {
    _populate: function(curObj, parts, callback) {
        var remaining = parts;

        async.eachLimit(parts, 1, function(prop, callback) {
            if (remaining.length === 0) {
                return callback();
            }

            remaining = remaining.splice(1);
            curObj = curObj[prop];

            if (curObj[prop] instanceof Array) {
                if (typeof curObj[prop][0] === "string") {
                    // Find many by ID
                    this.model.findById(curObj[prop], function(err, results) {
                        if (err) {
                            return callback(err);
                        }

                        for (var i = 0; i < results.length; i++) {
                            curObj[prop][i] = results[i];
                        }
                        callback();
                    }.bind(this));

                } else {
                    async.eachLimit(curObj[prop], 4, function(data, callback) {
                        this._populate(data, remaining, callback);
                    }.bind(this), function(err) {
                        remaining = [];
                        callback(err);
                    });
                }

            } else if (typeof curObj[prop] === "string") {
                this.model.findById(curObj[prop], function(err, data) {
                    if (!err && data) {
                        curObj[prop] = data;
                    }

                    callback(err);
                }.bind(this));

            } else {
                curObj = curObj[prop];
                callback();
            }
        }, callback);

        return this;
    },

    populate: function(path, callback) {
        if (!callback) {
            this.execQueue.push("populate", [path]);
            return this;
        }

        var parts = String(path).split(".");
        return this._populate(this.data, parts, callback);
    },

    save: function(callback) {
        this.validate(function(err) {
            if (err) {
                return callback(err);
            }

            client.index({
                index: this.index,
                type: this.type,
                id: this.id, // Optional?
                //version: this.version,
                body: this.data
            }, function(err, response) {
                // Maybe re-generate to pull in missing id?
                callback(err, this);
            }.bind(this));
        }.bind(this));
    },

    update: function(data, callback) {
        if (!this.id) {
            return callback(new Error("Cannot update: no ID specified."));
        }

        this.validate(function(err) {
            if (err) {
                return callback(err);
            }

            client.update({
                index: this.index,
                type: this.type,
                id: this.id,
                version: this.version,
                body: {
                    doc: this.data
                }
            }, function(err, response) {
                callback(err, this);
            }.bind(this));
        }.bind(this));
    },

    remove: function(callback) {
        if (!this.id) {
            return callback(new Error("Cannot remove: no ID specified."));
        }

        // Use client.delete
        client.delete({
            index: this.index,
            type: this.type,
            id: this.id
        }, function(err, response) {
            callback(err, this);
        }.bind(this));
    },

    validate: function(callback) {
        callback(null);
    },

    exec: function(callback) {
        async.eachLimit(this.execQueue, 1, function(call, callback) {
            this[call[0]].apply(this, call[1].concat([callback]));
        }.bind(this), callback);

        return this;
    }
};

var ModelStatics = {
    // NOTE: Maybe we don't need this? Move to just find?
    search: function(query, options, callback) {
        return callback ? this.exec(callback) : this;
    },

    where: function(query, callback) {
        var q = new Query(query, this);
        return callback ? q.exec(callback) : q;
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
        var q = new Query(query, this);
        return callback ? q.count(callback) : q;
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

    count: function(query, callback) {
        this.options.count = true;
        return callback ? this.exec(callback) : this;
    },

    exec: function(callback) {
        var model = this.model;
        var searchType = "search";
        var single = typeof this.query.id === "string";

        var query = {
            index: model.index,
            type: model.type,
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

        // TODO: Use .get() when only id is being used
        // and use +realtime: true

        if (this.query.id) {
            query.body = {
                ids: _.isArray(this.query.id) ?
                    [this.query.id] : this.query.id
            };

            searchType = "mget";
        }

        client[searchType](query, function(err, response) {
            if (err) {
                return callback(err);
            }

            var results = response.hits.hits;

            if (this.options.count) {
                results = response.hits.total;
                single = false;

            } else if (!this.options.lean || this.options.populate) {
                results = new Results(results, model);
            }

            if (this.options.populate && !this.options.count) {
                async.eachLimit(results, 4, function(item, callback) {
                    item.populate(this.options.populate, callback);
                }.bind(this), function(err) {
                    callback(err, single ? results[0] : results);
                });

            } else {
                callback(err, single ? results[0] : results);
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
                this.type = name;
                // TODO: Bring in properties, etc.
                this.data = data;
                this.execQueue = [];
                // TODO: Set virtuals
            };

            Model.displayName = name;
            Model.index = name;
            Model.type = name;

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