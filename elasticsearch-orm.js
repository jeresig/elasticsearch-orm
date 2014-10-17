var url = require("url");
var events = require("events");
var stream = require("stream");

var _ = require("lodash");
var async = require("async");
var diff = require("deep-diff").diff;
var es = require("elasticsearch");

// Store the models outside of the module, this mimics Mongoose's
// behavior and makes it possible to reference the same models in
// multiple files using .model("name").
var models = {};

// Store the client connection as well so that it's "shared"
var client;

var SchemaType = function(options) {
    this.init(options);
};

SchemaType.findType = function(val) {
    for (var i = 0, l = SchemaType.types.length; i < l; i++) {
        var type = SchemaType.types[i];
        if (type.object === val || val.type === type.type ||
            type.object === val.type) {
                return type;
        }
    }
};

SchemaType.prototype = {
    init: function(options) {
        this.options = options || {};
        this.getter = function() {};
        this.setter = function() {};
    },

    validate: function(val) {
        if (this.coherce) {
            val = this.coherce(val);
        }

        for (var option in this.options) {
            if (this.options[option] && this[option]) {
                // TODO: Maybe better handle exceptions?
                val = this[option](val);
            }
        }

        return val;
    },

    default: function(val) {
        return val == null ? this.options.default : val;
    },

    required: function(val) {
        if (val == null) {
            throw new Error("Undefined property, value required.");
        }

        return val;
    }
};

var SchemaString = function(options) {
    this.init(options);
};

SchemaString.object = String;
SchemaString.type = "string";
SchemaString.prototype = new SchemaType();

_.extend(SchemaString.prototype, {
    coherce: function(val) {
        if (typeof val === "object" || val === undefined) {
            throw new Error("Not a valid string.");
        }

        return String(val);
    },

    "enum": function(val) {
        if (this.options.enum.indexOf(val) < 0) {
            throw new Error("Expected enum value not found.");
        }

        return val;
    },

    match: function(val) {
        if (this.options.match.exec(val)) {
            throw new Error("Value does not match regex.");
        }

        return val;
    },

    lowercase: function(val) {
        return val.toLowerCase();
    },

    uppercase: function(val) {
        return val.toUpperCase();
    },

    trim: function(val) {
        return val.trim();
    }
});

var SchemaNumber = function(options) {
    this.init(options);
};

SchemaNumber.object = Number;
SchemaNumber.type = "number";
SchemaNumber.prototype = new SchemaType();

_.extend(SchemaNumber.prototype, {
    coherce: function(val) {
        val = parseFloat(val);

        if (isNaN(val)) {
            throw new Error("Not a number.");
        }

        return val;
    },

    min: function(val) {
        if (val < this.options.min) {
            throw new Error("Expected value to be greater than " +
                this.options.min + ".");
        }

        return val;
    },

    max: function(val) {
        if (val > this.options.max) {
            throw new Error("Expected value to be less than " +
                this.options.max + ".");
        }

        return val;
    }
});

var SchemaBoolean = function(options) {
    this.init(options);
};

SchemaBoolean.object = Boolean;
SchemaBoolean.type = "boolean";
SchemaBoolean.prototype = new SchemaType();

_.extend(SchemaBoolean.prototype, {
    coherce: function(val) {
        return !!val;
    }
});

var SchemaDate = function(options) {
    this.init(options);
};

SchemaDate.object = Date;
SchemaDate.type = "date";
SchemaDate.prototype = new SchemaType();

_.extend(SchemaDate.prototype, {
    coherce: function(val) {
        val = new Date(val);

        // Use this to trigger an exception on invalid date
        val.toISOString();

        return val;
    }
});

var SchemaObjectId = function(options) {
    this.init(options);
};

SchemaObjectId.object = SchemaObjectId;
SchemaObjectId.type = "objectid";
SchemaObjectId.prototype = new SchemaType();

SchemaType.types = [
    SchemaString,
    SchemaNumber,
    SchemaBoolean,
    SchemaDate,
    SchemaObjectId
];

var Schema = function(props) {
    this.props = props;
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
    },

    add: function(props) {
        _.extend(this.props, props);
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
        return this._populate(this.__data, parts, callback);
    },

    diff: function() {
        return diff(this.__origData, this.__data);
    },

    hasChanged: function() {
        return !!(diff && diff.length > 0);
    },

    save: function(callback) {
        if (!this.hasChanged()) {
            return callback();
        }

        return this.validate(function(err) {
            if (err) {
                return callback(err);
            }

            client.index({
                index: this.index,
                type: this.type,
                id: this.id, // Optional?
                //version: this.version,
                body: this.__data
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

        if (!this.hasChanged()) {
            return callback();
        }

        return this.validate(function(err) {
            if (err) {
                return callback(err);
            }

            client.update({
                index: this.index,
                type: this.type,
                id: this.id,
                version: this.version,
                body: {
                    doc: this.__data
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

        return this;
    },

    _validate: function(data, schema, path) {
        var wrongProp;

        for (var prop in schema) {
            var dataProp = data[prop];
            var schemaProp = schema[prop];
            var type = SchemaType.findType(schemaProp);
            var propPath = path + "." + prop;

            if (!type) {
                if (_.isArray(schemaProp)) {
                    if (!_.isArray(dataProp)) {
                        wrongProp = propPath;
                        break;
                    }

                    for (var i = 0, l = dataProp.length; i < l; i++) {
                        var itemPath = propPath + "[" + i + "]";
                        var ret = this._validate(dataProp[i], schemaProp[0],
                            itemPath);
                        if (ret) {
                            wrongProp = itemPath;
                            break
                        }
                    }

                // It's an object
                } else if (_.isPlainObject(schemaProp)) {
                    if (!_.isPlainObject(dataProp)) {
                        wrongProp = propPath;
                        break;
                    }

                    this._validate(dataProp, schemaProp, itemPath);

                } else {
                    throw new Error("Unknown schema for: " + prop);
                }
            }

            if (!type.validate(dataProp)) {
                wrongProp = propPath;
            }

            if (wrongProp) {
                break;
            }
        }

        return wrongProp;
    },

    validate: function(callback) {
        var wrongProp = this._validate(this.__data, this.schema.props, "");
        callback(wrongProp ? new Error("Mis-match type: " + wrongProp) : null);
        return this;
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
        // Events: data, error, close
        // Support: this.pause(), this.resume()
        var query = this;
        var rs = stream.Readable();
        var start = 0;

        rs._read = function() {
            query.skip(start);

            query.exec(function(err, results) {
                if (err) {
                    rs.emit("error", err);
                    return;
                }

                if (!results || results.length === 0) {
                    rs.push(null);
                    return;
                }

                results.forEach(function(item) {
                    rs.push(item);
                });
            });

            start += query.options.limit;
        };

        return rs;
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

    model: function(modelName, schema) {
        if (!schema) {
            if (!(modelName in models)) {
                throw new Error("Model not registered: " + modelName);
            }

            return models[modelName];
        }

        var Model = function(data) {
            this.__origData = _.cloneDeep(data);
            this.__data = {};
            this.schema = schema;

            for (var name in data) {
                this[name] = data[name];
            }

            // Define properties
            Object.keys(schema.props).forEach(function(name) {
                var schemaProp = schema.props[name];
                var type = new (SchemaType.findType(schemaProp))(schemaProp);

                Object.defineProperty(Model.prototype, name, {
                    get: function() {
                        return this.__data[name];
                    }.bind(this),

                    set: function(value) {
                        value = type.validate(value);
                        this.__data[name] = value;
                    }.bind(this)
                });
            }.bind(this));

            // Then define the virtual properties
            Object.keys(schema.virtuals).forEach(function(name) {
                Object.defineProperty(Model.prototype, name, {
                    get: function() {
                        return schema.virtuals[name].getter.call(this);
                    }.bind(this),

                    set: function(value) {
                        schema.virtuals[name].setter.call(this, val);
                    }.bind(this)
                });
            }.bind(this));
        };

        Model.prototype = {
            _type: modelName,
            _index: modelName
        };

        _.extend(Model, ModelStatics);
        _.extend(Model, schema.statics);

        _.extend(Model.prototype, ModelPrototype);
        _.extend(Model.prototype, schema.methods);

        models[modelName] = Model;

        return Model;
    },

    Schema: Schema
};