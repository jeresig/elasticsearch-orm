var url = require("url");
var events = require("events");
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

    }
};

var Model = function(name, options) {
    this.execQueue = [];
};

Model.prototype = {
    search: function(options, callback) {
        return callback ? this.exec(callback) : this;
    },

    find: function(options, callback) {
        return callback ? this.exec(callback) : this;
    },

    findOne: function(options, callback) {
        return callback ? this.exec(callback) : this;
    },

    sort: function(options, callback) {
        return callback ? this.exec(callback) : this;
    },

    limit: function(num, callback) {
        return callback ? this.exec(callback) : this;
    },

    skip: function(num, callback) {
        return callback ? this.exec(callback) : this;
    },

    populate: function(options, callback) {
        return callback ? this.exec(callback) : this;
    },

    exec: function(callback) {
        return this;
    },

    stream: function() {
        // TODO: Return stream object
    }
}

module.exports = {
    connect: function(esURL) {
        var urlParts = url.parse(esURL);

        this._connection = new es.Client({
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
            models[name] = new Model(name, schema);
        }

        if (!(name in models)) {
            throw "Model not registered: " + name;
        }

        return models[name];
    },

    Schema: Schema
};