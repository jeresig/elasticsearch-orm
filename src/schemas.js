var _ = require("lodash");

// Store the models outside of the module, this mimics Mongoose's
// behavior and makes it possible to reference the same models in
// multiple files using .model("name").
var models = {};

module.exports = function(ModelPrototype, ModelStatics) {

var findModelBySchema = function(schema) {
    for (var prop in models) {
        if (models[prop].__schema === schema) {
            return models[prop];
        }
    }
};

var findType = function(val) {
    if (val instanceof Schema || val.type instanceof Schema) {
        return SchemaCustom;
    }

    if (_.isArray(val)) {
        return SchemaArray;
    }

    if (!("type" in val) && _.isObject(val) && !_.isFunction(val)) {
        return SchemaObject;
    }

    for (var name in Schema.Types) {
        var type = Schema.Types[name];

        if (type.object === val || val.type === type.type ||
            type.object === val.type) {
                return type;
        }
    }
};

var SchemaType = function(options, prefix) {
    this.init(options, prefix);
};

SchemaType.prototype = {
    init: function(options, prefix) {
        this.options = options || {};
        this.prefix = prefix;
    },

    coherce: function(val) {
        // Functions won't serialize so we ignore them
        if (typeof val === "function" || val == null) {
            return null;
        }

        return val;
    },

    validate: function(val, prefix) {
        prefix = prefix || this.prefix || "";

        try {
            val = this.coherce(val);

            for (var option in this.options) {
                if (this.options[option] && this[option]) {
                    // TODO: Maybe better handle exceptions?
                    val = this[option](val);
                }
            }
        } catch(e) {
            var path = prefix;
            var message = e.message;

            if (/Error \((.*?)\): (.*)$/.test(message)) {
                message = RegExp.$2;
                path = prefix + "." + RegExp.$1;
                path = path.replace(/\.(\d+)/g, "[$1]");
            }

            throw new Error("Error (" + path + "): " + message);
        }

        return val;
    },

    default: function(val) {
        if (val == null && this.options.default != null) {
            val = this.options.default;

            if (typeof val === "function") {
                val = val();
            }

            return this.coherce(val);
        }

        return val;
    },

    required: function(val) {
        if (val == null) {
            throw new Error("Undefined property, value required.");
        }

        return val;
    }
};

var SchemaString = function(options, prefix) {
    this.init(options, prefix);
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

var SchemaNumber = function(options, prefix) {
    this.init(options, prefix);
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

var SchemaBoolean = function(options, prefix) {
    this.init(options, prefix);
};

SchemaBoolean.object = Boolean;
SchemaBoolean.type = "boolean";
SchemaBoolean.prototype = new SchemaType();

_.extend(SchemaBoolean.prototype, {
    coherce: function(val) {
        return !!val;
    }
});

var SchemaDate = function(options, prefix) {
    this.init(options, prefix);
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

var SchemaCustom = function(options, prefix) {
    this.init(options, prefix);
};

SchemaCustom.prototype = new SchemaType();

_.extend(SchemaCustom.prototype, {
    default: function(val) {
        val = SchemaType.prototype.default.call(this, val) || {};

        var schema = this.options.type || this.options;
        var model = findModelBySchema(schema);

        if (!model) {
            throw new Error("No model matching the schema was found.");
        }

        return new model(val);
    },

    coherce: function(val) {
        if (typeof val !== "object" && !_.isPlainObject(val)) {
            throw new Error("Not an object. Unable to turn into a model.");
        }

        var schema = this.options.type || this.options;
        var model = findModelBySchema(schema);

        if (!model) {
            throw new Error("No model matching the schema was found.");
        }

        return new model(val);
    }
});

var SchemaObjectId = function(options, prefix) {
    this.init(options, prefix);
};

SchemaObjectId.object = SchemaObjectId;
SchemaObjectId.type = "objectid";
SchemaObjectId.prototype = new SchemaType();

var genMockArray = function(type, array) {
    array = array || [];

    array.push = function(item) {
        return Array.prototype.push.call(this,
            type.validate(item, this.length.toString()));
    };

    array.unshift = function(item) {
        return Array.prototype.unshift.call(this,
            type.validate(item, "0"));
    };

    return array;
};

var SchemaArray = function(options, prefix) {
    if ("length" in options) {
        if (options[0]) {
            var type = findType(options[0]);

            if (type) {
                options = {subType: new type(options[0])};
            } else {
                options = {};
            }
        } else {
            options = {};
        }
    }

    this.init(options, prefix);
};

SchemaArray.object = Array;
SchemaArray.type = "array";
SchemaArray.prototype = new SchemaType();

_.extend(SchemaArray.prototype, {
    default: function(val) {
        val = SchemaType.prototype.default.call(this, val) || [];

        if (!this.options.subType) {
            return val;
        }

        return genMockArray(this.options.subType, val);
    },

    coherce: function(val) {
        if (val && typeof val === "object" && "length" in val) {
            if (!this.options.subType) {
                return val;
            }

            var ret = genMockArray(this.options.subType);

            for (var i = 0; i < val.length; i++) {
                ret.push(val[i]);
            }

            return ret;
        }

        throw new Error("Not a valid array.");
    }
});

var genMockObject = function(types, obj) {
    // Don't re-bind if we're already working with a mocked object
    if (obj && "__data" in obj) {
        return obj;
    }

    obj = obj || {};

    Object.defineProperty(obj, "__data", {
        value: {}
    });

    Object.keys(types).forEach(function(name) {
        var type = findType(types[name]);

        if (!type || name === "default") {
            return;
        }

        type = new type(types[name]);

        obj.__data[name] = type.default();

        Object.defineProperty(obj, name, {
            get: function() {
                if (type.options.get) {
                    return type.options.get.call(this.__data);
                }

                return obj.__data[name];
            },

            set: function(value) {
                if (type.options.set) {
                    return type.options.set.call(this.__data, value);
                }

                value = type.validate(value, name);
                obj.__data[name] = value;
            },

            enumerable: true
        });
    });

    return obj;
};

var SchemaObject = function(options, prefix) {
    this.init(options, prefix);
};

SchemaObject.object = Object;
SchemaObject.type = "object";
SchemaObject.prototype = new SchemaType();

_.extend(SchemaObject.prototype, {
    default: function(val) {
        val = SchemaType.prototype.default.call(this, val);

        return genMockObject(this.options, val);
    },

    coherce: function(val) {
        if (val && _.isPlainObject(val)) {
            var ret = genMockObject(this.options);

            for (var key in val) {
                if (val.hasOwnProperty(key)) {
                    ret[key] = val[key];
                }
            }

            return ret;
        }

        throw new Error("Not a valid object.");
    }
});

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

Schema.Types = {
    String: SchemaString,
    Number: SchemaNumber,
    Boolean: SchemaBoolean,
    Array: SchemaArray,
    Object: SchemaObject,
    Date: SchemaDate,
    ObjectId: SchemaObjectId,
    Mixed: SchemaType
};

var Virtual = function() {};

Virtual.prototype = {
    getter: function() {},

    setter: function() {},

    get: function(callback) {
        this.getter = callback;
        return this;
    },

    set: function(callback) {
        this.setter = callback;
        return this;
    }
};

return {
    Schema: Schema,

    model: function(modelName, schema) {
        if (!schema) {
            if (!(modelName in models)) {
                throw new Error("Model not registered: " + modelName);
            }

            return models[modelName];
        }

        var Model = function(data) {
            Object.defineProperty(this, "__origData", {
                value: _.cloneDeep(data)
            });
            Object.defineProperty(this, "__data", {
                value: {}
            });
            Object.defineProperty(this, "__schema", {
                value: schema
            });

            // Define properties
            Object.keys(schema.props).forEach(function(name) {
                var schemaProp = schema.props[name];
                var type = new (findType(schemaProp))(schemaProp, name);

                if ("default" in type) {
                    this.__data[name] = type.default();
                }

                Object.defineProperty(this, name, {
                    get: function() {
                        if (type.options.get) {
                            return type.options.get.call(this.__data);
                        }

                        return this.__data[name];
                    }.bind(this),

                    set: function(value) {
                        if (type.options.set) {
                            return type.options.set.call(this.__data, value);
                        }

                        value = type.validate(value);
                        this.__data[name] = value;
                    }.bind(this),

                    enumerable: true
                });
            }.bind(this));

            // Then define the virtual properties
            Object.keys(schema.virtuals).forEach(function(name) {
                Object.defineProperty(this, name, {
                    get: function() {
                        return schema.virtuals[name].getter.call(this);
                    }.bind(this),

                    set: function(value) {
                        schema.virtuals[name].setter.call(this, value);
                    }.bind(this),

                    enumerable: true
                });
            }.bind(this));

            // Bring in the user-specified data
            for (var name in data) {
                this[name] = data[name];
            }
        };

        Object.defineProperty(Model, "__schema", {
            value: schema
        });

        Model.prototype = {
            _type: modelName,
            _index: modelName
        };

        _.extend(Model, ModelStatics || {});
        _.extend(Model, schema.statics);

        _.extend(Model.prototype, ModelPrototype || {});
        _.extend(Model.prototype, schema.methods);

        models[modelName] = Model;

        return Model;
    }
};

};