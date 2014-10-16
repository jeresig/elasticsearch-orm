var es = require("../elasticsearch-orm");
var expect = require("expect.js");

describe("Schema", function() {
    it("can be instantiated", function() {
        new es.Schema();
    });

    it("can be instantiated with empty object", function() {
        new es.Schema({});
    });

    it("can be instantiated with basic schema", function() {
        new es.Schema({
            name: String
        });
    });
});

describe("Model", function() {
    var schema;

    before(function() {
        schema = new es.Schema({
            name: String
        });
    });

    it("throws an error if it doesn't exist", function() {
        expect(function() {
            es.model("Nope");
        }).to.throwError(/Model not registered: Nope/);
    });

    it("can be built from a schema", function() {
        es.model("Test", schema);
    });

    it("can be accessed later", function() {
        es.model("Test", schema);
        var Test = es.model("Test");
        expect(Test).to.be.ok();
    });

    it("can be instantiated", function() {
        var Test = es.model("Test", schema);
        var test = new Test();
        expect(test).to.be.ok();
    });

    it("can set and get a property", function() {
        var Test = es.model("Test", schema);
        var test = new Test();
        test.name = "John";
        expect(test.name).to.equal("John");
    });
});

describe("String Schema Type", function() {
    it("check validation on property set", function() {
        var Test = es.model("Test", new es.Schema({
            name: String
        }));

        var test = new Test();
        test.name = false;
        expect(test.name).to.equal("false");
    });

    it("check schema type", function() {
        var Test = es.model("Test", new es.Schema({
            name: {type: String}
        }));

        var test = new Test();
        test.name = false;
        expect(test.name).to.equal("false");
    });

    it("check schema string type", function() {
        var Test = es.model("Test", new es.Schema({
            name: {type: "string"}
        }));

        var test = new Test();
        test.name = false;
        expect(test.name).to.equal("false");
    });

    it("check schema type", function() {
        var Test = es.model("Test", new es.Schema({
            name: {type: String, "enum": [
                "cat", "dog"
            ]}
        }));

        var test = new Test();
        test.name = "cat";
        expect(test.name).to.equal("cat");

        expect(function() {
            test.name = "test";
        }).to.throwError(/Expected enum value not found/);
    });
});