"use strict";

const SerDe = require("./../index.js").SerDe;
const RegistryClient = require("./../index.js").RegistryClient;

const avro = require('avsc');
const mocha = require('mocha');
const sinon = require('sinon');
const chai = require('chai');
const expect = chai.expect;

describe("SerDe unit tests", function () {
  const id = 10;
  const version = 20;
  const name = 'aName';

  const schema = '{\n ' +
    '"type": "record",\n' +
    '"name": "aName",\n' +
    '"namespace": "test",\n' +
    '"fields": [{\n' +
    '  "name": "field1",\n' +
    '  "type": "string"\n' +
    '}],\n' +
    '"version": 20\n' +
    '}\n';

  const rc = new RegistryClient({});
  const serde = new SerDe(rc);

  mocha.beforeEach(function () {
    this.sandbox = sinon.sandbox.create();
  });

  mocha.afterEach(function () {
    this.sandbox.restore();
  });

  it('should serde protocol bytes', function () {
    //Given metadata
    const metadata = {
      protocol: 1,
      metadataId: 1234567890,
      version: 10
    };

    //When serde protocol
    const buf = Buffer.allocUnsafe(13);
    SerDe.writeProtocolMsg(metadata.metadataId, metadata.version, buf);
    const res = SerDe.readProtocolMsg(buf);

    //Then verify result
    expect(JSON.stringify(res)).to.equal(JSON.stringify(metadata));
  });

  it('should get type', function () {
    const expected = avro.parse(schema);

    this.sandbox.stub(rc, 'getSchemaById').withArgs(id).returns(Promise.resolve({schemaMetadata: {name: name}}));
    this.sandbox.stub(rc, 'getSchemaVersion').withArgs(name, version).returns(Promise.resolve({schemaText: schema}));

    return serde.getType(id, version).then(function (res) {
      expect(JSON.stringify(res)).to.equal(JSON.stringify(expected));
    });
  });

  it('should serde as avro payload', function () {
    const data = {
      field1: "aValue"
    };

    this.sandbox.stub(rc, 'getSchemaById').withArgs(id).returns(Promise.resolve({schemaMetadata: {name: name}}));
    this.sandbox.stub(rc, 'getSchemaVersion').withArgs(name, version).returns(Promise.resolve({schemaText: schema}));

    return serde.serialize(data, id, version)
      .then(buf => {
        expect(buf.length).to.greaterThan(13);
        return serde.deserialize(buf);
      })
      .then(result => {
        expect(JSON.stringify(result)).to.equal(JSON.stringify(data));
      });
  });
});


