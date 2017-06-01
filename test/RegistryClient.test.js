"use strict";

const RegistryClient = require("./../index.js").RegistryClient;

const request = require('request');

const sinon = require('sinon');
const chai = require('chai');
const mocha = require('mocha');
const expect = chai.expect;

describe("Registry client unit test", function () {
  const rc = new RegistryClient({});
  const schema = '{\n ' +
    '"type": "record",\n' +
    '"name": "aName",\n' +
    '"namespace": "test",\n' +
    '"fields": [{\n' +
    '  "name": "name",\n' +
    '  "type": "string"\n' +
    '}],\n' +
    '"version": 2\n' +
    '}\n';

  mocha.beforeEach(function () {
    this.sandbox = sinon.sandbox.create();
  });

  mocha.afterEach(function () {
    this.sandbox.restore();
  });

  it('should return schemas', function () {
    const expected = {entities: [{name: 'test', description: null}]};

    this.sandbox.stub(request, 'get').yields(null, null, expected);

    return rc.getSchemas().then(function (res) {
      expect(res.entities[0].name).to.equal('test');
    });
  });

  it('should return schema by id', function () {
    const id = 1;

    const expected = {
      schemaMetadata:
        {
          type: 'avro',
          schemaGroup: 'Kafka',
          name: 'aName',
          description: 'aDescription',
          compatibility: 'BACKWARD',
          evolve: true
        },
      id: 1,
      timestamp: 1495633103410
    };

    this.sandbox.stub(request, 'get').yields(null, null, expected);

    return rc.getSchemaById(id).then(function (res) {
      expect(res.id).to.equal(id);
      expect(res.schemaMetadata.name).to.equal('aName');
    });
  });

  it('should return schema by name', function () {
    const name = 'aName';

    const expected = {
      schemaMetadata:
        {
          type: 'avro',
          schemaGroup: 'Kafka',
          name: 'aName',
          description: 'aDescription',
          compatibility: 'BACKWARD',
          evolve: true
        },
      id: 1,
      timestamp: 1495633103410
    };

    this.sandbox.stub(request, 'get').yields(null, null, expected);

    return rc.getSchemaByName(name).then(function (res) {
      expect(res.id).to.equal(1);
      expect(res.schemaMetadata.name).to.equal('aName');
    });
  });


  it('should return schema versions', function () {
    const name = 'aName';

    const expected = {
      entities:
        [{
          description: 'test',
          version: 1,
          schemaText: schema,
          timestamp: 1495633103571
        },
          {
            description: 'test2',
            version: 2,
            schemaText: schema,
            timestamp: 1495698852213
          }]
    };

    this.sandbox.stub(request, 'get').yields(null, null, expected);

    return rc.getSchemaVersions(name).then(function (res) {
      expect(res.entities[0].version).to.equal(1);
      expect(res.entities[1].version).to.equal(2);
    });
  });

  it('should return latest schema version', function () {
    const name = 'aName';

    const expected = {
      description: 'test2',
      version: 2,
      schemaText: schema,
      timestamp: 1495698852213
    };

    this.sandbox.stub(request, 'get').yields(null, null, expected);

    return rc.getSchemaVersion(name).then(function (res) {
      expect(res.version).to.equal(2);
    });
  });


  it('should return schema version', function () {
    const name = 'aName';
    const version = 1;

    const expected = {
      description: 'test',
      version: 1,
      schemaText: schema,
      timestamp: 1495633103571
    };

    this.sandbox.stub(request, 'get').yields(null, null, expected);

    return rc.getSchemaVersion(name, version).then(function (res) {
      expect(res.version).to.equal(version);
    });
  });
});
