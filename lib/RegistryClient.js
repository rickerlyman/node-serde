"use strict";

const request = require("request");
const Promise = require("bluebird");
const logger = require('./Logger.js');

class RegistryClient {

  constructor({protocol = "http", host = "localhost", port = 9090}) {
    this.protocol = protocol;
    this.host = host;
    this.port = port;
  }

  getSchemas() {
    const options = this._getOptions(`/schemas`);
    return this.request(options);
  }

  getSchemaById(metadataId) {
    const options = this._getOptions(`/schemasById/${metadataId}`);
    return this.request(options);
  }

  getSchemaByName(name) {
    const options = this._getOptions(`/schemas/${name}`);
    return this.request(options);
  }

  getSchemaVersions(name) {
    const options = this._getOptions(`/schemas/${name}/versions`);
    return this.request(options);
  }

  getSchemaVersion(name, version = "latest") {
    const options = this._getOptions(`/schemas/${name}/versions/${version}`);
    return this.request(options);
  }

  _getOptions(path = "/", method = "GET") {
    return {
      url: `${this.protocol}://${this.host}:${this.port}/api/v1/schemaregistry${path}`,
      method,
      headers: {
        'accept': 'application/json'
      },
      json: true
    };
  }

  request(options) {
    return new Promise((resolve, reject) => {
      logger.info(`Calling ${options.method} to ${options.url}`);

      request.get(options, function (err, response, body) {
        if (err) {
          logger.error(`Failed to process request for ${options}: ${err}`);
          reject(err);
        }

        resolve(body);
      });
    });
  }
}

module.exports = RegistryClient;
