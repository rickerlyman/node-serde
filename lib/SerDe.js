"use strict";

const avro = require('avsc');
const logger = require('./Logger.js');

const CURRENT_PROTOCOL_VERSION = 0x1;

class SerDe {
  constructor(rc) {
    this.rc = rc;
    this.types = {};
  }

  serialize(data, metadataId, version, length = 1024) {
    return new Promise((resolve, reject) => {
      const buf = new Buffer(length);
      SerDe.writeProtocolMsg(metadataId, version, buf);

      this.getType(metadataId, version)
        .then(type => {
          const pos = type.encode(data, buf, 13);

          if (pos < 0) {
            // The buffer was too short, need to resize
            resolve(this.serialize(data, metadataId, version, length - pos));
          }

          resolve(buf.slice(0, pos));
        });
    });
  }

  deserialize(buf) {
    return new Promise((resolve, reject) => {
      const metadata = SerDe.readProtocolMsg(buf);

      this.getType(metadata.metadataId, metadata.version).then(function (type) {
        const decoded = type.decode(buf, 13);

        resolve(decoded.value);
      });
    });
  }

  // protocol format:
  // 1 byte  : protocol version
  // 8 bytes : schema metadata Id
  // 4 bytes : schema version
  static readProtocolMsg(buf) {
    return {
      protocol: buf.readUIntBE(0, 1),
      metadataId: (buf.readUInt32BE(1) << 8) + buf.readUInt32BE(5),
      version: buf.readUInt32BE(9)
    }
  }

  static writeProtocolMsg(metadataId, version, buf) {
    buf.writeUInt8(CURRENT_PROTOCOL_VERSION, 0);
    buf.writeUInt32BE(metadataId >> 8, 1);
    buf.writeUInt32BE(metadataId & 0x00ff, 5);
    buf.writeUInt32BE(version, 9);

    return buf;
  }

  getType(metadataId, version) {
    const types = this.types;
    const client = this.rc;

    //TODO handle client errors
    return new Promise((resolve, reject) => {
      const key = [metadataId, version];
      const type = types[key];
      if (!type) {
        client.getSchemaById(metadataId).then(function (res) {
          logger.info(`Schema metadata for metadataId=${metadataId} is ${JSON.stringify(res.schemaMetadata)}`);
          client.getSchemaVersion(res.schemaMetadata.name, version)
            .then(res => {
              types[key] = avro.parse(res.schemaText);
              resolve(types[key]);
            });
        });
      } else {
        resolve(type);
      }
    });
  }
}

module.exports = SerDe;
