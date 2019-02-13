/**
 * @fileOverview Wrapper for node-rdkafka Producer Ctor, a mixin.
 */
var Promise = require('bluebird');
var cip = require('cip');
var kafka = require('node-rdkafka');

var magicByte = require('./magic-byte');
var log = require('./log.lib').getChild(__filename);

/**
 * Wrapper for node-rdkafka Produce Ctor, a mixin.
 *
 * @constructor
 */
var Producer = module.exports = cip.extend();

/**
 * The wrapper of the node-rdkafka package Producer Ctor.
 *
 * @param {Object} opts Producer general options.
 * @param {Object=} topts Producer topic options.
 * @see https://github.com/edenhill/librdkafka/blob/2213fb29f98a7a73f22da21ef85e0783f6fd67c4/CONFIGURATION.md
 * @return {Promise(kafka.Producer)} A Promise.
 */
Producer.prototype.getProducer = Promise.method(function (opts, topts) {
  if (!opts) {
    opts = {};
  }

  if (!opts['metadata.broker.list']) {
    opts['metadata.broker.list'] = this.kafkaBrokerUrl;
  }

  log.info('getProducer() :: Starting producer with options:', opts);

  var producer = new kafka.Producer(opts, topts);

  this._producers.push(producer);

  // hack node-rdkafka
  producer.__kafkaAvro_produce = producer.produce;
  producer.produce = this._produceWrapper.bind(this, producer);

  return new Promise(function(resolve, reject) {
    producer.on('ready', function() {
      log.debug('getProducer() :: Got "ready" event.');
      resolve(producer);
    });

    producer.connect({}, function(err) {
      if (err) {
        log.error('getProducer() :: Connect failed:', err);
        reject(err);
        return;
      }
      log.debug('getProducer() :: Got "connect()" callback.');
      resolve(producer); // depend on Promises' single resolve contract.
    });
  })
    .return(producer);
});

/**
 * The node-rdkafka produce method wrapper, will validate and serialize
 * the message against the existing schemas.
 *
 * @param {kafka.Producer} producerInstance node-rdkafka instance.
 * @param {string} topicName The topic name.
 * @param {number} partition The partition to produce on.
 * @param {Object} value The message.
 * @param {string|number} key The partioning key.
 * @param {number} timestamp The create time value.
 * @param {*=} optOpaque Pass vars to receipt handler.
 */
Producer.prototype._produceWrapper = function (producerInstance, topicName,
  partition, value, key, timestamp, optOpaque) {

  if (!this.sr.valueSchemas[topicName] || !this.sr.keySchemas[topicName]) {
    // topic not found in schemas, bail early

    log.warn('_produceWrapper() :: Warning, did not find topic on SR:',
      topicName);

    var bufVal = !this.sr.valueSchemas[topicName]
      ? new Buffer(JSON.stringify(value))
      : value;

    var keyVal = !this.sr.keySchemas[topicName]
      ? new Buffer(JSON.stringify(key))
      : key;

    return producerInstance.__kafkaAvro_produce(topicName, partition, bufVal,
      keyVal, timestamp, optOpaque);
  }

  var typeKey = this.sr.keySchemas[topicName];
  var schemaIdKey = this.sr.schemaIds[topicName + '-key'];
  var bufKey = this.serialize(typeKey, schemaIdKey, key);

  var typeValue = this.sr.valueSchemas[topicName];
  var schemaIdValue = this.sr.schemaIds[topicName + '-value'];
  var bufValue = this.serialize(typeValue, schemaIdValue, value);

  return producerInstance.__kafkaAvro_produce(topicName, partition, bufValue,
    bufKey, timestamp, optOpaque);
};

/**
 * Serialize the message using avro.
 *
 * @param {avsc.Type} type The avro type instance.
 * @param {number} schemaId The schema id.
 * @param {*} value The value to serialize.
 * @return {Buffer} Serialized buffer.
 * @private
 */
Producer.prototype.serialize = function (type, schemaId, value) {
  var bufValue = magicByte.toMessageBuffer(value, type, schemaId);

  return bufValue;
};
