/**
 * @fileOverview Wrapper for node-rdkafka Consumer Ctor, a mixin.
 */

var Promise = require('bluebird');
var cip = require('cip');
var kafka = require('node-rdkafka');

var magicByte = require('./magic-byte');
var log = require('./log.lib').getChild(__filename);

/**
 * Wrapper for node-rdkafka Consumer Ctor, a mixin.
 *
 * @constructor
 */
var Consumer = module.exports = cip.extend();

/**
 * The wrapper of the node-rdkafka package Consumer Ctor.
 *
 * @param {Object} opts Consumer general options.
 * @param {Object} topts Topic specific options.
 * @see https://github.com/edenhill/librdkafka/blob/2213fb29f98a7a73f22da21ef85e0783f6fd67c4/CONFIGURATION.md
 * @return {Promise(kafka.Consumer)} A Promise with the consumer.
 */
Consumer.prototype.getConsumer = Promise.method(function (opts, topts) {
  if (!opts['metadata.broker.list']) {
    opts['metadata.broker.list'] = this.kafkaBrokerUrl;
  }

  log.info('getConsumer() :: Starting Consumer with opts:', opts);

  var consumer = new kafka.KafkaConsumer(opts, topts);

  this._consumers.push(consumer);

  consumer.on('disconnect', function(arg) {
    log.warn('getConsumer() :: Consumer disconnected. Args:', arg);
  });

  consumer.on('error', function(err) {
    log.error('getConsumer() :: Consumer Error event fired:', err);
  });

  // hack node-rdkafka
  consumer.__kafkaAvro_on = consumer.on;
  consumer.on = this._onWrapper.bind(this, consumer);

  return consumer;
});

/**
 * The wrapper of the node-rdkafka package KafkaConsumerStream.
 *
 * @param {Object} opts Consumer general options.
 * @param {Object} topts Topic specific options.
 * @param {Object} sopts node-rdkafka ConsumerStream options
 * @see https://github.com/edenhill/librdkafka/blob/2213fb29f98a7a73f22da21ef85e0783f6fd67c4/CONFIGURATION.md
 * @see https://blizzard.github.io/node-rdkafka/current/KafkaConsumerStream.html
 * @return {Promise(kafka.ConsumerStream)} A Promise with the consumer stream.
 */
Consumer.prototype.getConsumerStream = Promise.method(function (opts, topts, sopts) {
  if (!opts['metadata.broker.list']) {
    opts['metadata.broker.list'] = this.kafkaBrokerUrl;
  }

  log.info('getConsumerStream() :: Starting Consumer Stream with opts:', opts);

  var consumer = new kafka.KafkaConsumer.createReadStream(opts, topts, sopts);

  this._consumersStream.push(consumer);

  consumer.on('disconnect', function(arg) {
    log.warn('getConsumerStream() :: Consumer disconnected. Args:', arg);
  });

  consumer.on('error', function(err) {
    log.error('getConsumerStream() :: Consumer Error event fired:', err);
  });

  // hack node-rdkafka

  consumer.__kafkaAvro_on = consumer.on;
  consumer.on = this._onWrapper.bind(this, consumer);

  return consumer;
});

/**
 * The node-rdkafka on method wrapper, will intercept "data" events and
 * deserialize the incoming message using the existing schemas.
 *
 * @param {kafka.KafkaConsumer} consumerInstance node-rdkafka instance.
 * @param {string} eventName the name to listen for events on.
 * @param {Function} cb Event callback.
 * @private
 */
Consumer.prototype._onWrapper = function (consumerInstance, eventName, cb) {
  if (eventName !== 'data') {
    return consumerInstance.__kafkaAvro_on(eventName, cb);
  }

  return consumerInstance.__kafkaAvro_on('data', function(message) {
    if (!this.sr.keySchemas[message.topic]) {
      log.warn('_onWrapper() :: Warning, consumer did not find topic on SR for key schema:',
        message.topic);

      message.parsedKey = JSON.parse(message.key.toString('utf-8'));
    }

    if (!this.sr.valueSchemas[message.topic]) {
      log.warn('_onWrapper() :: Warning, consumer did not find topic on SR for value schema:',
        message.topic);

      message.parsedValue = JSON.parse(message.value.toString('utf-8'));

      cb(message);
      return;
    }

    var typeValue = this.sr.valueSchemas[message.topic];
    var decodedValue = this.deserialize(typeValue, message);

    if (!decodedValue) {
      message.parsedValue = null;
      message.schemaIdValue = null;
    } else {
      message.parsedValue = decodedValue.value;
      message.schemaIdValue = decodedValue.schemaId;
    }

    var typeKey = this.sr.keySchemas[message.topic];
    var decodedKey = this.deserialize(typeKey, message, true);

    if (!decodedKey) {
      message.parsedKey = null;
      message.schemaIdKey = null;
    } else {
      message.parsedKey = decodedKey.value;
      message.schemaIdKey = decodedKey.schemaId;
    }

    cb(message);
  }.bind(this));
};

/**
 * Deserialize an avro message.
 *
 * @param {avsc.Type} type Avro type instance.
 * @param {Object} message The raw message.
 * @return {Object} The deserialized object.
 */
Consumer.prototype.deserialize = function (type, message, isKey) {
  try {
    var parsed = magicByte.fromMessageBuffer(
      type,
      !isKey ? message.value : message.key,
      this.sr
    );
  } catch(ex) {
    log.warn(`deserialize() :: Error deserializing on topic ${ message.topic }`,
      'Raw value:', message.value, `Partition: ${ message.partition } Offset:`,
      `${ message.offset } Key: ${ message.key } Exception:`, ex);
    return null;
  }

  return parsed;
};

/**
 * Callback for Stream Transform, will deserialize the message properly.
 *
 * @param {*} data The message to decode.
 * @param {string=} encoding Encoding of the message.
 * @param {Function} callback Callback to call when done.
 * @private
 */
Consumer.prototype._transformAvro = function (data, encoding, callback) {
  const topicName = data.topic;

  if (!this.sr.keySchemas[topicName]) {
    log.warn('_transformAvro() :: Warning, consumer did not find topic on SR for key schema:',
      topicName);

    try {
      data.parsedKey = JSON.parse(data.key.toString('utf-8'));
    } catch(ex) {
      log.warn('_transformAvro() :: Error parsing key:', data.key,
        'Error:', ex);
    }
  }

  if (!this.sr.valueSchemas[topicName]) {
    log.warn('_transformAvro() :: Warning, consumer did not find topic on SR for value schema:',
      topicName);

    try {
      data.parsedValue = JSON.parse(data.value.toString('utf-8'));
    } catch(ex) {
      log.warn('_transformAvro() :: Error parsing value:', data.value,
        'Error:', ex);
    }

    callback(null, data);
    return;
  }

  var typeValue = this.sr.valueSchemas[topicName];

  var decodedValue = this.deserialize(typeValue, data);

  if (!decodedValue) {
    data.parsedValue = null;
    data.schemaIdValue = null;
  } else {
    data.parsedValue = decodedValue.value;
    data.schemaIdValue = decodedValue.schemaId;
  }

  var typeKey = this.sr.keySchemas[topicName];

  var decodedKey = this.deserialize(typeKey, data, true);

  if (!decodedKey) {
    data.parsedKey = null;
    data.schemaIdKey = null;
  } else {
    data.parsedKey = decodedKey.value;
    data.schemaIdKey = decodedKey.schemaId;
  }

  callback(null, data);
};
