module.exports = function (RED) {
    const { SchemaRegistry, SchemaType } = require('@kafkajs/confluent-schema-registry');
    const { CompressionTypes, CompressionCodecs } = require('kafkajs');
    const { getNameTypes, getMsgValues } = require('./utils');

    // Register compression codecs
    // KafkaJS expects factory functions that return codec instances
    try {
        const SnappyCodec = require('kafkajs-snappy');
        CompressionCodecs[CompressionTypes.Snappy] = SnappyCodec;
    } catch (error) {
        // Snappy codec not available
    }

    try {
        const LZ4Codec = require('kafkajs-lz4');
        CompressionCodecs[CompressionTypes.LZ4] = () => new LZ4Codec();
    } catch (error) {
        // LZ4 codec not available
    }

    function getIotOptions(config) {
        var options = new Object();
        if (config.useiot) {
            options = new Object();
            options.model = config.model;
            options.device = config.device;
            options.iotType = config.iotType;
            options.fields = config.fields;
        }
        return options;
    }

    function KafkaProducerNode(config) {
        RED.nodes.createNode(this, config);
        var node = this;
        node.ready = false;
        node.schemaRegistry = null;
        node.cachedSchemaId = null;
        node.cachedSchemaVersion = null;
        node.lastMessageTime = null;
        node.messageCount = 0;
        // Protobuf support
        node.protobufType = null;
        node.protobufRoot = null;

        let iotOptions = {};

        node.init = function () {
            const serializationType = config.serializationType || 'avro';
            const nodeType = config.useSchemaValidation
                ? (serializationType === 'protobuf' ? 'Protobuf Producer' : 'Schema Producer')
                : 'Producer';
            const versionInfo = config.useSchemaValidation && config.schemaVersion && config.schemaVersion.trim() !== ''
                ? ` (schema version: ${config.schemaVersion.trim()})` : '';
            node.debug(`[Kafka ${nodeType}] Initializing for topic: ${config.topic}${versionInfo}`);
            node.status({ fill: "yellow", shape: "ring", text: "Initializing..." });

            let broker = RED.nodes.getNode(config.broker);
            if (!broker) {
                node.error(`[Kafka ${nodeType}] No broker configuration found`);
                node.status({ fill: "red", shape: "ring", text: "No broker config" });
                return;
            }

            if (broker && broker.getIotConfig) {
                const brokerIotConfig = broker.getIotConfig();
                if (brokerIotConfig.useiot) {
                    iotOptions = brokerIotConfig;
                    node.debug(`[Kafka Producer] Using IoT configuration from broker`);
                } else {
                    iotOptions = getIotOptions(config);
                    node.debug(`[Kafka Producer] Using IoT configuration from producer`);
                }
            } else {
                iotOptions = getIotOptions(config);
                node.debug(`[Kafka Producer] Using producer IoT configuration`);
            }

            if (config.useSchemaValidation) {
                if (serializationType === 'protobuf') {
                    node.status({ fill: "yellow", shape: "ring", text: "Loading Protobuf schema..." });
                    try {
                        const protobuf = require('protobufjs');
                        if (!config.protobufSchema || config.protobufSchema.trim() === '') {
                            throw new Error('Protobuf schema definition is required');
                        }
                        const messageName = config.protobufMessageName || 'Message';
                        const root = protobuf.parse(config.protobufSchema, { keepCase: true }).root;
                        node.protobufType = root.lookupType(messageName);
                        node.protobufRoot = root;
                        node.debug(`[Kafka Protobuf Producer] Loaded type "${messageName}"`);
                        node.status({ fill: "yellow", shape: "ring", text: "Protobuf schema loaded" });
                    } catch (error) {
                        node.error(`[Kafka Protobuf Producer] Failed to load Protobuf schema: ${error.message}`);
                        node.status({ fill: "red", shape: "ring", text: `Proto error: ${error.message.substring(0, 15)}...` });
                        return;
                    }
                } else {
                    node.status({ fill: "yellow", shape: "ring", text: "Connecting to Schema Registry..." });
                    try {
                        const registryConfig = {
                            host: config.registryUrl,
                            clientId: 'node-red-schema-producer',
                            retry: { retries: 3, factor: 2, multiplier: 1000, maxRetryTimeInSecs: 60 }
                        };
                        if (config.useRegistryAuth && config.registryUsername && config.registryPassword) {
                            registryConfig.auth = {
                                username: config.registryUsername,
                                password: config.registryPassword,
                            };
                            node.debug(`[Kafka Schema Producer] Registry auth for user: ${config.registryUsername}`);
                        }
                        node.schemaRegistry = new SchemaRegistry(registryConfig);
                        node.debug(`[Kafka Schema Producer] Schema Registry client created for: ${config.registryUrl}`);
                        node.status({ fill: "yellow", shape: "ring", text: "Registry connected" });
                    } catch (error) {
                        node.error(`[Kafka Schema Producer] Failed to create Schema Registry client: ${error.message}`);
                        node.status({ fill: "red", shape: "ring", text: `Registry failed: ${error.message.substring(0, 10)}...` });
                        return;
                    }
                }
            } else {
                node.debug(`[Kafka Producer] Schema validation disabled`);
            }

            node.status({ fill: "yellow", shape: "ring", text: "Connecting to Kafka..." });
            try {
                const kafka = broker.getKafka();
                const producer = kafka.producer({
                    maxInFlightRequests: 1,
                    idempotent: config.requireAcks === 1,
                    requestTimeout: config.ackTimeoutMs || 100
                });

                producer.connect().then(() => {
                    node.debug(`[Kafka Schema Producer] Producer connected`);
                    node.ready = true;
                    node.lastMessageTime = new Date().getTime();
                    node.messageCount = 0;
                    node.status({ fill: "green", shape: "ring", text: "Ready" });

                    producer.on('producer.connect', () => {
                        node.debug(`[Kafka Schema Producer] Producer connected to Kafka`);
                        node.status({ fill: "green", shape: "ring", text: "Connected" });
                    });
                    producer.on('producer.disconnect', () => {
                        node.debug(`[Kafka Schema Producer] Producer disconnected from Kafka`);
                        node.status({ fill: "red", shape: "ring", text: "Disconnected" });
                        node.ready = false;
                        node.lastMessageTime = null;
                        node.messageCount = 0;
                    });

                    node.producer = producer;
                }).catch(error => {
                    node.error(`[Kafka Schema Producer] Failed to connect: ${error.message}`, error);
                    node.status({ fill: "red", shape: "ring", text: `Connect failed: ${error.message.substring(0, 15)}...` });
                    node.ready = false;
                    node.lastMessageTime = null;
                });
            } catch (error) {
                node.error(`[Kafka Schema Producer] Failed to get Kafka: ${error.message}`, error);
                node.status({ fill: "red", shape: "ring", text: `Kafka failed: ${error.message.substring(0, 10)}...` });
                node.ready = false;
                node.lastMessageTime = null;
            }
        };

        node.getOrRegisterSchema = async function () {
            try {
                const version = config.schemaVersion && config.schemaVersion.trim() !== '' ? config.schemaVersion.trim() : 'latest';
                if (node.cachedSchemaId && node.cachedSchemaVersion === version) {
                    node.debug(`[Kafka Schema Producer] Using cached schema ID: ${node.cachedSchemaId}`);
                    node.status({ fill: "blue", shape: "ring", text: `Using cached schema v${version}` });
                    return node.cachedSchemaId;
                }

                node.debug(`[Kafka Schema Producer] Fetching schema for version: ${version}`);
                node.status({ fill: "blue", shape: "ring", text: "Getting schema..." });

                try {
                    let schemaId;
                    if (version === 'latest') {
                        if (config.autoRegister && config.autoSchema) {
                            let schemaObject;
                            try {
                                schemaObject = JSON.parse(config.autoSchema);
                            } catch (parseError) {
                                throw new Error(`Invalid schema JSON: ${parseError.message}`);
                            }
                            const registeredSchema = await node.schemaRegistry.register({
                                type: SchemaType.AVRO,
                                schema: JSON.stringify(schemaObject)
                            }, { subject: config.schemaSubject });
                            schemaId = registeredSchema.id;
                            node.debug(`[Kafka Schema Producer] Schema auto-registered ID: ${schemaId}`);
                        } else {
                            schemaId = await node.schemaRegistry.getLatestSchemaId(config.schemaSubject);
                            node.debug(`[Kafka Schema Producer] Latest schema ID: ${schemaId}`);
                        }
                    } else {
                        schemaId = await node.schemaRegistry.getSchemaId(config.schemaSubject, parseInt(version));
                        node.debug(`[Kafka Schema Producer] Schema ID for v${version}: ${schemaId}`);
                    }
                    node.cachedSchemaId = schemaId;
                    node.cachedSchemaVersion = version;
                    node.status({ fill: "blue", shape: "ring", text: `Schema v${version} cached` });
                    return schemaId;
                } catch (fetchError) {
                    node.error(`[Kafka Schema Producer] Failed to get/register schema: ${fetchError.message}`);
                    throw fetchError;
                }
            } catch (error) {
                node.error(`[Kafka Schema Producer] Schema operation failed: ${error.message}`);
                throw error;
            }
        };

        node.encodeProtobuf = function (messageData) {
            if (!node.protobufType) {
                throw new Error('Protobuf type not loaded. Check your .proto schema definition.');
            }
            const errMsg = node.protobufType.verify(messageData);
            if (errMsg) {
                throw new Error(`Protobuf validation error: ${errMsg}`);
            }
            const protoMessage = node.protobufType.create(messageData);
            return Buffer.from(node.protobufType.encode(protoMessage).finish());
        };

        node.on('input', async function (msg) {
            const serializationType = config.serializationType || 'avro';
            const nodeType = config.useSchemaValidation
                ? (serializationType === 'protobuf' ? 'Protobuf Producer' : 'Schema Producer')
                : 'Producer';
            node.debug(`[Kafka ${nodeType}] Received input message`);

            if (!node.ready) {
                node.warn(`[Kafka ${nodeType}] Producer not ready, skipping message`);
                return;
            }

            try {
                let messageData = msg.payload;

                if (typeof messageData === 'string') {
                    try { messageData = JSON.parse(messageData); } catch (e) { /* keep as string */ }
                }

                if (iotOptions && iotOptions.useiot) {
                    if (typeof messageData === 'string') {
                        try {
                            messageData = JSON.parse(messageData);
                        } catch (parseError) {
                            node.error(`[Kafka ${nodeType}] Failed to parse payload as JSON: ${parseError.message}`);
                            node.status({ fill: "red", shape: "ring", text: "Parse error" });
                            return;
                        }
                    }
                    const nameTypes = getNameTypes(iotOptions.fields);
                    const msgValues = getMsgValues(messageData, iotOptions.fields);
                    messageData = {
                        mc: iotOptions.model,
                        dc: iotOptions.device,
                        type: iotOptions.iotType,
                        nameTypes: nameTypes,
                        ts: [new Date().getTime()],
                        values: [msgValues]
                    };
                    node.debug(`[Kafka Producer] IoT formatted message:`, messageData);
                }

                let serializedValue;

                if (config.useSchemaValidation) {
                    if (serializationType === 'protobuf') {
                        node.debug(`[Kafka Protobuf Producer] Encoding with Protobuf`);
                        node.status({ fill: "blue", shape: "dot", text: "Encoding (protobuf)" });

                        if (config.validateOnly) {
                            const errMsg = node.protobufType ? node.protobufType.verify(messageData) : 'Protobuf type not loaded';
                            if (errMsg) {
                                node.warn(`[Kafka Protobuf Producer] Validation failed: ${errMsg}`);
                                node.status({ fill: "yellow", shape: "ring", text: "Validation failed" });
                                msg.payload = { validated: false, error: errMsg };
                                node.send(msg);
                                return;
                            }
                            node.status({ fill: "green", shape: "ring", text: "Validated (not sent)" });
                            msg.payload = { validated: true };
                            node.send(msg);
                            return;
                        }

                        serializedValue = node.encodeProtobuf(messageData);
                        node.debug(`[Kafka Protobuf Producer] Encoded to ${serializedValue.length} bytes`);
                    } else {
                        node.debug(`[Kafka Schema Producer] Encoding with Avro`);
                        node.status({ fill: "blue", shape: "dot", text: "Encoding message" });

                        const schemaId = await node.getOrRegisterSchema();

                        if (config.validateOnly) {
                            try {
                                await node.schemaRegistry.encode(schemaId, messageData);
                                node.status({ fill: "green", shape: "ring", text: "Validated (not sent)" });
                                msg.payload = { validated: true, schemaId };
                                node.send(msg);
                            } catch (validationError) {
                                node.warn(`[Kafka Schema Producer] Validation failed: ${validationError.message}`);
                                node.status({ fill: "yellow", shape: "ring", text: "Validation failed" });
                                msg.payload = { validated: false, error: validationError.message };
                                node.send(msg);
                            }
                            return;
                        }

                        serializedValue = await node.schemaRegistry.encode(schemaId, messageData);
                        node.debug(`[Kafka Schema Producer] Encoded with schema ID: ${schemaId}`);
                    }
                } else {
                    serializedValue = typeof messageData === 'string' ? messageData : JSON.stringify(messageData);
                }

                const topic = msg.topic || config.topic;
                const kafkaMessage = { value: serializedValue };

                if (msg.key !== undefined) {
                    kafkaMessage.key = typeof msg.key === 'string' ? msg.key : JSON.stringify(msg.key);
                } else if (config.messageKey && config.messageKey.trim() !== '') {
                    kafkaMessage.key = config.messageKey.trim();
                }

                if (msg.headers && typeof msg.headers === 'object') {
                    kafkaMessage.headers = msg.headers;
                }

                node.debug(`[Kafka ${nodeType}] Sending message to topic: ${topic}`);
                node.status({ fill: "blue", shape: "dot", text: "Sending..." });

                const compressionType = config.compressionType !== undefined ? Number(config.compressionType) : CompressionTypes.None;

                await node.producer.send({
                    topic: topic,
                    compression: compressionType,
                    messages: [kafkaMessage]
                });

                node.messageCount++;
                node.lastMessageTime = new Date().getTime();

                const serTag = config.useSchemaValidation ? (serializationType === 'protobuf' ? ' [protobuf]' : ' [avro]') : '';
                node.status({ fill: "green", shape: "dot", text: `Sent${serTag} #${node.messageCount}` });
                node.debug(`[Kafka ${nodeType}] Sent OK. Count: ${node.messageCount}`);

                msg.payload = {
                    topic,
                    messageCount: node.messageCount,
                    timestamp: node.lastMessageTime,
                    serialization: config.useSchemaValidation ? (serializationType === 'protobuf' ? 'protobuf' : 'avro') : 'none'
                };
                node.send(msg);

            } catch (error) {
                const nt = config.useSchemaValidation
                    ? ((config.serializationType || 'avro') === 'protobuf' ? 'Protobuf Producer' : 'Schema Producer')
                    : 'Producer';
                node.error(`[Kafka ${nt}] Error: ${error.message}`, error);
                node.status({ fill: "red", shape: "ring", text: `Error: ${error.message.substring(0, 15)}...` });
                node.lastMessageTime = null;
            }
        });

        node.checkLastMessageTime = function () {
            if (node.lastMessageTime) {
                const timeSince = new Date().getTime() - node.lastMessageTime;
                const minutes = Math.floor(timeSince / 60000);
                const seconds = Math.floor((timeSince % 60000) / 1000);
                const serializationType = config.serializationType || 'avro';
                const serTag = config.useSchemaValidation ? ` [${serializationType}]` : '';
                node.status({ fill: "green", shape: "ring", text: `Ready${serTag} | Last: ${minutes}m ${seconds}s | Count: ${node.messageCount}` });
            }
        };

        node.on('close', function (done) {
            node.debug(`[Kafka Producer] Node closing`);
            node.ready = false;
            node.protobufType = null;
            node.protobufRoot = null;
            if (node.producer) {
                node.producer.disconnect().then(() => {
                    node.debug(`[Kafka Producer] Producer disconnected`);
                    done();
                }).catch(error => {
                    node.error(`[Kafka Producer] Error disconnecting: ${error.message}`);
                    done();
                });
            } else {
                done();
            }
        });

        node.init();
    }

    RED.nodes.registerType('oriolrius-kafka-producer', KafkaProducerNode);
};
