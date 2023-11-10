"use strict";
var __assign = (this && this.__assign) || function () {
    __assign = Object.assign || function(t) {
        for (var s, i = 1, n = arguments.length; i < n; i++) {
            s = arguments[i];
            for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p))
                t[p] = s[p];
        }
        return t;
    };
    return __assign.apply(this, arguments);
};
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g;
    return g = { next: verb(0), "throw": verb(1), "return": verb(2) }, typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (_) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.KafkaPubSub = void 0;
var pubsub_async_iterator_1 = require("./pubsub-async-iterator");
var KafkaPubSub = (function () {
    function KafkaPubSub(_a) {
        var kafka = _a.kafka, topic = _a.topic, groupIdPrefix = _a.groupIdPrefix, producerConfig = _a.producerConfig, consumerConfig = _a.consumerConfig;
        this.lastId = 0;
        this.client = kafka;
        this.subscriptionMap = new Map();
        this.channelSubscriptions = new Map();
        this.topic = topic;
        this.producer = this.client.producer(producerConfig);
        this.consumer = this.client.consumer(__assign(__assign({}, consumerConfig), { groupId: "".concat(groupIdPrefix, "-").concat(Math.ceil(Math.random() * 9999)) }));
    }
    KafkaPubSub.create = function (_a) {
        var kafka = _a.kafka, topic = _a.topic, groupIdPrefix = _a.groupIdPrefix, _b = _a.producerConfig, producerConfig = _b === void 0 ? {} : _b, _c = _a.consumerConfig, consumerConfig = _c === void 0 ? {} : _c;
        return __awaiter(this, void 0, void 0, function () {
            var pubsub;
            return __generator(this, function (_d) {
                switch (_d.label) {
                    case 0:
                        pubsub = new KafkaPubSub({
                            kafka: kafka,
                            topic: topic,
                            groupIdPrefix: groupIdPrefix,
                            producerConfig: producerConfig,
                            consumerConfig: consumerConfig,
                        });
                        return [4, pubsub.connectProducer()];
                    case 1:
                        _d.sent();
                        return [4, pubsub.runConsumer(pubsub.topic)];
                    case 2:
                        _d.sent();
                        return [2, pubsub];
                }
            });
        });
    };
    KafkaPubSub.prototype.publish = function (channel, payload, headers, sendOptions, key) {
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4, this.producer.send(__assign({ messages: [
                                {
                                    value: payload,
                                    key: key,
                                    headers: __assign(__assign({}, headers), { channel: channel }),
                                },
                            ], topic: this.topic }, sendOptions))];
                    case 1:
                        _a.sent();
                        return [2];
                }
            });
        });
    };
    KafkaPubSub.prototype.subscribe = function (channel, onMessage, _) {
        var _a;
        return __awaiter(this, void 0, void 0, function () {
            var subscriptions;
            return __generator(this, function (_b) {
                this.lastId = this.lastId + 1;
                this.subscriptionMap.set(this.lastId, [channel, onMessage]);
                subscriptions = (_a = this.channelSubscriptions.get(channel)) !== null && _a !== void 0 ? _a : new Set();
                subscriptions.add(this.lastId);
                this.channelSubscriptions.set(channel, subscriptions);
                return [2, this.lastId];
            });
        });
    };
    KafkaPubSub.prototype.unsubscribe = function (index) {
        var subscription = this.subscriptionMap.get(index);
        if (!subscription) {
            return;
        }
        var channel = subscription[0];
        var subscriptions = this.channelSubscriptions.get(channel);
        subscriptions === null || subscriptions === void 0 ? void 0 : subscriptions.delete(index);
        if ((subscriptions === null || subscriptions === void 0 ? void 0 : subscriptions.size) === 0) {
            this.channelSubscriptions.delete(channel);
        }
        this.subscriptionMap.delete(index);
    };
    KafkaPubSub.prototype.asyncIterator = function (triggers) {
        return new pubsub_async_iterator_1.PubSubAsyncIterator(this, triggers);
    };
    KafkaPubSub.prototype.onMessage = function (channel, message) {
        var _this = this;
        var subscriptions = this.channelSubscriptions.get(channel);
        if (!subscriptions) {
            return;
        }
        subscriptions.forEach(function (subId) {
            var subscription = _this.subscriptionMap.get(subId);
            if (subscription) {
                var _ = subscription[0], listener = subscription[1];
                listener(message);
            }
        });
    };
    KafkaPubSub.prototype.connectProducer = function () {
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4, this.producer.connect()];
                    case 1:
                        _a.sent();
                        return [2];
                }
            });
        });
    };
    KafkaPubSub.prototype.runConsumer = function (topic) {
        return __awaiter(this, void 0, void 0, function () {
            var _this = this;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4, this.consumer.connect()];
                    case 1:
                        _a.sent();
                        return [4, this.consumer.subscribe({ topic: topic })];
                    case 2:
                        _a.sent();
                        return [4, this.consumer.run({
                                eachMessage: function (_a) {
                                    var message = _a.message;
                                    return __awaiter(_this, void 0, void 0, function () {
                                        var _b;
                                        return __generator(this, function (_c) {
                                            if ((_b = message.headers) === null || _b === void 0 ? void 0 : _b.channel) {
                                                this.onMessage(message.headers.channel, message);
                                            }
                                            else {
                                                this.onMessage(topic, message);
                                            }
                                            return [2];
                                        });
                                    });
                                },
                            })];
                    case 3:
                        _a.sent();
                        return [2];
                }
            });
        });
    };
    return KafkaPubSub;
}());
exports.KafkaPubSub = KafkaPubSub;
//# sourceMappingURL=kafka-pubsub.js.map