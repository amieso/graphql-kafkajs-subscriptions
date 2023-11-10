/// <reference types="node" />
import { PubSubEngine } from "graphql-subscriptions";
import { Kafka, ProducerConfig, IHeaders, KafkaMessage, ConsumerConfig } from "kafkajs";
interface KafkaPubSubInput {
    kafka: Kafka;
    topic: string;
    groupIdPrefix: string;
    producerConfig?: ProducerConfig;
    consumerConfig?: Omit<ConsumerConfig, "groupId">;
}
export declare type MessageHandler = (msg: KafkaMessage) => any;
export declare class KafkaPubSub implements PubSubEngine {
    private client;
    private subscriptionMap;
    private lastId;
    private channelSubscriptions;
    private producer;
    private consumer;
    private topic;
    static create({ kafka, topic, groupIdPrefix, producerConfig, consumerConfig, }: KafkaPubSubInput): Promise<KafkaPubSub>;
    private constructor();
    publish(channel: string, payload: string | Buffer, headers?: IHeaders, sendOptions?: object, key?: string | Buffer): Promise<void>;
    subscribe(channel: string, onMessage: MessageHandler, _?: any): Promise<number>;
    unsubscribe(index: number): void;
    asyncIterator<T>(triggers: string | string[]): AsyncIterator<T>;
    private onMessage;
    private connectProducer;
    private runConsumer;
}
export {};
