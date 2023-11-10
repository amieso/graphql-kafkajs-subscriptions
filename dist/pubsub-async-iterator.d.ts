import { $$asyncIterator } from "iterall";
import { PubSubEngine } from "graphql-subscriptions";
export declare class PubSubAsyncIterator<T> implements AsyncIterator<T> {
    constructor(pubsub: PubSubEngine, eventNames: string | string[], options?: Object);
    next(): Promise<IteratorResult<any, any>>;
    return(): Promise<IteratorResult<T, any>>;
    throw(error?: any): Promise<IteratorResult<T, any>>;
    [$$asyncIterator](): this;
    private pullQueue;
    private pushQueue;
    private eventsArray;
    private subscriptionIds;
    private listening;
    private pubsub;
    private options;
    private pushValue;
    private pullValue;
    private emptyQueue;
    private subscribeAll;
    private unsubscribeAll;
}
