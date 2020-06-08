import * as firestore from '@google-cloud/firestore';
import * as functions from 'firebase-functions';

interface TemporalSlot {
    bounds: {
        readonly start: number;
        readonly end: number;
    };

    WALK: number;
    WHEELS: number;
    TRANSIT: number;
    CAR: number;

    [prop: string]: any;
}

interface Event {
    readonly type: string,
    readonly timestamp: number
}

function CreateTemporalSlot(start: number, end: number): TemporalSlot {
    return {
        bounds: {
            start: start,
            end: end
        },

        WALK: 0,
        WHEELS: 0,
        TRANSIT: 0,
        CAR: 0
    };
}

// Creates a firestore function to handle a temporal aggregation. A temporal aggregation consists of a
// number of {size} buckets, where buckets represent consecutive intervals of {timespan} minutes. This
// function will fire when any event is added to the designated stream and its response will be added
// to the current bucket. These buckets are essentially a circular buffer.

// Firestore function for updating the counter for the latest aggregation slot when an event is added
// to the provided stream.
function EventAdded(db: firestore.Firestore, aggregationId: string, streamId: string, timespan: number, size: number) {
    return functions.firestore
        .document(`streams/${streamId}/events/{eventId}`)
        .onCreate((event, context) => {
            const timespanSecs = timespan * 60;
            const type: string = event.get('type');
            const aggregationRef = db.doc(`aggregations/${aggregationId}`)

            return db.runTransaction(transaction => {
                return transaction.get(aggregationRef).then(aggregate => {
                    if (!aggregate.exists)
                        throw Error('Document does not exist.');

                    const slots: TemporalSlot[] = aggregate.get('slots');
                    const overflow: any[] = aggregate.get('overflow');

                    // The event will always exist at this point.
                    const now = event.createTime!.seconds;

                    if (slots.length === 0) {
                        slots.push(CreateTemporalSlot(now, now + timespanSecs - 1));
                    }

                    const lastBucket = slots[slots.length - 1];
                    if (now > lastBucket.bounds.end) {
                        overflow.push({
                            type: type,
                            timestamp: event.get('timestamp')
                        });
                    } else {
                        const slot = slots[slots.length - 1];
                        slot[type]++;
                    }

                    transaction.update(aggregationRef, {
                        slots: slots,
                        overflow: overflow
                    });
                });
            });
        });
}

// To keep the temporal aggregation slots up to date, this function will advance the slots on a set interval.
function SlotPassed(db: firestore.Firestore, aggregationId: string, timespan: number, size: number) {
    return functions.pubsub
        .schedule(`every ${timespan} minutes`)
        .onRun(context => {
            const timespanSecs = timespan * 60;
            const aggregationRef = db.doc(`aggregations/${aggregationId}`)

            return db.runTransaction(transaction => {
                return transaction.get(aggregationRef).then(aggregate => {
                    if (!aggregate.exists)
                        throw Error('Document does not exist.');

                    const slots: TemporalSlot[] = aggregate.get('slots');
                    const overflow: Event[] = aggregate.get('overflow');

                    const now = Math.floor(Date.parse(context.timestamp) / 1000);
                    let start: number;
                    let end: number;

                    if (slots.length === 0) {
                        // Although this function will not run before its period has already passed, which
                        // means that no slots can assumed to be an empty first slot and another empty slot
                        // in the current time window, it is easier to assume a new window starting now. It
                        // is also a minor edge case when starting the aggregation for the first time. This
                        // should be revisited and followed however, to see if this simplifying assumption does
                        // not cause issues later.
                        start = now;
                        end = start + (timespanSecs - 1);
                    } else {
                        const lastSlot = slots[slots.length - 1];
                        start = lastSlot.bounds.end + 1;
                        end = start + timespanSecs - 1;
                    }

                    const nextSlot = CreateTemporalSlot(start, end);
                    slots.push(nextSlot);

                    if (slots.length > size) {
                        const diff = slots.length - size;
                        slots.splice(0, diff);
                    }

                    for (const event of overflow) {
                        nextSlot[event.type]++;
                    }

                    transaction.update(aggregationRef, {
                        slots: slots,
                        overflow: []
                    });
                });
            });
        });
}

// A temporal aggregation aggregates over a timespan a specific number of times. It consists of an id for the associated
// entry in firestore, the id of the associated stream to aggregate over, the width fo the slots, and the number of
// slots. The defined aggregation can be used by exporting it from this module using the export member function. This
// creates an exportable object which adds the necessary set of firestore functions for creating the defined aggregation.
class TemporalAggregation {
    readonly id: string;
    readonly streamId: string;
    readonly timespan: number;
    readonly size: number;
    readonly db: firestore.Firestore;

    constructor(db: firestore.Firestore, id: string, streamId: string, timespan: number, size: number) {
        this.db = db;
        this.id = id;
        this.streamId = streamId;
        this.timespan = timespan;
        this.size = size;
    }

    // Create the exportable set of firestore functions that define this aggregation for firestore.
    Export() {
        const e: any = {
            EventAdded: EventAdded(this.db, this.id, this.streamId, this.timespan, this.size),
            SlotPassed: SlotPassed(this.db, this.id, this.timespan, this.size)
        };

        return e;
    }
}

export { TemporalAggregation };