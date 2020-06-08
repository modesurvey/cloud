import * as firestore from '@google-cloud/firestore';
import * as functions from 'firebase-functions';

function ConvertTemporalAggregationToPercentages(aggregate: firestore.DocumentSnapshot<firestore.DocumentData>) {
    const slots = aggregate.get('slots');

    const totals = [0, 0, 0, 0];

    for (const slot of slots) {
        totals[0] += slot.WALK;
        totals[1] += slot.WHEELS;
        totals[2] += slot.TRANSIT;
        totals[3] += slot.CAR;
    }

    const sum = totals[0] + totals[1] + totals[2] + totals[3];
    const update = {
        WALK: totals[0] / sum,
        WHEELS: totals[1] / sum,
        TRANSIT: totals[2] / sum,
        CAR: totals[3] / sum,
        updated: firestore.Timestamp.fromMillis(Date.now())
    };
    
    return update;
}

class View {
    readonly db: firestore.Firestore;

    readonly id: string;
    readonly aggregationId: string;
    readonly frequency: number;
    readonly mapper: (doc: firestore.DocumentSnapshot<firestore.DocumentData>) => any;

    constructor(db: firestore.Firestore, id: string, aggregationId: string, frequency: number, mapper: (doc: firestore.DocumentSnapshot<firestore.DocumentData>) => any) {
        this.db = db;

        this.id = id;
        this.aggregationId = aggregationId;
        this.frequency = frequency;
        this.mapper = mapper;
    }

    Export() {
        const f = functions.pubsub
            .schedule(`every ${this.frequency} minutes`)
            .onRun(context => {
                const aggregationRef = this.db.doc(`aggregations/${this.aggregationId}`)
                const viewRef = this.db.doc(`/aggregation_views/${this.id}`);

                return this.db.runTransaction(transaction => {
                    return transaction.get(aggregationRef).then(aggregate => {
                        const result = this.mapper(aggregate);
                        transaction.update(viewRef, result);
                    });
                });
            });

        return {
            Mapper: f
        }
    }
}

function PercentageView(db: firestore.Firestore, id: string, aggregationId: string, frequency: number) {
    return new View(db, id, aggregationId, frequency, ConvertTemporalAggregationToPercentages);
}


export { View, PercentageView };