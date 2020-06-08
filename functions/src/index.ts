import * as firestore from '@google-cloud/firestore';
import { TemporalAggregation } from './aggregation';
import { PercentageView } from './view';

const db = new firestore.Firestore({
    projectId: 'surveybox-fe69c',
    timestampsInSnapshots: true
});

const bulldogMonthAggregation = new TemporalAggregation(db, 'GsuDbAbxKhd39b1A2esv', 'Kwi4xoIpmsmbcTQDjTr7', 86400, 28);
const bulldogPercentageView = PercentageView(db, 'nBCvmKEQ6jwhniZLQeAU', 'GsuDbAbxKhd39b1A2esv', 60);

export const bulldogAggregation = bulldogMonthAggregation.Export();
export const bulldogView = bulldogPercentageView.Export();
