import { OnChanges, TemplateRef } from '@angular/core';
import { NzStatisticValueType } from './nz-statistic-definitions';
export declare class NzStatisticNumberComponent implements OnChanges {
    private locale_id;
    nzValue: NzStatisticValueType;
    nzValueTemplate: TemplateRef<{
        $implicit: NzStatisticValueType;
    }>;
    displayInt: string;
    displayDecimal: string;
    constructor(locale_id: string);
    ngOnChanges(): void;
    private formatNumber;
}
