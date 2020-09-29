import { TemplateRef } from '@angular/core';
import { NzStatisticValueType } from './nz-statistic-definitions';
export declare class NzStatisticComponent {
    nzPrefix: string | TemplateRef<void>;
    nzSuffix: string | TemplateRef<void>;
    nzTitle: string | TemplateRef<void>;
    nzValue: NzStatisticValueType;
    nzValueStyle: {};
    nzValueTemplate: TemplateRef<{
        $implicit: NzStatisticValueType;
    }>;
}
