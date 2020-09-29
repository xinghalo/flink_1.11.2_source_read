import { EventEmitter } from '@angular/core';
import { NzCalendarI18nInterface } from '../../../i18n/nz-i18n.interface';
import { CandyDate } from '../candy-date';
export declare class MonthPanelComponent {
    locale: NzCalendarI18nInterface;
    value: CandyDate;
    disabledDate: (date: Date) => boolean;
    readonly valueChange: EventEmitter<CandyDate>;
    readonly yearPanelShow: EventEmitter<void>;
    prefixCls: string;
    previousYear(): void;
    nextYear(): void;
    private gotoYear;
}
