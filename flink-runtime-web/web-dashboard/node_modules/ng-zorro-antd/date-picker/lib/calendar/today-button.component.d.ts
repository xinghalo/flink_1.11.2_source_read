import { EventEmitter, OnChanges, OnInit, SimpleChanges } from '@angular/core';
import { DateHelperService } from '../../../i18n/date-helper.service';
import { NzCalendarI18nInterface } from '../../../i18n/nz-i18n.interface';
import { CandyDate } from '../candy-date';
export declare class TodayButtonComponent implements OnInit, OnChanges {
    private dateHelper;
    locale: NzCalendarI18nInterface;
    hasTimePicker: boolean;
    disabledDate: (d: Date) => boolean;
    readonly clickToday: EventEmitter<CandyDate>;
    prefixCls: string;
    isDisabled: boolean;
    title: string;
    private now;
    constructor(dateHelper: DateHelperService);
    ngOnInit(): void;
    ngOnChanges(changes: SimpleChanges): void;
    onClickToday(): void;
}
