import { EventEmitter, OnInit } from '@angular/core';
import { DateHelperService } from '../../../i18n/date-helper.service';
import { NzCalendarI18nInterface } from '../../../i18n/nz-i18n.interface';
import { CandyDate } from '../candy-date';
export declare class CalendarInputComponent implements OnInit {
    private dateHelper;
    locale: NzCalendarI18nInterface;
    format: string;
    placeholder: string;
    disabledDate: (d: Date) => boolean;
    value: CandyDate;
    readonly valueChange: EventEmitter<CandyDate>;
    prefixCls: string;
    invalidInputClass: string;
    constructor(dateHelper: DateHelperService);
    ngOnInit(): void;
    onInputKeyup(event: Event): void;
    toReadableInput(value: CandyDate): string;
    private checkValidInputDate;
}
