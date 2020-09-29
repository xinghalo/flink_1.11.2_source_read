import { EventEmitter, OnInit } from '@angular/core';
import { DateHelperService } from '../i18n/date-helper.service';
import { NzI18nService as I18n } from '../i18n/nz-i18n.service';
export declare class NzCalendarHeaderComponent implements OnInit {
    private i18n;
    private dateHelper;
    mode: 'month' | 'year';
    readonly modeChange: EventEmitter<'month' | 'year'>;
    fullscreen: boolean;
    activeDate: Date;
    readonly yearChange: EventEmitter<number>;
    readonly monthChange: EventEmitter<number>;
    _activeDate: Date;
    yearOffset: number;
    yearTotal: number;
    years: Array<{
        label: string;
        value: number;
    }>;
    months: Array<{
        label: string;
        value: number;
    }>;
    readonly activeYear: number;
    readonly activeMonth: number;
    readonly size: string;
    readonly yearTypeText: string;
    readonly monthTypeText: string;
    constructor(i18n: I18n, dateHelper: DateHelperService);
    ngOnInit(): void;
    updateYear(year: number): void;
    private setUpYears;
    private setUpMonths;
}
