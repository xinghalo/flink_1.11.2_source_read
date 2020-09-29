import { EventEmitter, OnChanges, OnInit, SimpleChanges } from '@angular/core';
import { DateHelperService } from '../../../i18n/date-helper.service';
import { NzCalendarI18nInterface } from '../../../i18n/nz-i18n.interface';
import { PanelMode } from '../../standard-types';
import { CandyDate } from '../candy-date';
export declare class CalendarHeaderComponent implements OnInit, OnChanges {
    private dateHelper;
    locale: NzCalendarI18nInterface;
    enablePrev: boolean;
    enableNext: boolean;
    disabledMonth: (date: Date) => boolean;
    disabledYear: (date: Date) => boolean;
    showTimePicker: boolean;
    value: CandyDate;
    readonly valueChange: EventEmitter<CandyDate>;
    panelMode: PanelMode;
    readonly panelModeChange: EventEmitter<PanelMode>;
    readonly chooseDecade: EventEmitter<CandyDate>;
    readonly chooseYear: EventEmitter<CandyDate>;
    readonly chooseMonth: EventEmitter<CandyDate>;
    prefixCls: string;
    yearMonthDaySelectors: YearMonthDaySelector[];
    private yearToMonth;
    constructor(dateHelper: DateHelperService);
    ngOnInit(): void;
    ngOnChanges(changes: SimpleChanges): void;
    previousYear(): void;
    nextYear(): void;
    previousMonth(): void;
    nextMonth(): void;
    changePanel(mode: PanelMode, value?: CandyDate): void;
    onChooseDecade(value: CandyDate): void;
    onChooseYear(value: CandyDate): void;
    onChooseMonth(value: CandyDate): void;
    changeToMonthPanel(): void;
    private render;
    private gotoMonth;
    private gotoYear;
    private changeValueFromInside;
    private formatDateTime;
    private createYearMonthDaySelectors;
}
export interface YearMonthDaySelector {
    className: string;
    title?: string;
    label: string;
    onClick?(): void;
}
