import { EventEmitter, OnChanges, OnInit, SimpleChanges, TemplateRef } from '@angular/core';
import { FunctionProp } from '../../../core/types/common-wrap';
import { isNonEmptyString, isTemplateRef } from '../../../core/util/check';
import { DateHelperService } from '../../../i18n/date-helper.service';
import { NzCalendarI18nInterface } from '../../../i18n/nz-i18n.interface';
import { NzI18nService } from '../../../i18n/nz-i18n.service';
import { CandyDate } from '../candy-date';
export declare class DateTableComponent implements OnInit, OnChanges {
    private i18n;
    private dateHelper;
    locale: NzCalendarI18nInterface;
    selectedValue: CandyDate[];
    hoverValue: CandyDate[];
    value: CandyDate;
    readonly valueChange: EventEmitter<CandyDate>;
    showWeek: boolean;
    disabledDate: (d: Date) => boolean;
    dateRender: FunctionProp<TemplateRef<Date> | string>;
    readonly dayHover: EventEmitter<CandyDate>;
    prefixCls: string;
    headWeekDays: WeekDayLabel[];
    weekRows: WeekRow[];
    isTemplateRef: typeof isTemplateRef;
    isNonEmptyString: typeof isNonEmptyString;
    constructor(i18n: NzI18nService, dateHelper: DateHelperService);
    ngOnInit(): void;
    ngOnChanges(changes: SimpleChanges): void;
    private isDateRealChange;
    private isSameDate;
    private render;
    private changeValueFromInside;
    private makeHeadWeekDays;
    private getVeryShortWeekFormat;
    private makeWeekRows;
    private getDateTitle;
    private getWeekNum;
    private isBeforeMonthYear;
    private isAfterMonthYear;
}
export interface WeekDayLabel {
    short: string;
    veryShort: string;
}
export interface DateCell {
    value: CandyDate;
    title: string;
    customContent: TemplateRef<Date> | string;
    content: string;
    isSelected?: boolean;
    isToday?: boolean;
    isDisabled?: boolean;
    isSelectedStartDate?: boolean;
    isSelectedEndDate?: boolean;
    isInRange?: boolean;
    classMap?: object;
    onClick(date: CandyDate): void;
    onMouseEnter(): void;
}
export interface WeekRow {
    isCurrent?: boolean;
    isActive?: boolean;
    weekNum?: number;
    classMap?: object;
    dateCells: DateCell[];
}
