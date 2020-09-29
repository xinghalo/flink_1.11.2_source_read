/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { coerceBooleanProperty } from '@angular/cdk/coercion';
import { forwardRef, ChangeDetectionStrategy, ChangeDetectorRef, Component, ContentChild, EventEmitter, HostBinding, Input, Output, TemplateRef, ViewEncapsulation } from '@angular/core';
import { NG_VALUE_ACCESSOR } from '@angular/forms';
import addDays from 'date-fns/add_days';
import differenceInCalendarDays from 'date-fns/difference_in_calendar_days';
import differenceInCalendarMonths from 'date-fns/difference_in_calendar_months';
import differenceInCalendarWeeks from 'date-fns/difference_in_calendar_weeks';
import endOfMonth from 'date-fns/end_of_month';
import isSameDay from 'date-fns/is_same_day';
import isSameMonth from 'date-fns/is_same_month';
import isSameYear from 'date-fns/is_same_year';
import isThisMonth from 'date-fns/is_this_month';
import isThisYear from 'date-fns/is_this_year';
import setMonth from 'date-fns/set_month';
import setYear from 'date-fns/set_year';
import startOfMonth from 'date-fns/start_of_month';
import startOfWeek from 'date-fns/start_of_week';
import startOfYear from 'date-fns/start_of_year';
import { DateHelperService } from '../i18n/date-helper.service';
import { NzI18nService } from '../i18n/nz-i18n.service';
import { NzDateCellDirective as DateCell, NzDateFullCellDirective as DateFullCell, NzMonthCellDirective as MonthCell, NzMonthFullCellDirective as MonthFullCell } from './nz-calendar-cells';
export class NzCalendarComponent {
    /**
     * @param {?} i18n
     * @param {?} cdr
     * @param {?} dateHelper
     */
    constructor(i18n, cdr, dateHelper) {
        this.i18n = i18n;
        this.cdr = cdr;
        this.dateHelper = dateHelper;
        this.nzMode = 'month';
        this.nzModeChange = new EventEmitter();
        this.nzPanelChange = new EventEmitter();
        this.nzSelectChange = new EventEmitter();
        this.nzValueChange = new EventEmitter();
        this.fullscreen = true;
        this.daysInWeek = [];
        this.monthsInYear = [];
        this.dateMatrix = [];
        this.activeDate = new Date();
        this.currentDateRow = -1;
        this.currentDateCol = -1;
        this.activeDateRow = -1;
        this.activeDateCol = -1;
        this.currentMonthRow = -1;
        this.currentMonthCol = -1;
        this.activeMonthRow = -1;
        this.activeMonthCol = -1;
        this.dateCell = null;
        this.dateFullCell = null;
        this.monthCell = null;
        this.monthFullCell = null;
        this.currentDate = new Date();
        this.onChangeFn = (/**
         * @return {?}
         */
        () => { });
        this.onTouchFn = (/**
         * @return {?}
         */
        () => { });
    }
    /**
     * @param {?} value
     * @return {?}
     */
    set nzValue(value) {
        this.updateDate(value, false);
    }
    /**
     * @param {?} value
     * @return {?}
     */
    set nzDateCell(value) {
        this.dateCell = value;
    }
    /**
     * @param {?} value
     * @return {?}
     */
    set nzDateFullCell(value) {
        this.dateFullCell = value;
    }
    /**
     * @param {?} value
     * @return {?}
     */
    set nzMonthCell(value) {
        this.monthCell = value;
    }
    /**
     * @param {?} value
     * @return {?}
     */
    set nzMonthFullCell(value) {
        this.monthFullCell = value;
    }
    /**
     * @param {?} value
     * @return {?}
     */
    set nzFullscreen(value) {
        this.fullscreen = coerceBooleanProperty(value);
    }
    /**
     * @return {?}
     */
    get nzFullscreen() {
        return this.fullscreen;
    }
    /**
     * @param {?} value
     * @return {?}
     */
    set nzCard(value) {
        this.fullscreen = !coerceBooleanProperty(value);
    }
    /**
     * @return {?}
     */
    get nzCard() {
        return !this.fullscreen;
    }
    /**
     * @param {?} value
     * @return {?}
     */
    set dateCellChild(value) {
        if (value) {
            this.dateCell = value;
        }
    }
    /**
     * @param {?} value
     * @return {?}
     */
    set dateFullCellChild(value) {
        if (value) {
            this.dateFullCell = value;
        }
    }
    /**
     * @param {?} value
     * @return {?}
     */
    set monthCellChild(value) {
        if (value) {
            this.monthCell = value;
        }
    }
    /**
     * @param {?} value
     * @return {?}
     */
    set monthFullCellChild(value) {
        if (value) {
            this.monthFullCell = value;
        }
    }
    /**
     * @private
     * @return {?}
     */
    get calendarStart() {
        return startOfWeek(startOfMonth(this.activeDate), { weekStartsOn: this.dateHelper.getFirstDayOfWeek() });
    }
    /**
     * @return {?}
     */
    ngOnInit() {
        this.setUpDaysInWeek();
        this.setUpMonthsInYear();
        this.setUpDateMatrix();
        this.calculateCurrentDate();
        this.calculateActiveDate();
        this.calculateCurrentMonth();
        this.calculateActiveMonth();
    }
    /**
     * @param {?} mode
     * @return {?}
     */
    onModeChange(mode) {
        this.nzModeChange.emit(mode);
        this.nzPanelChange.emit({ date: this.activeDate, mode });
    }
    /**
     * @param {?} date
     * @return {?}
     */
    onDateSelect(date) {
        this.updateDate(date);
        this.nzSelectChange.emit(date);
    }
    /**
     * @param {?} year
     * @return {?}
     */
    onYearSelect(year) {
        /** @type {?} */
        const date = setYear(this.activeDate, year);
        this.updateDate(date);
        this.nzSelectChange.emit(date);
    }
    /**
     * @param {?} month
     * @return {?}
     */
    onMonthSelect(month) {
        /** @type {?} */
        const date = setMonth(this.activeDate, month);
        this.updateDate(date);
        this.nzSelectChange.emit(date);
    }
    /**
     * @param {?} value
     * @return {?}
     */
    writeValue(value) {
        this.updateDate(value || new Date(), false);
        this.cdr.markForCheck();
    }
    /**
     * @param {?} fn
     * @return {?}
     */
    registerOnChange(fn) {
        this.onChangeFn = fn;
    }
    /**
     * @param {?} fn
     * @return {?}
     */
    registerOnTouched(fn) {
        this.onTouchFn = fn;
    }
    /**
     * @private
     * @param {?} date
     * @param {?=} touched
     * @return {?}
     */
    updateDate(date, touched = true) {
        /** @type {?} */
        const dayChanged = !isSameDay(date, this.activeDate);
        /** @type {?} */
        const monthChanged = !isSameMonth(date, this.activeDate);
        /** @type {?} */
        const yearChanged = !isSameYear(date, this.activeDate);
        this.activeDate = date;
        if (dayChanged) {
            this.calculateActiveDate();
        }
        if (monthChanged) {
            this.setUpDateMatrix();
            this.calculateCurrentDate();
            this.calculateActiveMonth();
        }
        if (yearChanged) {
            this.calculateCurrentMonth();
        }
        if (touched) {
            this.onChangeFn(date);
            this.onTouchFn();
            this.nzValueChange.emit(date);
        }
    }
    /**
     * @private
     * @return {?}
     */
    setUpDaysInWeek() {
        this.daysInWeek = [];
        /** @type {?} */
        const weekStart = startOfWeek(this.activeDate, { weekStartsOn: this.dateHelper.getFirstDayOfWeek() });
        for (let i = 0; i < 7; i++) {
            /** @type {?} */
            const date = addDays(weekStart, i);
            /** @type {?} */
            const title = this.dateHelper.format(date, this.dateHelper.relyOnDatePipe ? 'E' : 'ddd');
            /** @type {?} */
            const label = this.dateHelper.format(date, this.dateHelper.relyOnDatePipe ? 'EEEEEE' : 'dd');
            this.daysInWeek.push({ title, label });
        }
    }
    /**
     * @private
     * @return {?}
     */
    setUpMonthsInYear() {
        this.monthsInYear = [];
        for (let i = 0; i < 12; i++) {
            /** @type {?} */
            const date = setMonth(this.activeDate, i);
            /** @type {?} */
            const title = this.dateHelper.format(date, 'MMM');
            /** @type {?} */
            const label = this.dateHelper.format(date, 'MMM');
            /** @type {?} */
            const start = startOfMonth(date);
            this.monthsInYear.push({ title, label, start });
        }
    }
    /**
     * @private
     * @return {?}
     */
    setUpDateMatrix() {
        this.dateMatrix = [];
        /** @type {?} */
        const monthStart = startOfMonth(this.activeDate);
        /** @type {?} */
        const monthEnd = endOfMonth(this.activeDate);
        /** @type {?} */
        const weekDiff = differenceInCalendarWeeks(monthEnd, monthStart, { weekStartsOn: this.dateHelper.getFirstDayOfWeek() }) + 2;
        for (let week = 0; week < weekDiff; week++) {
            /** @type {?} */
            const row = [];
            /** @type {?} */
            const weekStart = addDays(this.calendarStart, week * 7);
            for (let day = 0; day < 7; day++) {
                /** @type {?} */
                const date = addDays(weekStart, day);
                /** @type {?} */
                const monthDiff = differenceInCalendarMonths(date, this.activeDate);
                /** @type {?} */
                const dateFormat = this.dateHelper.relyOnDatePipe
                    ? 'longDate'
                    : this.i18n.getLocaleData('DatePicker.lang.dateFormat', 'YYYY-MM-DD');
                /** @type {?} */
                const title = this.dateHelper.format(date, dateFormat);
                /** @type {?} */
                const label = this.dateHelper.format(date, this.dateHelper.relyOnDatePipe ? 'dd' : 'DD');
                /** @type {?} */
                const rel = monthDiff === 0 ? 'current' : monthDiff < 0 ? 'last' : 'next';
                row.push({ title, label, rel, value: date });
            }
            this.dateMatrix.push(row);
        }
    }
    /**
     * @private
     * @return {?}
     */
    calculateCurrentDate() {
        if (isThisMonth(this.activeDate)) {
            this.currentDateRow = differenceInCalendarWeeks(this.currentDate, this.calendarStart, {
                weekStartsOn: this.dateHelper.getFirstDayOfWeek()
            });
            this.currentDateCol = differenceInCalendarDays(this.currentDate, addDays(this.calendarStart, this.currentDateRow * 7));
        }
        else {
            this.currentDateRow = -1;
            this.currentDateCol = -1;
        }
    }
    /**
     * @private
     * @return {?}
     */
    calculateActiveDate() {
        this.activeDateRow = differenceInCalendarWeeks(this.activeDate, this.calendarStart, {
            weekStartsOn: this.dateHelper.getFirstDayOfWeek()
        });
        this.activeDateCol = differenceInCalendarDays(this.activeDate, addDays(this.calendarStart, this.activeDateRow * 7));
    }
    /**
     * @private
     * @return {?}
     */
    calculateCurrentMonth() {
        if (isThisYear(this.activeDate)) {
            /** @type {?} */
            const yearStart = startOfYear(this.currentDate);
            /** @type {?} */
            const monthDiff = differenceInCalendarMonths(this.currentDate, yearStart);
            this.currentMonthRow = Math.floor(monthDiff / 3);
            this.currentMonthCol = monthDiff % 3;
        }
        else {
            this.currentMonthRow = -1;
            this.currentMonthCol = -1;
        }
    }
    /**
     * @private
     * @return {?}
     */
    calculateActiveMonth() {
        this.activeMonthRow = Math.floor(this.activeDate.getMonth() / 3);
        this.activeMonthCol = this.activeDate.getMonth() % 3;
    }
}
NzCalendarComponent.decorators = [
    { type: Component, args: [{
                encapsulation: ViewEncapsulation.None,
                changeDetection: ChangeDetectionStrategy.OnPush,
                selector: 'nz-calendar',
                template: "<nz-calendar-header [fullscreen]=\"fullscreen\" [activeDate]=\"activeDate\"\n                    [(mode)]=\"nzMode\" (modeChange)=\"onModeChange($event)\"\n                    (yearChange)=\"onYearSelect($event)\" (monthChange)=\"onMonthSelect($event)\">\n</nz-calendar-header>\n\n<div class=\"ant-fullcalendar ant-fullcalendar-full\" [ngClass]=\"fullscreen ? 'ant-fullcalendar-fullscreen' : ''\">\n  <div class=\"ant-fullcalendar-calendar-body\">\n    <ng-container *ngIf=\"nzMode === 'month' then monthModeTable else yearModeTable\"></ng-container>\n  </div>\n</div>\n\n<ng-template #monthModeTable>\n  <table class=\"ant-fullcalendar-table\" cellspacing=\"0\" role=\"grid\">\n    <thead>\n      <tr role=\"row\">\n        <th *ngFor=\"let day of daysInWeek\" class=\"ant-fullcalendar-column-header\" role=\"columnheader\" [title]=\"day.title\">\n          <span class=\"ant-fullcalendar-column-header-inner\">{{ day.label }}</span>\n        </th>\n      </tr>\n    </thead>\n    <tbody class=\"ant-fullcalendar-tbody\">\n      <tr *ngFor=\"let week of dateMatrix; index as row\"\n          [class.ant-fullcalendar-current-week]=\"row === currentDateRow\"\n          [class.ant-fullcalendar-active-week]=\"row === activeDateRow\">\n        <td *ngFor=\"let day of week; index as col\" role=\"gridcell\" class=\"ant-fullcalendar-cell\" [title]=\"day.title\"\n            [class.ant-fullcalendar-today]=\"row === currentDateRow && col === currentDateCol\"\n            [class.ant-fullcalendar-selected-day]=\"row === activeDateRow && col === activeDateCol\"\n            [class.ant-fullcalendar-last-month-cell]=\"day.rel === 'last'\"\n            [class.ant-fullcalendar-next-month-btn-day]=\"day.rel === 'next'\"\n            (click)=\"onDateSelect(day.value)\">\n            <div class=\"ant-fullcalendar-date\">\n              <ng-container *ngIf=\"dateFullCell else defaultCell\">\n                <ng-container *ngTemplateOutlet=\"dateFullCell; context: {$implicit: day.value}\"></ng-container>\n              </ng-container>\n              <ng-template #defaultCell>\n                <div class=\"ant-fullcalendar-value\">{{ day.label }}</div>\n                <div *ngIf=\"dateCell\" class=\"ant-fullcalendar-content\">\n                  <ng-container *ngTemplateOutlet=\"dateCell; context: {$implicit: day.value}\"></ng-container>\n                </div>\n              </ng-template>\n            </div>\n        </td>\n      </tr>\n    </tbody>\n  </table>\n</ng-template>\n\n<ng-template #yearModeTable>\n  <table class=\"ant-fullcalendar-month-panel-table\" cellspacing=\"0\" role=\"grid\">\n    <tbody class=\"ant-fullcalendar-month-panel-tbody\">\n      <tr *ngFor=\"let row of [0, 1, 2, 3]\" role=\"row\">\n        <td *ngFor=\"let col of [0, 1, 2]\" role=\"gridcell\" [title]=\"monthsInYear[row * 3 + col].title\"\n            class=\"ant-fullcalendar-month-panel-cell\"\n            [class.ant-fullcalendar-month-panel-current-cell]=\"row === currentMonthRow && col === currentMonthCol\"\n            [class.ant-fullcalendar-month-panel-selected-cell]=\"row === activeMonthRow && col === activeMonthCol\"\n            (click)=\"onMonthSelect(row * 3 + col)\">\n          <div class=\"ant-fullcalendar-month\">\n            <ng-container *ngIf=\"monthFullCell else defaultCell\">\n              <ng-container *ngTemplateOutlet=\"monthFullCell; context: {$implicit: monthsInYear[row * 3 + col].start}\"></ng-container>\n            </ng-container>\n            <ng-template #defaultCell>\n              <div class=\"ant-fullcalendar-value\">{{ monthsInYear[row * 3 + col].label }}</div>\n              <div *ngIf=\"monthCell\" class=\"ant-fullcalendar-content\">\n                <ng-container *ngTemplateOutlet=\"monthCell; context: {$implicit: monthsInYear[row * 3 + col].start}\"></ng-container>\n              </div>\n            </ng-template>\n          </div>\n        </td>\n      </tr>\n    </tbody>\n  </table>\n</ng-template>\n",
                providers: [{ provide: NG_VALUE_ACCESSOR, useExisting: forwardRef((/**
                         * @return {?}
                         */
                        () => NzCalendarComponent)), multi: true }]
            }] }
];
/** @nocollapse */
NzCalendarComponent.ctorParameters = () => [
    { type: NzI18nService },
    { type: ChangeDetectorRef },
    { type: DateHelperService }
];
NzCalendarComponent.propDecorators = {
    nzMode: [{ type: Input }],
    nzModeChange: [{ type: Output }],
    nzPanelChange: [{ type: Output }],
    nzSelectChange: [{ type: Output }],
    nzValue: [{ type: Input }],
    nzValueChange: [{ type: Output }],
    nzDateCell: [{ type: Input }],
    nzDateFullCell: [{ type: Input }],
    nzMonthCell: [{ type: Input }],
    nzMonthFullCell: [{ type: Input }],
    nzFullscreen: [{ type: Input }],
    nzCard: [{ type: Input }],
    dateCellChild: [{ type: ContentChild, args: [DateCell, { read: TemplateRef },] }],
    dateFullCellChild: [{ type: ContentChild, args: [DateFullCell, { read: TemplateRef },] }],
    monthCellChild: [{ type: ContentChild, args: [MonthCell, { read: TemplateRef },] }],
    monthFullCellChild: [{ type: ContentChild, args: [MonthFullCell, { read: TemplateRef },] }],
    fullscreen: [{ type: HostBinding, args: ['class.ant-fullcalendar--fullscreen',] }]
};
if (false) {
    /** @type {?} */
    NzCalendarComponent.prototype.nzMode;
    /** @type {?} */
    NzCalendarComponent.prototype.nzModeChange;
    /** @type {?} */
    NzCalendarComponent.prototype.nzPanelChange;
    /** @type {?} */
    NzCalendarComponent.prototype.nzSelectChange;
    /** @type {?} */
    NzCalendarComponent.prototype.nzValueChange;
    /** @type {?} */
    NzCalendarComponent.prototype.fullscreen;
    /** @type {?} */
    NzCalendarComponent.prototype.daysInWeek;
    /** @type {?} */
    NzCalendarComponent.prototype.monthsInYear;
    /** @type {?} */
    NzCalendarComponent.prototype.dateMatrix;
    /** @type {?} */
    NzCalendarComponent.prototype.activeDate;
    /** @type {?} */
    NzCalendarComponent.prototype.currentDateRow;
    /** @type {?} */
    NzCalendarComponent.prototype.currentDateCol;
    /** @type {?} */
    NzCalendarComponent.prototype.activeDateRow;
    /** @type {?} */
    NzCalendarComponent.prototype.activeDateCol;
    /** @type {?} */
    NzCalendarComponent.prototype.currentMonthRow;
    /** @type {?} */
    NzCalendarComponent.prototype.currentMonthCol;
    /** @type {?} */
    NzCalendarComponent.prototype.activeMonthRow;
    /** @type {?} */
    NzCalendarComponent.prototype.activeMonthCol;
    /** @type {?} */
    NzCalendarComponent.prototype.dateCell;
    /** @type {?} */
    NzCalendarComponent.prototype.dateFullCell;
    /** @type {?} */
    NzCalendarComponent.prototype.monthCell;
    /** @type {?} */
    NzCalendarComponent.prototype.monthFullCell;
    /**
     * @type {?}
     * @private
     */
    NzCalendarComponent.prototype.currentDate;
    /**
     * @type {?}
     * @private
     */
    NzCalendarComponent.prototype.onChangeFn;
    /**
     * @type {?}
     * @private
     */
    NzCalendarComponent.prototype.onTouchFn;
    /**
     * @type {?}
     * @private
     */
    NzCalendarComponent.prototype.i18n;
    /**
     * @type {?}
     * @private
     */
    NzCalendarComponent.prototype.cdr;
    /**
     * @type {?}
     * @private
     */
    NzCalendarComponent.prototype.dateHelper;
}
/**
 * @record
 */
export function DayCellContext() { }
if (false) {
    /** @type {?} */
    DayCellContext.prototype.title;
    /** @type {?} */
    DayCellContext.prototype.label;
}
/**
 * @record
 */
export function MonthCellContext() { }
if (false) {
    /** @type {?} */
    MonthCellContext.prototype.title;
    /** @type {?} */
    MonthCellContext.prototype.label;
    /** @type {?} */
    MonthCellContext.prototype.start;
}
/**
 * @record
 */
export function DateCellContext() { }
if (false) {
    /** @type {?} */
    DateCellContext.prototype.title;
    /** @type {?} */
    DateCellContext.prototype.label;
    /** @type {?} */
    DateCellContext.prototype.rel;
    /** @type {?} */
    DateCellContext.prototype.value;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotY2FsZW5kYXIuY29tcG9uZW50LmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbImNhbGVuZGFyL256LWNhbGVuZGFyLmNvbXBvbmVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7O0FBQUEsT0FBTyxFQUFFLHFCQUFxQixFQUFFLE1BQU0sdUJBQXVCLENBQUM7QUFDOUQsT0FBTyxFQUNMLFVBQVUsRUFDVix1QkFBdUIsRUFDdkIsaUJBQWlCLEVBQ2pCLFNBQVMsRUFDVCxZQUFZLEVBQ1osWUFBWSxFQUNaLFdBQVcsRUFDWCxLQUFLLEVBRUwsTUFBTSxFQUNOLFdBQVcsRUFDWCxpQkFBaUIsRUFDbEIsTUFBTSxlQUFlLENBQUM7QUFDdkIsT0FBTyxFQUF3QixpQkFBaUIsRUFBRSxNQUFNLGdCQUFnQixDQUFDO0FBQ3pFLE9BQU8sT0FBTyxNQUFNLG1CQUFtQixDQUFDO0FBQ3hDLE9BQU8sd0JBQXdCLE1BQU0sc0NBQXNDLENBQUM7QUFDNUUsT0FBTywwQkFBMEIsTUFBTSx3Q0FBd0MsQ0FBQztBQUNoRixPQUFPLHlCQUF5QixNQUFNLHVDQUF1QyxDQUFDO0FBQzlFLE9BQU8sVUFBVSxNQUFNLHVCQUF1QixDQUFDO0FBQy9DLE9BQU8sU0FBUyxNQUFNLHNCQUFzQixDQUFDO0FBQzdDLE9BQU8sV0FBVyxNQUFNLHdCQUF3QixDQUFDO0FBQ2pELE9BQU8sVUFBVSxNQUFNLHVCQUF1QixDQUFDO0FBQy9DLE9BQU8sV0FBVyxNQUFNLHdCQUF3QixDQUFDO0FBQ2pELE9BQU8sVUFBVSxNQUFNLHVCQUF1QixDQUFDO0FBQy9DLE9BQU8sUUFBUSxNQUFNLG9CQUFvQixDQUFDO0FBQzFDLE9BQU8sT0FBTyxNQUFNLG1CQUFtQixDQUFDO0FBQ3hDLE9BQU8sWUFBWSxNQUFNLHlCQUF5QixDQUFDO0FBQ25ELE9BQU8sV0FBVyxNQUFNLHdCQUF3QixDQUFDO0FBQ2pELE9BQU8sV0FBVyxNQUFNLHdCQUF3QixDQUFDO0FB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QUUsa0NBQThCOzs7OztJQUFFLHlDQUFxQzs7Ozs7QUFpS2hILG9DQUdDOzs7SUFGQywrQkFBYzs7SUFDZCwrQkFBYzs7Ozs7QUFHaEIsc0NBSUM7OztJQUhDLGlDQUFjOztJQUNkLGlDQUFjOztJQUNkLGlDQUFZOzs7OztBQUdkLHFDQUtDOzs7SUFKQyxnQ0FBYzs7SUFDZCxnQ0FBYzs7SUFDZCw4QkFBaUM7O0lBQ2pDLGdDQUFZIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHsgY29lcmNlQm9vbGVhblByb3BlcnR5IH0gZnJvbSAnQGFuZ3VsYXIvY2RrL2NvZXJjaW9uJztcbmltcG9ydCB7XG4gIGZvcndhcmRSZWYsXG4gIENoYW5nZURldGVjdGlvblN0cmF0ZWd5LFxuICBDaGFuZ2VEZXRlY3RvclJlZixcbiAgQ29tcG9uZW50LFxuICBDb250ZW50Q2hpbGQsXG4gIEV2ZW50RW1pdHRlcixcbiAgSG9zdEJpbmRpbmcsXG4gIElucHV0LFxuICBPbkluaXQsXG4gIE91dHB1dCxcbiAgVGVtcGxhdGVSZWYsXG4gIFZpZXdFbmNhcHN1bGF0aW9uXG59IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuaW1wb3J0IHsgQ29udHJvbFZhbHVlQWNjZXNzb3IsIE5HX1ZBTFVFX0FDQ0VTU09SIH0gZnJvbSAnQGFuZ3VsYXIvZm9ybXMnO1xuaW1wb3J0IGFkZERheXMgZnJvbSAnZGF0ZS1mbnMvYWRkX2RheXMnO1xuaW1wb3J0IGRpZmZlcmVuY2VJbkNhbGVuZGFyRGF5cyBmcm9tICdkYXRlLWZucy9kaWZmZXJlbmNlX2luX2NhbGVuZGFyX2RheXMnO1xuaW1wb3J0IGRpZmZlcmVuY2VJbkNhbGVuZGFyTW9udGhzIGZyb20gJ2RhdGUtZm5zL2RpZmZlcmVuY2VfaW5fY2FsZW5kYXJfbW9udGhzJztcbmltcG9ydCBkaWZmZXJlbmNlSW5DYWxlbmRhcldlZWtzIGZyb20gJ2RhdGUtZm5zL2RpZmZlcmVuY2VfaW5fY2FsZW5kYXJfd2Vla3MnO1xuaW1wb3J0IGVuZE9mTW9udGggZnJvbSAnZGF0ZS1mbnMvZW5kX29mX21vbnRoJztcbmltcG9ydCBpc1NhbWVEYXkgZnJvbSAnZGF0ZS1mbnMvaXNfc2FtZV9kYXknO1xuaW1wb3J0IGlzU2FtZU1vbnRoIGZyb20gJ2RhdGUtZm5zL2lzX3NhbWVfbW9udGgnO1xuaW1wb3J0IGlzU2FtZVllYXIgZnJvbSAnZGF0ZS1mbnMvaXNfc2FtZV95ZWFyJztcbmltcG9ydCBpc1RoaXNNb250aCBmcm9tICdkYXRlLWZucy9pc190aGlzX21vbnRoJztcbmltcG9ydCBpc1RoaXNZZWFyIGZyb20gJ2RhdGUtZm5zL2lzX3RoaXNfeWVhcic7XG5pbXBvcnQgc2V0TW9udGggZnJvbSAnZGF0ZS1mbnMvc2V0X21vbnRoJztcbmltcG9ydCBzZXRZZWFyIGZyb20gJ2RhdGUtZm5zL3NldF95ZWFyJztcbmltcG9ydCBzdGFydE9mTW9udGggZnJvbSAnZGF0ZS1mbnMvc3RhcnRfb2ZfbW9udGgnO1xuaW1wb3J0IHN0YXJ0T2ZXZWVrIGZyb20gJ2RhdGUtZm5zL3N0YXJ0X29mX3dlZWsnO1xuaW1wb3J0IHN0YXJ0T2ZZZWFyIGZyb20gJ2RhdGUtZm5zL3N0YXJ0X29mX3llYXInO1xuaW1wb3J0IHsgRGF0ZUhlbHBlclNlcnZpY2UgfSBmcm9tICcuLi9pMThuL2RhdGUtaGVscGVyLnNlcnZpY2UnO1xuaW1wb3J0IHsgTnpJMThuU2VydmljZSB9IGZyb20gJy4uL2kxOG4vbnotaTE4bi5zZXJ2aWNlJztcbmltcG9ydCB7XG4gIE56RGF0ZUNlbGxEaXJlY3RpdmUgYXMgRGF0ZUNlbGwsXG4gIE56RGF0ZUZ1bGxDZWxsRGlyZWN0aXZlIGFzIERhdGVGdWxsQ2VsbCxcbiAgTnpNb250aENlbGxEaXJlY3RpdmUgYXMgTW9udGhDZWxsLFxuICBOek1vbnRoRnVsbENlbGxEaXJlY3RpdmUgYXMgTW9udGhGdWxsQ2VsbFxufSBmcm9tICcuL256LWNhbGVuZGFyLWNlbGxzJztcblxuZXhwb3J0IHR5cGUgTW9kZVR5cGUgPSAnbW9udGgnIHwgJ3llYXInO1xuXG5AQ29tcG9uZW50KHtcbiAgZW5jYXBzdWxhdGlvbjogVmlld0VuY2Fwc3VsYXRpb24uTm9uZSxcbiAgY2hhbmdlRGV0ZWN0aW9uOiBDaGFuZ2VEZXRlY3Rpb25TdHJhdGVneS5PblB1c2gsXG4gIHNlbGVjdG9yOiAnbnotY2FsZW5kYXInLFxuICB0ZW1wbGF0ZVVybDogJy4vbnotY2FsZW5kYXIuY29tcG9uZW50Lmh0bWwnLFxuICBwcm92aWRlcnM6IFt7IHByb3ZpZGU6IE5HX1ZBTFVFX0FDQ0VTU09SLCB1c2VFeGlzdGluZzogZm9yd2FyZFJlZigoKSA9PiBOekNhbGVuZGFyQ29tcG9uZW50KSwgbXVsdGk6IHRydWUgfV1cbn0pXG5leHBvcnQgY2xhc3MgTnpDYWxlbmRhckNvbXBvbmVudCBpbXBsZW1lbnRzIENvbnRyb2xWYWx1ZUFjY2Vzc29yLCBPbkluaXQge1xuICBASW5wdXQoKSBuek1vZGU6IE1vZGVUeXBlID0gJ21vbnRoJztcbiAgQE91dHB1dCgpIHJlYWRvbmx5IG56TW9kZUNoYW5nZTogRXZlbnRFbWl0dGVyPE1vZGVUeXBlPiA9IG5ldyBFdmVudEVtaXR0ZXIoKTtcbiAgQE91dHB1dCgpIHJlYWRvbmx5IG56UGFuZWxDaGFuZ2U6IEV2ZW50RW1pdHRlcjx7IGRhdGU6IERhdGU7IG1vZGU6IE1vZGVUeXBlIH0+ID0gbmV3IEV2ZW50RW1pdHRlcigpO1xuXG4gIEBPdXRwdXQoKSByZWFkb25seSBuelNlbGVjdENoYW5nZTogRXZlbnRFbWl0dGVyPERhdGU+ID0gbmV3IEV2ZW50RW1pdHRlcigpO1xuXG4gIEBJbnB1dCgpIHNldCBuelZhbHVlKHZhbHVlOiBEYXRlKSB7XG4gICAgdGhpcy51cGRhdGVEYXRlKHZhbHVlLCBmYWxzZSk7XG4gIH1cbiAgQE91dHB1dCgpIHJlYWRvbmx5IG56VmFsdWVDaGFuZ2U6IEV2ZW50RW1pdHRlcjxEYXRlPiA9IG5ldyBFdmVudEVtaXR0ZXIoKTtcblxuICBASW5wdXQoKVxuICBzZXQgbnpEYXRlQ2VsbCh2YWx1ZTogVGVtcGxhdGVSZWY8eyAkaW1wbGljaXQ6IERhdGUgfT4pIHtcbiAgICB0aGlzLmRhdGVDZWxsID0gdmFsdWU7XG4gIH1cblxuICBASW5wdXQoKVxuICBzZXQgbnpEYXRlRnVsbENlbGwodmFsdWU6IFRlbXBsYXRlUmVmPHsgJGltcGxpY2l0OiBEYXRlIH0+KSB7XG4gICAgdGhpcy5kYXRlRnVsbENlbGwgPSB2YWx1ZTtcbiAgfVxuXG4gIEBJbnB1dCgpXG4gIHNldCBuek1vbnRoQ2VsbCh2YWx1ZTogVGVtcGxhdGVSZWY8eyAkaW1wbGljaXQ6IERhdGUgfT4pIHtcbiAgICB0aGlzLm1vbnRoQ2VsbCA9IHZhbHVlO1xuICB9XG5cbiAgQElucHV0KClcbiAgc2V0IG56TW9udGhGdWxsQ2VsbCh2YWx1ZTogVGVtcGxhdGVSZWY8eyAkaW1wbGljaXQ6IERhdGUgfT4pIHtcbiAgICB0aGlzLm1vbnRoRnVsbENlbGwgPSB2YWx1ZTtcbiAgfVxuXG4gIEBJbnB1dCgpXG4gIHNldCBuekZ1bGxzY3JlZW4odmFsdWU6IGJvb2xlYW4pIHtcbiAgICB0aGlzLmZ1bGxzY3JlZW4gPSBjb2VyY2VCb29sZWFuUHJvcGVydHkodmFsdWUpO1xuICB9XG4gIGdldCBuekZ1bGxzY3JlZW4oKTogYm9vbGVhbiB7XG4gICAgcmV0dXJuIHRoaXMuZnVsbHNjcmVlbjtcbiAgfVxuXG4gIEBJbnB1dCgpXG4gIHNldCBuekNhcmQodmFsdWU6IGJvb2xlYW4pIHtcbiAgICB0aGlzLmZ1bGxzY3JlZW4gPSAhY29lcmNlQm9vbGVhblByb3BlcnR5KHZhbHVlKTtcbiAgfVxuICBnZXQgbnpDYXJkKCk6IGJvb2xlYW4ge1xuICAgIHJldHVybiAhdGhpcy5mdWxsc2NyZWVuO1xuICB9XG5cbiAgQENvbnRlbnRDaGlsZChEYXRlQ2VsbCwgeyByZWFkOiBUZW1wbGF0ZVJlZiB9KVxuICBzZXQgZGF0ZUNlbGxDaGlsZCh2YWx1ZTogVGVtcGxhdGVSZWY8eyAkaW1wbGljaXQ6IERhdGUgfT4pIHtcbiAgICBpZiAodmFsdWUpIHtcbiAgICAgIHRoaXMuZGF0ZUNlbGwgPSB2YWx1ZTtcbiAgICB9XG4gIH1cblxuICBAQ29udGVudENoaWxkKERhdGVGdWxsQ2VsbCwgeyByZWFkOiBUZW1wbGF0ZVJlZiB9KVxuICBzZXQgZGF0ZUZ1bGxDZWxsQ2hpbGQodmFsdWU6IFRlbXBsYXRlUmVmPHsgJGltcGxpY2l0OiBEYXRlIH0+KSB7XG4gICAgaWYgKHZhbHVlKSB7XG4gICAgICB0aGlzLmRhdGVGdWxsQ2VsbCA9IHZhbHVlO1xuICAgIH1cbiAgfVxuXG4gIEBDb250ZW50Q2hpbGQoTW9udGhDZWxsLCB7IHJlYWQ6IFRlbXBsYXRlUmVmIH0pXG4gIHNldCBtb250aENlbGxDaGlsZCh2YWx1ZTogVGVtcGxhdGVSZWY8eyAkaW1wbGljaXQ6IERhdGUgfT4pIHtcbiAgICBpZiAodmFsdWUpIHtcbiAgICAgIHRoaXMubW9udGhDZWxsID0gdmFsdWU7XG4gICAgfVxuICB9XG5cbiAgQENvbnRlbnRDaGlsZChNb250aEZ1bGxDZWxsLCB7IHJlYWQ6IFRlbXBsYXRlUmVmIH0pXG4gIHNldCBtb250aEZ1bGxDZWxsQ2hpbGQodmFsdWU6IFRlbXBsYXRlUmVmPHsgJGltcGxpY2l0OiBEYXRlIH0+KSB7XG4gICAgaWYgKHZhbHVlKSB7XG4gICAgICB0aGlzLm1vbnRoRnVsbENlbGwgPSB2YWx1ZTtcbiAgICB9XG4gIH1cblxuICBASG9zdEJpbmRpbmcoJ2NsYXNzLmFudC1mdWxsY2FsZW5kYXItLWZ1bGxzY3JlZW4nKVxuICBmdWxsc2NyZWVuID0gdHJ1ZTtcblxuICBkYXlzSW5XZWVrOiBEYXlDZWxsQ29udGV4dFtdID0gW107XG4gIG1vbnRoc0luWWVhcjogTW9udGhDZWxsQ29udGV4dFtdID0gW107XG4gIGRhdGVNYXRyaXg6IERhdGVDZWxsQ29udGV4dFtdW10gPSBbXTtcbiAgYWN0aXZlRGF0ZTogRGF0ZSA9IG5ldyBEYXRlKCk7XG4gIGN1cnJlbnREYXRlUm93OiBudW1iZXIgPSAtMTtcbiAgY3VycmVudERhdGVDb2w6IG51bWJlciA9IC0xO1xuICBhY3RpdmVEYXRlUm93OiBudW1iZXIgPSAtMTtcbiAgYWN0aXZlRGF0ZUNvbDogbnVtYmVyID0gLTE7XG4gIGN1cnJlbnRNb250aFJvdzogbnVtYmVyID0gLTE7XG4gIGN1cnJlbnRNb250aENvbDogbnVtYmVyID0gLTE7XG4gIGFjdGl2ZU1vbnRoUm93OiBudW1iZXIgPSAtMTtcbiAgYWN0aXZlTW9udGhDb2w6IG51bWJlciA9IC0xO1xuICBkYXRlQ2VsbDogVGVtcGxhdGVSZWY8eyAkaW1wbGljaXQ6IERhdGUgfT4gfCBudWxsID0gbnVsbDtcbiAgZGF0ZUZ1bGxDZWxsOiBUZW1wbGF0ZVJlZjx7ICRpbXBsaWNpdDogRGF0ZSB9PiB8IG51bGwgPSBudWxsO1xuICBtb250aENlbGw6IFRlbXBsYXRlUmVmPHsgJGltcGxpY2l0OiBEYXRlIH0+IHwgbnVsbCA9IG51bGw7XG4gIG1vbnRoRnVsbENlbGw6IFRlbXBsYXRlUmVmPHsgJGltcGxpY2l0OiBEYXRlIH0+IHwgbnVsbCA9IG51bGw7XG5cbiAgcHJpdmF0ZSBjdXJyZW50RGF0ZSA9IG5ldyBEYXRlKCk7XG4gIHByaXZhdGUgb25DaGFuZ2VGbjogKGRhdGU6IERhdGUpID0+IHZvaWQgPSAoKSA9PiB7fTtcbiAgcHJpdmF0ZSBvblRvdWNoRm46ICgpID0+IHZvaWQgPSAoKSA9PiB7fTtcblxuICBwcml2YXRlIGdldCBjYWxlbmRhclN0YXJ0KCk6IERhdGUge1xuICAgIHJldHVybiBzdGFydE9mV2VlayhzdGFydE9mTW9udGgodGhpcy5hY3RpdmVEYXRlKSwgeyB3ZWVrU3RhcnRzT246IHRoaXMuZGF0ZUhlbHBlci5nZXRGaXJzdERheU9mV2VlaygpIH0pO1xuICB9XG5cbiAgY29uc3RydWN0b3IocHJpdmF0ZSBpMThuOiBOekkxOG5TZXJ2aWNlLCBwcml2YXRlIGNkcjogQ2hhbmdlRGV0ZWN0b3JSZWYsIHByaXZhdGUgZGF0ZUhlbHBlcjogRGF0ZUhlbHBlclNlcnZpY2UpIHt9XG5cbiAgbmdPbkluaXQoKTogdm9pZCB7XG4gICAgdGhpcy5zZXRVcERheXNJbldlZWsoKTtcbiAgICB0aGlzLnNldFVwTW9udGhzSW5ZZWFyKCk7XG4gICAgdGhpcy5zZXRVcERhdGVNYXRyaXgoKTtcbiAgICB0aGlzLmNhbGN1bGF0ZUN1cnJlbnREYXRlKCk7XG4gICAgdGhpcy5jYWxjdWxhdGVBY3RpdmVEYXRlKCk7XG4gICAgdGhpcy5jYWxjdWxhdGVDdXJyZW50TW9udGgoKTtcbiAgICB0aGlzLmNhbGN1bGF0ZUFjdGl2ZU1vbnRoKCk7XG4gIH1cblxuICBvbk1vZGVDaGFuZ2UobW9kZTogTW9kZVR5cGUpOiB2b2lkIHtcbiAgICB0aGlzLm56TW9kZUNoYW5nZS5lbWl0KG1vZGUpO1xuICAgIHRoaXMubnpQYW5lbENoYW5nZS5lbWl0KHsgZGF0ZTogdGhpcy5hY3RpdmVEYXRlLCBtb2RlIH0pO1xuICB9XG5cbiAgb25EYXRlU2VsZWN0KGRhdGU6IERhdGUpOiB2b2lkIHtcbiAgICB0aGlzLnVwZGF0ZURhdGUoZGF0ZSk7XG4gICAgdGhpcy5uelNlbGVjdENoYW5nZS5lbWl0KGRhdGUpO1xuICB9XG5cbiAgb25ZZWFyU2VsZWN0KHllYXI6IG51bWJlcik6IHZvaWQge1xuICAgIGNvbnN0IGRhdGUgPSBzZXRZZWFyKHRoaXMuYWN0aXZlRGF0ZSwgeWVhcik7XG4gICAgdGhpcy51cGRhdGVEYXRlKGRhdGUpO1xuICAgIHRoaXMubnpTZWxlY3RDaGFuZ2UuZW1pdChkYXRlKTtcbiAgfVxuXG4gIG9uTW9udGhTZWxlY3QobW9udGg6IG51bWJlcik6IHZvaWQge1xuICAgIGNvbnN0IGRhdGUgPSBzZXRNb250aCh0aGlzLmFjdGl2ZURhdGUsIG1vbnRoKTtcbiAgICB0aGlzLnVwZGF0ZURhdGUoZGF0ZSk7XG4gICAgdGhpcy5uelNlbGVjdENoYW5nZS5lbWl0KGRhdGUpO1xuICB9XG5cbiAgd3JpdGVWYWx1ZSh2YWx1ZTogRGF0ZSB8IG51bGwpOiB2b2lkIHtcbiAgICB0aGlzLnVwZGF0ZURhdGUodmFsdWUgfHwgbmV3IERhdGUoKSwgZmFsc2UpO1xuICAgIHRoaXMuY2RyLm1hcmtGb3JDaGVjaygpO1xuICB9XG5cbiAgcmVnaXN0ZXJPbkNoYW5nZShmbjogKGRhdGU6IERhdGUpID0+IHZvaWQpOiB2b2lkIHtcbiAgICB0aGlzLm9uQ2hhbmdlRm4gPSBmbjtcbiAgfVxuXG4gIHJlZ2lzdGVyT25Ub3VjaGVkKGZuOiAoKSA9PiB2b2lkKTogdm9pZCB7XG4gICAgdGhpcy5vblRvdWNoRm4gPSBmbjtcbiAgfVxuXG4gIHByaXZhdGUgdXBkYXRlRGF0ZShkYXRlOiBEYXRlLCB0b3VjaGVkOiBib29sZWFuID0gdHJ1ZSk6IHZvaWQge1xuICAgIGNvbnN0IGRheUNoYW5nZWQgPSAhaXNTYW1lRGF5KGRhdGUsIHRoaXMuYWN0aXZlRGF0ZSk7XG4gICAgY29uc3QgbW9udGhDaGFuZ2VkID0gIWlzU2FtZU1vbnRoKGRhdGUsIHRoaXMuYWN0aXZlRGF0ZSk7XG4gICAgY29uc3QgeWVhckNoYW5nZWQgPSAhaXNTYW1lWWVhcihkYXRlLCB0aGlzLmFjdGl2ZURhdGUpO1xuXG4gICAgdGhpcy5hY3RpdmVEYXRlID0gZGF0ZTtcblxuICAgIGlmIChkYXlDaGFuZ2VkKSB7XG4gICAgICB0aGlzLmNhbGN1bGF0ZUFjdGl2ZURhdGUoKTtcbiAgICB9XG4gICAgaWYgKG1vbnRoQ2hhbmdlZCkge1xuICAgICAgdGhpcy5zZXRVcERhdGVNYXRyaXgoKTtcbiAgICAgIHRoaXMuY2FsY3VsYXRlQ3VycmVudERhdGUoKTtcbiAgICAgIHRoaXMuY2FsY3VsYXRlQWN0aXZlTW9udGgoKTtcbiAgICB9XG4gICAgaWYgKHllYXJDaGFuZ2VkKSB7XG4gICAgICB0aGlzLmNhbGN1bGF0ZUN1cnJlbnRNb250aCgpO1xuICAgIH1cblxuICAgIGlmICh0b3VjaGVkKSB7XG4gICAgICB0aGlzLm9uQ2hhbmdlRm4oZGF0ZSk7XG4gICAgICB0aGlzLm9uVG91Y2hGbigpO1xuICAgICAgdGhpcy5uelZhbHVlQ2hhbmdlLmVtaXQoZGF0ZSk7XG4gICAgfVxuICB9XG5cbiAgcHJpdmF0ZSBzZXRVcERheXNJbldlZWsoKTogdm9pZCB7XG4gICAgdGhpcy5kYXlzSW5XZWVrID0gW107XG4gICAgY29uc3Qgd2Vla1N0YXJ0ID0gc3RhcnRPZldlZWsodGhpcy5hY3RpdmVEYXRlLCB7IHdlZWtTdGFydHNPbjogdGhpcy5kYXRlSGVscGVyLmdldEZpcnN0RGF5T2ZXZWVrKCkgfSk7XG4gICAgZm9yIChsZXQgaSA9IDA7IGkgPCA3OyBpKyspIHtcbiAgICAgIGNvbnN0IGRhdGUgPSBhZGREYXlzKHdlZWtTdGFydCwgaSk7XG4gICAgICBjb25zdCB0aXRsZSA9IHRoaXMuZGF0ZUhlbHBlci5mb3JtYXQoZGF0ZSwgdGhpcy5kYXRlSGVscGVyLnJlbHlPbkRhdGVQaXBlID8gJ0UnIDogJ2RkZCcpO1xuICAgICAgY29uc3QgbGFiZWwgPSB0aGlzLmRhdGVIZWxwZXIuZm9ybWF0KGRhdGUsIHRoaXMuZGF0ZUhlbHBlci5yZWx5T25EYXRlUGlwZSA/ICdFRUVFRUUnIDogJ2RkJyk7XG4gICAgICB0aGlzLmRheXNJbldlZWsucHVzaCh7IHRpdGxlLCBsYWJlbCB9KTtcbiAgICB9XG4gIH1cblxuICBwcml2YXRlIHNldFVwTW9udGhzSW5ZZWFyKCk6IHZvaWQge1xuICAgIHRoaXMubW9udGhzSW5ZZWFyID0gW107XG4gICAgZm9yIChsZXQgaSA9IDA7IGkgPCAxMjsgaSsrKSB7XG4gICAgICBjb25zdCBkYXRlID0gc2V0TW9udGgodGhpcy5hY3RpdmVEYXRlLCBpKTtcbiAgICAgIGNvbnN0IHRpdGxlID0gdGhpcy5kYXRlSGVscGVyLmZvcm1hdChkYXRlLCAnTU1NJyk7XG4gICAgICBjb25zdCBsYWJlbCA9IHRoaXMuZGF0ZUhlbHBlci5mb3JtYXQoZGF0ZSwgJ01NTScpO1xuICAgICAgY29uc3Qgc3RhcnQgPSBzdGFydE9mTW9udGgoZGF0ZSk7XG4gICAgICB0aGlzLm1vbnRoc0luWWVhci5wdXNoKHsgdGl0bGUsIGxhYmVsLCBzdGFydCB9KTtcbiAgICB9XG4gIH1cblxuICBwcml2YXRlIHNldFVwRGF0ZU1hdHJpeCgpOiB2b2lkIHtcbiAgICB0aGlzLmRhdGVNYXRyaXggPSBbXTtcbiAgICBjb25zdCBtb250aFN0YXJ0ID0gc3RhcnRPZk1vbnRoKHRoaXMuYWN0aXZlRGF0ZSk7XG4gICAgY29uc3QgbW9udGhFbmQgPSBlbmRPZk1vbnRoKHRoaXMuYWN0aXZlRGF0ZSk7XG4gICAgY29uc3Qgd2Vla0RpZmYgPVxuICAgICAgZGlmZmVyZW5jZUluQ2FsZW5kYXJXZWVrcyhtb250aEVuZCwgbW9udGhTdGFydCwgeyB3ZWVrU3RhcnRzT246IHRoaXMuZGF0ZUhlbHBlci5nZXRGaXJzdERheU9mV2VlaygpIH0pICsgMjtcblxuICAgIGZvciAobGV0IHdlZWsgPSAwOyB3ZWVrIDwgd2Vla0RpZmY7IHdlZWsrKykge1xuICAgICAgY29uc3Qgcm93OiBEYXRlQ2VsbENvbnRleHRbXSA9IFtdO1xuICAgICAgY29uc3Qgd2Vla1N0YXJ0ID0gYWRkRGF5cyh0aGlzLmNhbGVuZGFyU3RhcnQsIHdlZWsgKiA3KTtcblxuICAgICAgZm9yIChsZXQgZGF5ID0gMDsgZGF5IDwgNzsgZGF5KyspIHtcbiAgICAgICAgY29uc3QgZGF0ZSA9IGFkZERheXMod2Vla1N0YXJ0LCBkYXkpO1xuICAgICAgICBjb25zdCBtb250aERpZmYgPSBkaWZmZXJlbmNlSW5DYWxlbmRhck1vbnRocyhkYXRlLCB0aGlzLmFjdGl2ZURhdGUpO1xuICAgICAgICBjb25zdCBkYXRlRm9ybWF0ID0gdGhpcy5kYXRlSGVscGVyLnJlbHlPbkRhdGVQaXBlXG4gICAgICAgICAgPyAnbG9uZ0RhdGUnXG4gICAgICAgICAgOiB0aGlzLmkxOG4uZ2V0TG9jYWxlRGF0YSgnRGF0ZVBpY2tlci5sYW5nLmRhdGVGb3JtYXQnLCAnWVlZWS1NTS1ERCcpO1xuICAgICAgICBjb25zdCB0aXRsZSA9IHRoaXMuZGF0ZUhlbHBlci5mb3JtYXQoZGF0ZSwgZGF0ZUZvcm1hdCk7XG4gICAgICAgIGNvbnN0IGxhYmVsID0gdGhpcy5kYXRlSGVscGVyLmZvcm1hdChkYXRlLCB0aGlzLmRhdGVIZWxwZXIucmVseU9uRGF0ZVBpcGUgPyAnZGQnIDogJ0REJyk7XG4gICAgICAgIGNvbnN0IHJlbCA9IG1vbnRoRGlmZiA9PT0gMCA/ICdjdXJyZW50JyA6IG1vbnRoRGlmZiA8IDAgPyAnbGFzdCcgOiAnbmV4dCc7XG4gICAgICAgIHJvdy5wdXNoKHsgdGl0bGUsIGxhYmVsLCByZWwsIHZhbHVlOiBkYXRlIH0pO1xuICAgICAgfVxuICAgICAgdGhpcy5kYXRlTWF0cml4LnB1c2gocm93KTtcbiAgICB9XG4gIH1cblxuICBwcml2YXRlIGNhbGN1bGF0ZUN1cnJlbnREYXRlKCk6IHZvaWQge1xuICAgIGlmIChpc1RoaXNNb250aCh0aGlzLmFjdGl2ZURhdGUpKSB7XG4gICAgICB0aGlzLmN1cnJlbnREYXRlUm93ID0gZGlmZmVyZW5jZUluQ2FsZW5kYXJXZWVrcyh0aGlzLmN1cnJlbnREYXRlLCB0aGlzLmNhbGVuZGFyU3RhcnQsIHtcbiAgICAgICAgd2Vla1N0YXJ0c09uOiB0aGlzLmRhdGVIZWxwZXIuZ2V0Rmlyc3REYXlPZldlZWsoKVxuICAgICAgfSk7XG4gICAgICB0aGlzLmN1cnJlbnREYXRlQ29sID0gZGlmZmVyZW5jZUluQ2FsZW5kYXJEYXlzKFxuICAgICAgICB0aGlzLmN1cnJlbnREYXRlLFxuICAgICAgICBhZGREYXlzKHRoaXMuY2FsZW5kYXJTdGFydCwgdGhpcy5jdXJyZW50RGF0ZVJvdyAqIDcpXG4gICAgICApO1xuICAgIH0gZWxzZSB7XG4gICAgICB0aGlzLmN1cnJlbnREYXRlUm93ID0gLTE7XG4gICAgICB0aGlzLmN1cnJlbnREYXRlQ29sID0gLTE7XG4gICAgfVxuICB9XG5cbiAgcHJpdmF0ZSBjYWxjdWxhdGVBY3RpdmVEYXRlKCk6IHZvaWQge1xuICAgIHRoaXMuYWN0aXZlRGF0ZVJvdyA9IGRpZmZlcmVuY2VJbkNhbGVuZGFyV2Vla3ModGhpcy5hY3RpdmVEYXRlLCB0aGlzLmNhbGVuZGFyU3RhcnQsIHtcbiAgICAgIHdlZWtTdGFydHNPbjogdGhpcy5kYXRlSGVscGVyLmdldEZpcnN0RGF5T2ZXZWVrKClcbiAgICB9KTtcbiAgICB0aGlzLmFjdGl2ZURhdGVDb2wgPSBkaWZmZXJlbmNlSW5DYWxlbmRhckRheXModGhpcy5hY3RpdmVEYXRlLCBhZGREYXlzKHRoaXMuY2FsZW5kYXJTdGFydCwgdGhpcy5hY3RpdmVEYXRlUm93ICogNykpO1xuICB9XG5cbiAgcHJpdmF0ZSBjYWxjdWxhdGVDdXJyZW50TW9udGgoKTogdm9pZCB7XG4gICAgaWYgKGlzVGhpc1llYXIodGhpcy5hY3RpdmVEYXRlKSkge1xuICAgICAgY29uc3QgeWVhclN0YXJ0ID0gc3RhcnRPZlllYXIodGhpcy5jdXJyZW50RGF0ZSk7XG4gICAgICBjb25zdCBtb250aERpZmYgPSBkaWZmZXJlbmNlSW5DYWxlbmRhck1vbnRocyh0aGlzLmN1cnJlbnREYXRlLCB5ZWFyU3RhcnQpO1xuICAgICAgdGhpcy5jdXJyZW50TW9udGhSb3cgPSBNYXRoLmZsb29yKG1vbnRoRGlmZiAvIDMpO1xuICAgICAgdGhpcy5jdXJyZW50TW9udGhDb2wgPSBtb250aERpZmYgJSAzO1xuICAgIH0gZWxzZSB7XG4gICAgICB0aGlzLmN1cnJlbnRNb250aFJvdyA9IC0xO1xuICAgICAgdGhpcy5jdXJyZW50TW9udGhDb2wgPSAtMTtcbiAgICB9XG4gIH1cblxuICBwcml2YXRlIGNhbGN1bGF0ZUFjdGl2ZU1vbnRoKCk6IHZvaWQge1xuICAgIHRoaXMuYWN0aXZlTW9udGhSb3cgPSBNYXRoLmZsb29yKHRoaXMuYWN0aXZlRGF0ZS5nZXRNb250aCgpIC8gMyk7XG4gICAgdGhpcy5hY3RpdmVNb250aENvbCA9IHRoaXMuYWN0aXZlRGF0ZS5nZXRNb250aCgpICUgMztcbiAgfVxufVxuXG5leHBvcnQgaW50ZXJmYWNlIERheUNlbGxDb250ZXh0IHtcbiAgdGl0bGU6IHN0cmluZztcbiAgbGFiZWw6IHN0cmluZztcbn1cblxuZXhwb3J0IGludGVyZmFjZSBNb250aENlbGxDb250ZXh0IHtcbiAgdGl0bGU6IHN0cmluZztcbiAgbGFiZWw6IHN0cmluZztcbiAgc3RhcnQ6IERhdGU7XG59XG5cbmV4cG9ydCBpbnRlcmZhY2UgRGF0ZUNlbGxDb250ZXh0IHtcbiAgdGl0bGU6IHN0cmluZztcbiAgbGFiZWw6IHN0cmluZztcbiAgcmVsOiAnbGFzdCcgfCAnY3VycmVudCcgfCAnbmV4dCc7XG4gIHZhbHVlOiBEYXRlO1xufVxuIl19