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
var NzCalendarComponent = /** @class */ (function () {
    function NzCalendarComponent(i18n, cdr, dateHelper) {
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
        function () { });
        this.onTouchFn = (/**
         * @return {?}
         */
        function () { });
    }
    Object.defineProperty(NzCalendarComponent.prototype, "nzValue", {
        set: /**
         * @param {?} value
         * @return {?}
         */
        function (value) {
            this.updateDate(value, false);
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzCalendarComponent.prototype, "nzDateCell", {
        set: /**
         * @param {?} value
         * @return {?}
         */
        function (value) {
            this.dateCell = value;
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzCalendarComponent.prototype, "nzDateFullCell", {
        set: /**
         * @param {?} value
         * @return {?}
         */
        function (value) {
            this.dateFullCell = value;
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzCalendarComponent.prototype, "nzMonthCell", {
        set: /**
         * @param {?} value
         * @return {?}
         */
        function (value) {
            this.monthCell = value;
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzCalendarComponent.prototype, "nzMonthFullCell", {
        set: /**
         * @param {?} value
         * @return {?}
         */
        function (value) {
            this.monthFullCell = value;
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzCalendarComponent.prototype, "nzFullscreen", {
        get: /**
         * @return {?}
         */
        function () {
            return this.fullscreen;
        },
        set: /**
         * @param {?} value
         * @return {?}
         */
        function (value) {
            this.fullscreen = coerceBooleanProperty(value);
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzCalendarComponent.prototype, "nzCard", {
        get: /**
         * @return {?}
         */
        function () {
            return !this.fullscreen;
        },
        set: /**
         * @param {?} value
         * @return {?}
         */
        function (value) {
            this.fullscreen = !coerceBooleanProperty(value);
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzCalendarComponent.prototype, "dateCellChild", {
        set: /**
         * @param {?} value
         * @return {?}
         */
        function (value) {
            if (value) {
                this.dateCell = value;
            }
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzCalendarComponent.prototype, "dateFullCellChild", {
        set: /**
         * @param {?} value
         * @return {?}
         */
        function (value) {
            if (value) {
                this.dateFullCell = value;
            }
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzCalendarComponent.prototype, "monthCellChild", {
        set: /**
         * @param {?} value
         * @return {?}
         */
        function (value) {
            if (value) {
                this.monthCell = value;
            }
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzCalendarComponent.prototype, "monthFullCellChild", {
        set: /**
         * @param {?} value
         * @return {?}
         */
        function (value) {
            if (value) {
                this.monthFullCell = value;
            }
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzCalendarComponent.prototype, "calendarStart", {
        get: /**
         * @private
         * @return {?}
         */
        function () {
            return startOfWeek(startOfMonth(this.activeDate), { weekStartsOn: this.dateHelper.getFirstDayOfWeek() });
        },
        enumerable: true,
        configurable: true
    });
    /**
     * @return {?}
     */
    NzCalendarComponent.prototype.ngOnInit = /**
     * @return {?}
     */
    function () {
        this.setUpDaysInWeek();
        this.setUpMonthsInYear();
        this.setUpDateMatrix();
        this.calculateCurrentDate();
        this.calculateActiveDate();
        this.calculateCurrentMonth();
        this.calculateActiveMonth();
    };
    /**
     * @param {?} mode
     * @return {?}
     */
    NzCalendarComponent.prototype.onModeChange = /**
     * @param {?} mode
     * @return {?}
     */
    function (mode) {
        this.nzModeChange.emit(mode);
        this.nzPanelChange.emit({ date: this.activeDate, mode: mode });
    };
    /**
     * @param {?} date
     * @return {?}
     */
    NzCalendarComponent.prototype.onDateSelect = /**
     * @param {?} date
     * @return {?}
     */
    function (date) {
        this.updateDate(date);
        this.nzSelectChange.emit(date);
    };
    /**
     * @param {?} year
     * @return {?}
     */
    NzCalendarComponent.prototype.onYearSelect = /**
     * @param {?} year
     * @return {?}
     */
    function (year) {
        /** @type {?} */
        var date = setYear(this.activeDate, year);
        this.updateDate(date);
        this.nzSelectChange.emit(date);
    };
    /**
     * @param {?} month
     * @return {?}
     */
    NzCalendarComponent.prototype.onMonthSelect = /**
     * @param {?} month
     * @return {?}
     */
    function (month) {
        /** @type {?} */
        var date = setMonth(this.activeDate, month);
        this.updateDate(date);
        this.nzSelectChange.emit(date);
    };
    /**
     * @param {?} value
     * @return {?}
     */
    NzCalendarComponent.prototype.writeValue = /**
     * @param {?} value
     * @return {?}
     */
    function (value) {
        this.updateDate(value || new Date(), false);
        this.cdr.markForCheck();
    };
    /**
     * @param {?} fn
     * @return {?}
     */
    NzCalendarComponent.prototype.registerOnChange = /**
     * @param {?} fn
     * @return {?}
     */
    function (fn) {
        this.onChangeFn = fn;
    };
    /**
     * @param {?} fn
     * @return {?}
     */
    NzCalendarComponent.prototype.registerOnTouched = /**
     * @param {?} fn
     * @return {?}
     */
    function (fn) {
        this.onTouchFn = fn;
    };
    /**
     * @private
     * @param {?} date
     * @param {?=} touched
     * @return {?}
     */
    NzCalendarComponent.prototype.updateDate = /**
     * @private
     * @param {?} date
     * @param {?=} touched
     * @return {?}
     */
    function (date, touched) {
        if (touched === void 0) { touched = true; }
        /** @type {?} */
        var dayChanged = !isSameDay(date, this.activeDate);
        /** @type {?} */
        var monthChanged = !isSameMonth(date, this.activeDate);
        /** @type {?} */
        var yearChanged = !isSameYear(date, this.activeDate);
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
    };
    /**
     * @private
     * @return {?}
     */
    NzCalendarComponent.prototype.setUpDaysInWeek = /**
     * @private
     * @return {?}
     */
    function () {
        this.daysInWeek = [];
        /** @type {?} */
        var weekStart = startOfWeek(this.activeDate, { weekStartsOn: this.dateHelper.getFirstDayOfWeek() });
        for (var i = 0; i < 7; i++) {
            /** @type {?} */
            var date = addDays(weekStart, i);
            /** @type {?} */
            var title = this.dateHelper.format(date, this.dateHelper.relyOnDatePipe ? 'E' : 'ddd');
            /** @type {?} */
            var label = this.dateHelper.format(date, this.dateHelper.relyOnDatePipe ? 'EEEEEE' : 'dd');
            this.daysInWeek.push({ title: title, label: label });
        }
    };
    /**
     * @private
     * @return {?}
     */
    NzCalendarComponent.prototype.setUpMonthsInYear = /**
     * @private
     * @return {?}
     */
    function () {
        this.monthsInYear = [];
        for (var i = 0; i < 12; i++) {
            /** @type {?} */
            var date = setMonth(this.activeDate, i);
            /** @type {?} */
            var title = this.dateHelper.format(date, 'MMM');
            /** @type {?} */
            var label = this.dateHelper.format(date, 'MMM');
            /** @type {?} */
            var start = startOfMonth(date);
            this.monthsInYear.push({ title: title, label: label, start: start });
        }
    };
    /**
     * @private
     * @return {?}
     */
    NzCalendarComponent.prototype.setUpDateMatrix = /**
     * @private
     * @return {?}
     */
    function () {
        this.dateMatrix = [];
        /** @type {?} */
        var monthStart = startOfMonth(this.activeDate);
        /** @type {?} */
        var monthEnd = endOfMonth(this.activeDate);
        /** @type {?} */
        var weekDiff = differenceInCalendarWeeks(monthEnd, monthStart, { weekStartsOn: this.dateHelper.getFirstDayOfWeek() }) + 2;
        for (var week = 0; week < weekDiff; week++) {
            /** @type {?} */
            var row = [];
            /** @type {?} */
            var weekStart = addDays(this.calendarStart, week * 7);
            for (var day = 0; day < 7; day++) {
                /** @type {?} */
                var date = addDays(weekStart, day);
                /** @type {?} */
                var monthDiff = differenceInCalendarMonths(date, this.activeDate);
                /** @type {?} */
                var dateFormat = this.dateHelper.relyOnDatePipe
                    ? 'longDate'
                    : this.i18n.getLocaleData('DatePicker.lang.dateFormat', 'YYYY-MM-DD');
                /** @type {?} */
                var title = this.dateHelper.format(date, dateFormat);
                /** @type {?} */
                var label = this.dateHelper.format(date, this.dateHelper.relyOnDatePipe ? 'dd' : 'DD');
                /** @type {?} */
                var rel = monthDiff === 0 ? 'current' : monthDiff < 0 ? 'last' : 'next';
                row.push({ title: title, label: label, rel: rel, value: date });
            }
            this.dateMatrix.push(row);
        }
    };
    /**
     * @private
     * @return {?}
     */
    NzCalendarComponent.prototype.calculateCurrentDate = /**
     * @private
     * @return {?}
     */
    function () {
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
    };
    /**
     * @private
     * @return {?}
     */
    NzCalendarComponent.prototype.calculateActiveDate = /**
     * @private
     * @return {?}
     */
    function () {
        this.activeDateRow = differenceInCalendarWeeks(this.activeDate, this.calendarStart, {
            weekStartsOn: this.dateHelper.getFirstDayOfWeek()
        });
        this.activeDateCol = differenceInCalendarDays(this.activeDate, addDays(this.calendarStart, this.activeDateRow * 7));
    };
    /**
     * @private
     * @return {?}
     */
    NzCalendarComponent.prototype.calculateCurrentMonth = /**
     * @private
     * @return {?}
     */
    function () {
        if (isThisYear(this.activeDate)) {
            /** @type {?} */
            var yearStart = startOfYear(this.currentDate);
            /** @type {?} */
            var monthDiff = differenceInCalendarMonths(this.currentDate, yearStart);
            this.currentMonthRow = Math.floor(monthDiff / 3);
            this.currentMonthCol = monthDiff % 3;
        }
        else {
            this.currentMonthRow = -1;
            this.currentMonthCol = -1;
        }
    };
    /**
     * @private
     * @return {?}
     */
    NzCalendarComponent.prototype.calculateActiveMonth = /**
     * @private
     * @return {?}
     */
    function () {
        this.activeMonthRow = Math.floor(this.activeDate.getMonth() / 3);
        this.activeMonthCol = this.activeDate.getMonth() % 3;
    };
    NzCalendarComponent.decorators = [
        { type: Component, args: [{
                    encapsulation: ViewEncapsulation.None,
                    changeDetection: ChangeDetectionStrategy.OnPush,
                    selector: 'nz-calendar',
                    template: "<nz-calendar-header [fullscreen]=\"fullscreen\" [activeDate]=\"activeDate\"\n                    [(mode)]=\"nzMode\" (modeChange)=\"onModeChange($event)\"\n                    (yearChange)=\"onYearSelect($event)\" (monthChange)=\"onMonthSelect($event)\">\n</nz-calendar-header>\n\n<div class=\"ant-fullcalendar ant-fullcalendar-full\" [ngClass]=\"fullscreen ? 'ant-fullcalendar-fullscreen' : ''\">\n  <div class=\"ant-fullcalendar-calendar-body\">\n    <ng-container *ngIf=\"nzMode === 'month' then monthModeTable else yearModeTable\"></ng-container>\n  </div>\n</div>\n\n<ng-template #monthModeTable>\n  <table class=\"ant-fullcalendar-table\" cellspacing=\"0\" role=\"grid\">\n    <thead>\n      <tr role=\"row\">\n        <th *ngFor=\"let day of daysInWeek\" class=\"ant-fullcalendar-column-header\" role=\"columnheader\" [title]=\"day.title\">\n          <span class=\"ant-fullcalendar-column-header-inner\">{{ day.label }}</span>\n        </th>\n      </tr>\n    </thead>\n    <tbody class=\"ant-fullcalendar-tbody\">\n      <tr *ngFor=\"let week of dateMatrix; index as row\"\n          [class.ant-fullcalendar-current-week]=\"row === currentDateRow\"\n          [class.ant-fullcalendar-active-week]=\"row === activeDateRow\">\n        <td *ngFor=\"let day of week; index as col\" role=\"gridcell\" class=\"ant-fullcalendar-cell\" [title]=\"day.title\"\n            [class.ant-fullcalendar-today]=\"row === currentDateRow && col === currentDateCol\"\n            [class.ant-fullcalendar-selected-day]=\"row === activeDateRow && col === activeDateCol\"\n            [class.ant-fullcalendar-last-month-cell]=\"day.rel === 'last'\"\n            [class.ant-fullcalendar-next-month-btn-day]=\"day.rel === 'next'\"\n            (click)=\"onDateSelect(day.value)\">\n            <div class=\"ant-fullcalendar-date\">\n              <ng-container *ngIf=\"dateFullCell else defaultCell\">\n                <ng-container *ngTemplateOutlet=\"dateFullCell; context: {$implicit: day.value}\"></ng-container>\n              </ng-container>\n              <ng-template #defaultCell>\n                <div class=\"ant-fullcalendar-value\">{{ day.label }}</div>\n                <div *ngIf=\"dateCell\" class=\"ant-fullcalendar-content\">\n                  <ng-container *ngTemplateOutlet=\"dateCell; context: {$implicit: day.value}\"></ng-container>\n                </div>\n              </ng-template>\n            </div>\n        </td>\n      </tr>\n    </tbody>\n  </table>\n</ng-template>\n\n<ng-template #yearModeTable>\n  <table class=\"ant-fullcalendar-month-panel-table\" cellspacing=\"0\" role=\"grid\">\n    <tbody class=\"ant-fullcalendar-month-panel-tbody\">\n      <tr *ngFor=\"let row of [0, 1, 2, 3]\" role=\"row\">\n        <td *ngFor=\"let col of [0, 1, 2]\" role=\"gridcell\" [title]=\"monthsInYear[row * 3 + col].title\"\n            class=\"ant-fullcalendar-month-panel-cell\"\n            [class.ant-fullcalendar-month-panel-current-cell]=\"row === currentMonthRow && col === currentMonthCol\"\n            [class.ant-fullcalendar-month-panel-selected-cell]=\"row === activeMonthRow && col === activeMonthCol\"\n            (click)=\"onMonthSelect(row * 3 + col)\">\n          <div class=\"ant-fullcalendar-month\">\n            <ng-container *ngIf=\"monthFullCell else defaultCell\">\n              <ng-container *ngTemplateOutlet=\"monthFullCell; context: {$implicit: monthsInYear[row * 3 + col].start}\"></ng-container>\n            </ng-container>\n            <ng-template #defaultCell>\n              <div class=\"ant-fullcalendar-value\">{{ monthsInYear[row * 3 + col].label }}</div>\n              <div *ngIf=\"monthCell\" class=\"ant-fullcalendar-content\">\n                <ng-container *ngTemplateOutlet=\"monthCell; context: {$implicit: monthsInYear[row * 3 + col].start}\"></ng-container>\n              </div>\n            </ng-template>\n          </div>\n        </td>\n      </tr>\n    </tbody>\n  </table>\n</ng-template>\n",
                    providers: [{ provide: NG_VALUE_ACCESSOR, useExisting: forwardRef((/**
                             * @return {?}
                             */
                            function () { return NzCalendarComponent; })), multi: true }]
                }] }
    ];
    /** @nocollapse */
    NzCalendarComponent.ctorParameters = function () { return [
        { type: NzI18nService },
        { type: ChangeDetectorRef },
        { type: DateHelperService }
    ]; };
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
    return NzCalendarComponent;
}());
export { NzCalendarComponent };
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
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotY2FsZW5kYXIuY29tcG9uZW50LmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbImNhbGVuZGFyL256LWNhbGVuZGFyLmNvbXBvbmVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7O0FBQUEsT0FBTyxFQUFFLHFCQUFxQixFQUFFLE1BQU0sdUJBQXVCLENBQUM7QUFDOUQsT0FBTyxFQUNMLFVBQVUsRUFDVix1QkFBdUIsRUFDdkIsaUJBQWlCLEVBQ2pCLFNBQVMsRUFDVCxZQUFZLEVBQ1osWUFBWSxFQUNaLFdBQVcsRUFDWCxLQUFLLEVBRUwsTUFBTSxFQUNOLFdBQVcsRUFDWCxpQkFBaUIsRUFDbEIsTUFBTSxlQUFlLENBQUM7QUFDdkIsT0FBTyxFQUF3QixpQkFBaUIsRUFBRSxNQUFNLGdCQUFnQixDQUFDO0FBQ3pFLE9BQU8sT0FBTyxNQUFNLG1CQUFtQixDQUFDO0FBQ3hDLE9BQU8sd0JBQXdCLE1BQU0sc0NBQXNDLENBQUM7QUFDNUUsT0FBTywwQkFBMEIsTUFBTSx3Q0FBd0MsQ0FBQztBQUNoRixPQUFPLHlCQUF5QixNQUFNLHVDQUF1QyxDQUFDO0FBQzlFLE9BQU8sVUFBVSxNQUFNLHVCQUF1QixDQUFDO0FBQy9DLE9BQU8sU0FBUyxNQUFNLHNCQUFzQixDQUFDO0FBQzdDLE9BQU8sV0FBVyxNQUFNLHdCQUF3QixDQUFDO0FBQ2pELE9BQU8sVUFBVSxNQUFNLHVCQUF1QixDQUFDO0FBQy9DLE9BQU8sV0FBVyxNQUFNLHdCQUF3QixDQUFDO0FBQ2pELE9BQU8sVUFBVSxNQUFNLHVCQUF1QixDQUFDO0FBQy9DLE9BQU8sUUFBUSxNQUFNLG9CQUFvQixDQUFDO0FBQzFDLE9BQU8sT0FBTyxNQUFNLG1CQUFtQixDQUFDO0FBQ3hDLE9BQU8sWUFBWSxNQUFNLHlCQUF5QixDQUFDO0FBQ25ELE9BQU8sV0FBVyxNQUFNLHdCQUF3QixDQUFDO0FBQ2pELE9BQU8sV0FBVyxNQUFNLHdCQUF3QixDQUFDO0FB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Ozs7SUFDOUIscUNBQW9DOztJQUNwQywyQ0FBNkU7O0lBQzdFLDRDQUFvRzs7SUFFcEcsNkNBQTJFOztJQUszRSw0Q0FBMEU7O0lBa0UxRSx5Q0FDa0I7O0lBRWxCLHlDQUFrQzs7SUFDbEMsMkNBQXNDOztJQUN0Qyx5Q0FBcUM7O0lBQ3JDLHlDQUE4Qjs7SUFDOUIsNkNBQTRCOztJQUM1Qiw2Q0FBNEI7O0lBQzVCLDRDQUEyQjs7SUFDM0IsNENBQTJCOztJQUMzQiw4Q0FBNkI7O0lBQzdCLDhDQUE2Qjs7SUFDN0IsNkNBQTRCOztJQUM1Qiw2Q0FBNEI7O0lBQzVCLHVDQUF5RDs7SUFDekQsMkNBQTZEOztJQUM3RCx3Q0FBMEQ7O0lBQzFELDRDQUE4RDs7Ozs7SUFFOUQsMENBQWlDOzs7OztJQUNqQyx5Q0FBb0Q7Ozs7O0lBQ3BELHdDQUF5Qzs7Ozs7SUFNN0IsbUNBQTJCOzs7OztJQUFFLGtDQUE4Qjs7Ozs7SUFBRSx5Q0FBcUM7Ozs7O0FBaUtoSCxvQ0FHQzs7O0lBRkMsK0JBQWM7O0lBQ2QsK0JBQWM7Ozs7O0FBR2hCLHNDQUlDOzs7SUFIQyxpQ0FBYzs7SUFDZCxpQ0FBYzs7SUFDZCxpQ0FBWTs7Ozs7QUFHZCxxQ0FLQzs7O0lBSkMsZ0NBQWM7O0lBQ2QsZ0NBQWM7O0lBQ2QsOEJBQWlDOztJQUNqQyxnQ0FBWSIsInNvdXJjZXNDb250ZW50IjpbImltcG9ydCB7IGNvZXJjZUJvb2xlYW5Qcm9wZXJ0eSB9IGZyb20gJ0Bhbmd1bGFyL2Nkay9jb2VyY2lvbic7XG5pbXBvcnQge1xuICBmb3J3YXJkUmVmLFxuICBDaGFuZ2VEZXRlY3Rpb25TdHJhdGVneSxcbiAgQ2hhbmdlRGV0ZWN0b3JSZWYsXG4gIENvbXBvbmVudCxcbiAgQ29udGVudENoaWxkLFxuICBFdmVudEVtaXR0ZXIsXG4gIEhvc3RCaW5kaW5nLFxuICBJbnB1dCxcbiAgT25Jbml0LFxuICBPdXRwdXQsXG4gIFRlbXBsYXRlUmVmLFxuICBWaWV3RW5jYXBzdWxhdGlvblxufSBmcm9tICdAYW5ndWxhci9jb3JlJztcbmltcG9ydCB7IENvbnRyb2xWYWx1ZUFjY2Vzc29yLCBOR19WQUxVRV9BQ0NFU1NPUiB9IGZyb20gJ0Bhbmd1bGFyL2Zvcm1zJztcbmltcG9ydCBhZGREYXlzIGZyb20gJ2RhdGUtZm5zL2FkZF9kYXlzJztcbmltcG9ydCBkaWZmZXJlbmNlSW5DYWxlbmRhckRheXMgZnJvbSAnZGF0ZS1mbnMvZGlmZmVyZW5jZV9pbl9jYWxlbmRhcl9kYXlzJztcbmltcG9ydCBkaWZmZXJlbmNlSW5DYWxlbmRhck1vbnRocyBmcm9tICdkYXRlLWZucy9kaWZmZXJlbmNlX2luX2NhbGVuZGFyX21vbnRocyc7XG5pbXBvcnQgZGlmZmVyZW5jZUluQ2FsZW5kYXJXZWVrcyBmcm9tICdkYXRlLWZucy9kaWZmZXJlbmNlX2luX2NhbGVuZGFyX3dlZWtzJztcbmltcG9ydCBlbmRPZk1vbnRoIGZyb20gJ2RhdGUtZm5zL2VuZF9vZl9tb250aCc7XG5pbXBvcnQgaXNTYW1lRGF5IGZyb20gJ2RhdGUtZm5zL2lzX3NhbWVfZGF5JztcbmltcG9ydCBpc1NhbWVNb250aCBmcm9tICdkYXRlLWZucy9pc19zYW1lX21vbnRoJztcbmltcG9ydCBpc1NhbWVZZWFyIGZyb20gJ2RhdGUtZm5zL2lzX3NhbWVfeWVhcic7XG5pbXBvcnQgaXNUaGlzTW9udGggZnJvbSAnZGF0ZS1mbnMvaXNfdGhpc19tb250aCc7XG5pbXBvcnQgaXNUaGlzWWVhciBmcm9tICdkYXRlLWZucy9pc190aGlzX3llYXInO1xuaW1wb3J0IHNldE1vbnRoIGZyb20gJ2RhdGUtZm5zL3NldF9tb250aCc7XG5pbXBvcnQgc2V0WWVhciBmcm9tICdkYXRlLWZucy9zZXRfeWVhcic7XG5pbXBvcnQgc3RhcnRPZk1vbnRoIGZyb20gJ2RhdGUtZm5zL3N0YXJ0X29mX21vbnRoJztcbmltcG9ydCBzdGFydE9mV2VlayBmcm9tICdkYXRlLWZucy9zdGFydF9vZl93ZWVrJztcbmltcG9ydCBzdGFydE9mWWVhciBmcm9tICdkYXRlLWZucy9zdGFydF9vZl95ZWFyJztcbmltcG9ydCB7IERhdGVIZWxwZXJTZXJ2aWNlIH0gZnJvbSAnLi4vaTE4bi9kYXRlLWhlbHBlci5zZXJ2aWNlJztcbmltcG9ydCB7IE56STE4blNlcnZpY2UgfSBmcm9tICcuLi9pMThuL256LWkxOG4uc2VydmljZSc7XG5pbXBvcnQge1xuICBOekRhdGVDZWxsRGlyZWN0aXZlIGFzIERhdGVDZWxsLFxuICBOekRhdGVGdWxsQ2VsbERpcmVjdGl2ZSBhcyBEYXRlRnVsbENlbGwsXG4gIE56TW9udGhDZWxsRGlyZWN0aXZlIGFzIE1vbnRoQ2VsbCxcbiAgTnpNb250aEZ1bGxDZWxsRGlyZWN0aXZlIGFzIE1vbnRoRnVsbENlbGxcbn0gZnJvbSAnLi9uei1jYWxlbmRhci1jZWxscyc7XG5cbmV4cG9ydCB0eXBlIE1vZGVUeXBlID0gJ21vbnRoJyB8ICd5ZWFyJztcblxuQENvbXBvbmVudCh7XG4gIGVuY2Fwc3VsYXRpb246IFZpZXdFbmNhcHN1bGF0aW9uLk5vbmUsXG4gIGNoYW5nZURldGVjdGlvbjogQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3kuT25QdXNoLFxuICBzZWxlY3RvcjogJ256LWNhbGVuZGFyJyxcbiAgdGVtcGxhdGVVcmw6ICcuL256LWNhbGVuZGFyLmNvbXBvbmVudC5odG1sJyxcbiAgcHJvdmlkZXJzOiBbeyBwcm92aWRlOiBOR19WQUxVRV9BQ0NFU1NPUiwgdXNlRXhpc3Rpbmc6IGZvcndhcmRSZWYoKCkgPT4gTnpDYWxlbmRhckNvbXBvbmVudCksIG11bHRpOiB0cnVlIH1dXG59KVxuZXhwb3J0IGNsYXNzIE56Q2FsZW5kYXJDb21wb25lbnQgaW1wbGVtZW50cyBDb250cm9sVmFsdWVBY2Nlc3NvciwgT25Jbml0IHtcbiAgQElucHV0KCkgbnpNb2RlOiBNb2RlVHlwZSA9ICdtb250aCc7XG4gIEBPdXRwdXQoKSByZWFkb25seSBuek1vZGVDaGFuZ2U6IEV2ZW50RW1pdHRlcjxNb2RlVHlwZT4gPSBuZXcgRXZlbnRFbWl0dGVyKCk7XG4gIEBPdXRwdXQoKSByZWFkb25seSBuelBhbmVsQ2hhbmdlOiBFdmVudEVtaXR0ZXI8eyBkYXRlOiBEYXRlOyBtb2RlOiBNb2RlVHlwZSB9PiA9IG5ldyBFdmVudEVtaXR0ZXIoKTtcblxuICBAT3V0cHV0KCkgcmVhZG9ubHkgbnpTZWxlY3RDaGFuZ2U6IEV2ZW50RW1pdHRlcjxEYXRlPiA9IG5ldyBFdmVudEVtaXR0ZXIoKTtcblxuICBASW5wdXQoKSBzZXQgbnpWYWx1ZSh2YWx1ZTogRGF0ZSkge1xuICAgIHRoaXMudXBkYXRlRGF0ZSh2YWx1ZSwgZmFsc2UpO1xuICB9XG4gIEBPdXRwdXQoKSByZWFkb25seSBuelZhbHVlQ2hhbmdlOiBFdmVudEVtaXR0ZXI8RGF0ZT4gPSBuZXcgRXZlbnRFbWl0dGVyKCk7XG5cbiAgQElucHV0KClcbiAgc2V0IG56RGF0ZUNlbGwodmFsdWU6IFRlbXBsYXRlUmVmPHsgJGltcGxpY2l0OiBEYXRlIH0+KSB7XG4gICAgdGhpcy5kYXRlQ2VsbCA9IHZhbHVlO1xuICB9XG5cbiAgQElucHV0KClcbiAgc2V0IG56RGF0ZUZ1bGxDZWxsKHZhbHVlOiBUZW1wbGF0ZVJlZjx7ICRpbXBsaWNpdDogRGF0ZSB9Pikge1xuICAgIHRoaXMuZGF0ZUZ1bGxDZWxsID0gdmFsdWU7XG4gIH1cblxuICBASW5wdXQoKVxuICBzZXQgbnpNb250aENlbGwodmFsdWU6IFRlbXBsYXRlUmVmPHsgJGltcGxpY2l0OiBEYXRlIH0+KSB7XG4gICAgdGhpcy5tb250aENlbGwgPSB2YWx1ZTtcbiAgfVxuXG4gIEBJbnB1dCgpXG4gIHNldCBuek1vbnRoRnVsbENlbGwodmFsdWU6IFRlbXBsYXRlUmVmPHsgJGltcGxpY2l0OiBEYXRlIH0+KSB7XG4gICAgdGhpcy5tb250aEZ1bGxDZWxsID0gdmFsdWU7XG4gIH1cblxuICBASW5wdXQoKVxuICBzZXQgbnpGdWxsc2NyZWVuKHZhbHVlOiBib29sZWFuKSB7XG4gICAgdGhpcy5mdWxsc2NyZWVuID0gY29lcmNlQm9vbGVhblByb3BlcnR5KHZhbHVlKTtcbiAgfVxuICBnZXQgbnpGdWxsc2NyZWVuKCk6IGJvb2xlYW4ge1xuICAgIHJldHVybiB0aGlzLmZ1bGxzY3JlZW47XG4gIH1cblxuICBASW5wdXQoKVxuICBzZXQgbnpDYXJkKHZhbHVlOiBib29sZWFuKSB7XG4gICAgdGhpcy5mdWxsc2NyZWVuID0gIWNvZXJjZUJvb2xlYW5Qcm9wZXJ0eSh2YWx1ZSk7XG4gIH1cbiAgZ2V0IG56Q2FyZCgpOiBib29sZWFuIHtcbiAgICByZXR1cm4gIXRoaXMuZnVsbHNjcmVlbjtcbiAgfVxuXG4gIEBDb250ZW50Q2hpbGQoRGF0ZUNlbGwsIHsgcmVhZDogVGVtcGxhdGVSZWYgfSlcbiAgc2V0IGRhdGVDZWxsQ2hpbGQodmFsdWU6IFRlbXBsYXRlUmVmPHsgJGltcGxpY2l0OiBEYXRlIH0+KSB7XG4gICAgaWYgKHZhbHVlKSB7XG4gICAgICB0aGlzLmRhdGVDZWxsID0gdmFsdWU7XG4gICAgfVxuICB9XG5cbiAgQENvbnRlbnRDaGlsZChEYXRlRnVsbENlbGwsIHsgcmVhZDogVGVtcGxhdGVSZWYgfSlcbiAgc2V0IGRhdGVGdWxsQ2VsbENoaWxkKHZhbHVlOiBUZW1wbGF0ZVJlZjx7ICRpbXBsaWNpdDogRGF0ZSB9Pikge1xuICAgIGlmICh2YWx1ZSkge1xuICAgICAgdGhpcy5kYXRlRnVsbENlbGwgPSB2YWx1ZTtcbiAgICB9XG4gIH1cblxuICBAQ29udGVudENoaWxkKE1vbnRoQ2VsbCwgeyByZWFkOiBUZW1wbGF0ZVJlZiB9KVxuICBzZXQgbW9udGhDZWxsQ2hpbGQodmFsdWU6IFRlbXBsYXRlUmVmPHsgJGltcGxpY2l0OiBEYXRlIH0+KSB7XG4gICAgaWYgKHZhbHVlKSB7XG4gICAgICB0aGlzLm1vbnRoQ2VsbCA9IHZhbHVlO1xuICAgIH1cbiAgfVxuXG4gIEBDb250ZW50Q2hpbGQoTW9udGhGdWxsQ2VsbCwgeyByZWFkOiBUZW1wbGF0ZVJlZiB9KVxuICBzZXQgbW9udGhGdWxsQ2VsbENoaWxkKHZhbHVlOiBUZW1wbGF0ZVJlZjx7ICRpbXBsaWNpdDogRGF0ZSB9Pikge1xuICAgIGlmICh2YWx1ZSkge1xuICAgICAgdGhpcy5tb250aEZ1bGxDZWxsID0gdmFsdWU7XG4gICAgfVxuICB9XG5cbiAgQEhvc3RCaW5kaW5nKCdjbGFzcy5hbnQtZnVsbGNhbGVuZGFyLS1mdWxsc2NyZWVuJylcbiAgZnVsbHNjcmVlbiA9IHRydWU7XG5cbiAgZGF5c0luV2VlazogRGF5Q2VsbENvbnRleHRbXSA9IFtdO1xuICBtb250aHNJblllYXI6IE1vbnRoQ2VsbENvbnRleHRbXSA9IFtdO1xuICBkYXRlTWF0cml4OiBEYXRlQ2VsbENvbnRleHRbXVtdID0gW107XG4gIGFjdGl2ZURhdGU6IERhdGUgPSBuZXcgRGF0ZSgpO1xuICBjdXJyZW50RGF0ZVJvdzogbnVtYmVyID0gLTE7XG4gIGN1cnJlbnREYXRlQ29sOiBudW1iZXIgPSAtMTtcbiAgYWN0aXZlRGF0ZVJvdzogbnVtYmVyID0gLTE7XG4gIGFjdGl2ZURhdGVDb2w6IG51bWJlciA9IC0xO1xuICBjdXJyZW50TW9udGhSb3c6IG51bWJlciA9IC0xO1xuICBjdXJyZW50TW9udGhDb2w6IG51bWJlciA9IC0xO1xuICBhY3RpdmVNb250aFJvdzogbnVtYmVyID0gLTE7XG4gIGFjdGl2ZU1vbnRoQ29sOiBudW1iZXIgPSAtMTtcbiAgZGF0ZUNlbGw6IFRlbXBsYXRlUmVmPHsgJGltcGxpY2l0OiBEYXRlIH0+IHwgbnVsbCA9IG51bGw7XG4gIGRhdGVGdWxsQ2VsbDogVGVtcGxhdGVSZWY8eyAkaW1wbGljaXQ6IERhdGUgfT4gfCBudWxsID0gbnVsbDtcbiAgbW9udGhDZWxsOiBUZW1wbGF0ZVJlZjx7ICRpbXBsaWNpdDogRGF0ZSB9PiB8IG51bGwgPSBudWxsO1xuICBtb250aEZ1bGxDZWxsOiBUZW1wbGF0ZVJlZjx7ICRpbXBsaWNpdDogRGF0ZSB9PiB8IG51bGwgPSBudWxsO1xuXG4gIHByaXZhdGUgY3VycmVudERhdGUgPSBuZXcgRGF0ZSgpO1xuICBwcml2YXRlIG9uQ2hhbmdlRm46IChkYXRlOiBEYXRlKSA9PiB2b2lkID0gKCkgPT4ge307XG4gIHByaXZhdGUgb25Ub3VjaEZuOiAoKSA9PiB2b2lkID0gKCkgPT4ge307XG5cbiAgcHJpdmF0ZSBnZXQgY2FsZW5kYXJTdGFydCgpOiBEYXRlIHtcbiAgICByZXR1cm4gc3RhcnRPZldlZWsoc3RhcnRPZk1vbnRoKHRoaXMuYWN0aXZlRGF0ZSksIHsgd2Vla1N0YXJ0c09uOiB0aGlzLmRhdGVIZWxwZXIuZ2V0Rmlyc3REYXlPZldlZWsoKSB9KTtcbiAgfVxuXG4gIGNvbnN0cnVjdG9yKHByaXZhdGUgaTE4bjogTnpJMThuU2VydmljZSwgcHJpdmF0ZSBjZHI6IENoYW5nZURldGVjdG9yUmVmLCBwcml2YXRlIGRhdGVIZWxwZXI6IERhdGVIZWxwZXJTZXJ2aWNlKSB7fVxuXG4gIG5nT25Jbml0KCk6IHZvaWQge1xuICAgIHRoaXMuc2V0VXBEYXlzSW5XZWVrKCk7XG4gICAgdGhpcy5zZXRVcE1vbnRoc0luWWVhcigpO1xuICAgIHRoaXMuc2V0VXBEYXRlTWF0cml4KCk7XG4gICAgdGhpcy5jYWxjdWxhdGVDdXJyZW50RGF0ZSgpO1xuICAgIHRoaXMuY2FsY3VsYXRlQWN0aXZlRGF0ZSgpO1xuICAgIHRoaXMuY2FsY3VsYXRlQ3VycmVudE1vbnRoKCk7XG4gICAgdGhpcy5jYWxjdWxhdGVBY3RpdmVNb250aCgpO1xuICB9XG5cbiAgb25Nb2RlQ2hhbmdlKG1vZGU6IE1vZGVUeXBlKTogdm9pZCB7XG4gICAgdGhpcy5uek1vZGVDaGFuZ2UuZW1pdChtb2RlKTtcbiAgICB0aGlzLm56UGFuZWxDaGFuZ2UuZW1pdCh7IGRhdGU6IHRoaXMuYWN0aXZlRGF0ZSwgbW9kZSB9KTtcbiAgfVxuXG4gIG9uRGF0ZVNlbGVjdChkYXRlOiBEYXRlKTogdm9pZCB7XG4gICAgdGhpcy51cGRhdGVEYXRlKGRhdGUpO1xuICAgIHRoaXMubnpTZWxlY3RDaGFuZ2UuZW1pdChkYXRlKTtcbiAgfVxuXG4gIG9uWWVhclNlbGVjdCh5ZWFyOiBudW1iZXIpOiB2b2lkIHtcbiAgICBjb25zdCBkYXRlID0gc2V0WWVhcih0aGlzLmFjdGl2ZURhdGUsIHllYXIpO1xuICAgIHRoaXMudXBkYXRlRGF0ZShkYXRlKTtcbiAgICB0aGlzLm56U2VsZWN0Q2hhbmdlLmVtaXQoZGF0ZSk7XG4gIH1cblxuICBvbk1vbnRoU2VsZWN0KG1vbnRoOiBudW1iZXIpOiB2b2lkIHtcbiAgICBjb25zdCBkYXRlID0gc2V0TW9udGgodGhpcy5hY3RpdmVEYXRlLCBtb250aCk7XG4gICAgdGhpcy51cGRhdGVEYXRlKGRhdGUpO1xuICAgIHRoaXMubnpTZWxlY3RDaGFuZ2UuZW1pdChkYXRlKTtcbiAgfVxuXG4gIHdyaXRlVmFsdWUodmFsdWU6IERhdGUgfCBudWxsKTogdm9pZCB7XG4gICAgdGhpcy51cGRhdGVEYXRlKHZhbHVlIHx8IG5ldyBEYXRlKCksIGZhbHNlKTtcbiAgICB0aGlzLmNkci5tYXJrRm9yQ2hlY2soKTtcbiAgfVxuXG4gIHJlZ2lzdGVyT25DaGFuZ2UoZm46IChkYXRlOiBEYXRlKSA9PiB2b2lkKTogdm9pZCB7XG4gICAgdGhpcy5vbkNoYW5nZUZuID0gZm47XG4gIH1cblxuICByZWdpc3Rlck9uVG91Y2hlZChmbjogKCkgPT4gdm9pZCk6IHZvaWQge1xuICAgIHRoaXMub25Ub3VjaEZuID0gZm47XG4gIH1cblxuICBwcml2YXRlIHVwZGF0ZURhdGUoZGF0ZTogRGF0ZSwgdG91Y2hlZDogYm9vbGVhbiA9IHRydWUpOiB2b2lkIHtcbiAgICBjb25zdCBkYXlDaGFuZ2VkID0gIWlzU2FtZURheShkYXRlLCB0aGlzLmFjdGl2ZURhdGUpO1xuICAgIGNvbnN0IG1vbnRoQ2hhbmdlZCA9ICFpc1NhbWVNb250aChkYXRlLCB0aGlzLmFjdGl2ZURhdGUpO1xuICAgIGNvbnN0IHllYXJDaGFuZ2VkID0gIWlzU2FtZVllYXIoZGF0ZSwgdGhpcy5hY3RpdmVEYXRlKTtcblxuICAgIHRoaXMuYWN0aXZlRGF0ZSA9IGRhdGU7XG5cbiAgICBpZiAoZGF5Q2hhbmdlZCkge1xuICAgICAgdGhpcy5jYWxjdWxhdGVBY3RpdmVEYXRlKCk7XG4gICAgfVxuICAgIGlmIChtb250aENoYW5nZWQpIHtcbiAgICAgIHRoaXMuc2V0VXBEYXRlTWF0cml4KCk7XG4gICAgICB0aGlzLmNhbGN1bGF0ZUN1cnJlbnREYXRlKCk7XG4gICAgICB0aGlzLmNhbGN1bGF0ZUFjdGl2ZU1vbnRoKCk7XG4gICAgfVxuICAgIGlmICh5ZWFyQ2hhbmdlZCkge1xuICAgICAgdGhpcy5jYWxjdWxhdGVDdXJyZW50TW9udGgoKTtcbiAgICB9XG5cbiAgICBpZiAodG91Y2hlZCkge1xuICAgICAgdGhpcy5vbkNoYW5nZUZuKGRhdGUpO1xuICAgICAgdGhpcy5vblRvdWNoRm4oKTtcbiAgICAgIHRoaXMubnpWYWx1ZUNoYW5nZS5lbWl0KGRhdGUpO1xuICAgIH1cbiAgfVxuXG4gIHByaXZhdGUgc2V0VXBEYXlzSW5XZWVrKCk6IHZvaWQge1xuICAgIHRoaXMuZGF5c0luV2VlayA9IFtdO1xuICAgIGNvbnN0IHdlZWtTdGFydCA9IHN0YXJ0T2ZXZWVrKHRoaXMuYWN0aXZlRGF0ZSwgeyB3ZWVrU3RhcnRzT246IHRoaXMuZGF0ZUhlbHBlci5nZXRGaXJzdERheU9mV2VlaygpIH0pO1xuICAgIGZvciAobGV0IGkgPSAwOyBpIDwgNzsgaSsrKSB7XG4gICAgICBjb25zdCBkYXRlID0gYWRkRGF5cyh3ZWVrU3RhcnQsIGkpO1xuICAgICAgY29uc3QgdGl0bGUgPSB0aGlzLmRhdGVIZWxwZXIuZm9ybWF0KGRhdGUsIHRoaXMuZGF0ZUhlbHBlci5yZWx5T25EYXRlUGlwZSA/ICdFJyA6ICdkZGQnKTtcbiAgICAgIGNvbnN0IGxhYmVsID0gdGhpcy5kYXRlSGVscGVyLmZvcm1hdChkYXRlLCB0aGlzLmRhdGVIZWxwZXIucmVseU9uRGF0ZVBpcGUgPyAnRUVFRUVFJyA6ICdkZCcpO1xuICAgICAgdGhpcy5kYXlzSW5XZWVrLnB1c2goeyB0aXRsZSwgbGFiZWwgfSk7XG4gICAgfVxuICB9XG5cbiAgcHJpdmF0ZSBzZXRVcE1vbnRoc0luWWVhcigpOiB2b2lkIHtcbiAgICB0aGlzLm1vbnRoc0luWWVhciA9IFtdO1xuICAgIGZvciAobGV0IGkgPSAwOyBpIDwgMTI7IGkrKykge1xuICAgICAgY29uc3QgZGF0ZSA9IHNldE1vbnRoKHRoaXMuYWN0aXZlRGF0ZSwgaSk7XG4gICAgICBjb25zdCB0aXRsZSA9IHRoaXMuZGF0ZUhlbHBlci5mb3JtYXQoZGF0ZSwgJ01NTScpO1xuICAgICAgY29uc3QgbGFiZWwgPSB0aGlzLmRhdGVIZWxwZXIuZm9ybWF0KGRhdGUsICdNTU0nKTtcbiAgICAgIGNvbnN0IHN0YXJ0ID0gc3RhcnRPZk1vbnRoKGRhdGUpO1xuICAgICAgdGhpcy5tb250aHNJblllYXIucHVzaCh7IHRpdGxlLCBsYWJlbCwgc3RhcnQgfSk7XG4gICAgfVxuICB9XG5cbiAgcHJpdmF0ZSBzZXRVcERhdGVNYXRyaXgoKTogdm9pZCB7XG4gICAgdGhpcy5kYXRlTWF0cml4ID0gW107XG4gICAgY29uc3QgbW9udGhTdGFydCA9IHN0YXJ0T2ZNb250aCh0aGlzLmFjdGl2ZURhdGUpO1xuICAgIGNvbnN0IG1vbnRoRW5kID0gZW5kT2ZNb250aCh0aGlzLmFjdGl2ZURhdGUpO1xuICAgIGNvbnN0IHdlZWtEaWZmID1cbiAgICAgIGRpZmZlcmVuY2VJbkNhbGVuZGFyV2Vla3MobW9udGhFbmQsIG1vbnRoU3RhcnQsIHsgd2Vla1N0YXJ0c09uOiB0aGlzLmRhdGVIZWxwZXIuZ2V0Rmlyc3REYXlPZldlZWsoKSB9KSArIDI7XG5cbiAgICBmb3IgKGxldCB3ZWVrID0gMDsgd2VlayA8IHdlZWtEaWZmOyB3ZWVrKyspIHtcbiAgICAgIGNvbnN0IHJvdzogRGF0ZUNlbGxDb250ZXh0W10gPSBbXTtcbiAgICAgIGNvbnN0IHdlZWtTdGFydCA9IGFkZERheXModGhpcy5jYWxlbmRhclN0YXJ0LCB3ZWVrICogNyk7XG5cbiAgICAgIGZvciAobGV0IGRheSA9IDA7IGRheSA8IDc7IGRheSsrKSB7XG4gICAgICAgIGNvbnN0IGRhdGUgPSBhZGREYXlzKHdlZWtTdGFydCwgZGF5KTtcbiAgICAgICAgY29uc3QgbW9udGhEaWZmID0gZGlmZmVyZW5jZUluQ2FsZW5kYXJNb250aHMoZGF0ZSwgdGhpcy5hY3RpdmVEYXRlKTtcbiAgICAgICAgY29uc3QgZGF0ZUZvcm1hdCA9IHRoaXMuZGF0ZUhlbHBlci5yZWx5T25EYXRlUGlwZVxuICAgICAgICAgID8gJ2xvbmdEYXRlJ1xuICAgICAgICAgIDogdGhpcy5pMThuLmdldExvY2FsZURhdGEoJ0RhdGVQaWNrZXIubGFuZy5kYXRlRm9ybWF0JywgJ1lZWVktTU0tREQnKTtcbiAgICAgICAgY29uc3QgdGl0bGUgPSB0aGlzLmRhdGVIZWxwZXIuZm9ybWF0KGRhdGUsIGRhdGVGb3JtYXQpO1xuICAgICAgICBjb25zdCBsYWJlbCA9IHRoaXMuZGF0ZUhlbHBlci5mb3JtYXQoZGF0ZSwgdGhpcy5kYXRlSGVscGVyLnJlbHlPbkRhdGVQaXBlID8gJ2RkJyA6ICdERCcpO1xuICAgICAgICBjb25zdCByZWwgPSBtb250aERpZmYgPT09IDAgPyAnY3VycmVudCcgOiBtb250aERpZmYgPCAwID8gJ2xhc3QnIDogJ25leHQnO1xuICAgICAgICByb3cucHVzaCh7IHRpdGxlLCBsYWJlbCwgcmVsLCB2YWx1ZTogZGF0ZSB9KTtcbiAgICAgIH1cbiAgICAgIHRoaXMuZGF0ZU1hdHJpeC5wdXNoKHJvdyk7XG4gICAgfVxuICB9XG5cbiAgcHJpdmF0ZSBjYWxjdWxhdGVDdXJyZW50RGF0ZSgpOiB2b2lkIHtcbiAgICBpZiAoaXNUaGlzTW9udGgodGhpcy5hY3RpdmVEYXRlKSkge1xuICAgICAgdGhpcy5jdXJyZW50RGF0ZVJvdyA9IGRpZmZlcmVuY2VJbkNhbGVuZGFyV2Vla3ModGhpcy5jdXJyZW50RGF0ZSwgdGhpcy5jYWxlbmRhclN0YXJ0LCB7XG4gICAgICAgIHdlZWtTdGFydHNPbjogdGhpcy5kYXRlSGVscGVyLmdldEZpcnN0RGF5T2ZXZWVrKClcbiAgICAgIH0pO1xuICAgICAgdGhpcy5jdXJyZW50RGF0ZUNvbCA9IGRpZmZlcmVuY2VJbkNhbGVuZGFyRGF5cyhcbiAgICAgICAgdGhpcy5jdXJyZW50RGF0ZSxcbiAgICAgICAgYWRkRGF5cyh0aGlzLmNhbGVuZGFyU3RhcnQsIHRoaXMuY3VycmVudERhdGVSb3cgKiA3KVxuICAgICAgKTtcbiAgICB9IGVsc2Uge1xuICAgICAgdGhpcy5jdXJyZW50RGF0ZVJvdyA9IC0xO1xuICAgICAgdGhpcy5jdXJyZW50RGF0ZUNvbCA9IC0xO1xuICAgIH1cbiAgfVxuXG4gIHByaXZhdGUgY2FsY3VsYXRlQWN0aXZlRGF0ZSgpOiB2b2lkIHtcbiAgICB0aGlzLmFjdGl2ZURhdGVSb3cgPSBkaWZmZXJlbmNlSW5DYWxlbmRhcldlZWtzKHRoaXMuYWN0aXZlRGF0ZSwgdGhpcy5jYWxlbmRhclN0YXJ0LCB7XG4gICAgICB3ZWVrU3RhcnRzT246IHRoaXMuZGF0ZUhlbHBlci5nZXRGaXJzdERheU9mV2VlaygpXG4gICAgfSk7XG4gICAgdGhpcy5hY3RpdmVEYXRlQ29sID0gZGlmZmVyZW5jZUluQ2FsZW5kYXJEYXlzKHRoaXMuYWN0aXZlRGF0ZSwgYWRkRGF5cyh0aGlzLmNhbGVuZGFyU3RhcnQsIHRoaXMuYWN0aXZlRGF0ZVJvdyAqIDcpKTtcbiAgfVxuXG4gIHByaXZhdGUgY2FsY3VsYXRlQ3VycmVudE1vbnRoKCk6IHZvaWQge1xuICAgIGlmIChpc1RoaXNZZWFyKHRoaXMuYWN0aXZlRGF0ZSkpIHtcbiAgICAgIGNvbnN0IHllYXJTdGFydCA9IHN0YXJ0T2ZZZWFyKHRoaXMuY3VycmVudERhdGUpO1xuICAgICAgY29uc3QgbW9udGhEaWZmID0gZGlmZmVyZW5jZUluQ2FsZW5kYXJNb250aHModGhpcy5jdXJyZW50RGF0ZSwgeWVhclN0YXJ0KTtcbiAgICAgIHRoaXMuY3VycmVudE1vbnRoUm93ID0gTWF0aC5mbG9vcihtb250aERpZmYgLyAzKTtcbiAgICAgIHRoaXMuY3VycmVudE1vbnRoQ29sID0gbW9udGhEaWZmICUgMztcbiAgICB9IGVsc2Uge1xuICAgICAgdGhpcy5jdXJyZW50TW9udGhSb3cgPSAtMTtcbiAgICAgIHRoaXMuY3VycmVudE1vbnRoQ29sID0gLTE7XG4gICAgfVxuICB9XG5cbiAgcHJpdmF0ZSBjYWxjdWxhdGVBY3RpdmVNb250aCgpOiB2b2lkIHtcbiAgICB0aGlzLmFjdGl2ZU1vbnRoUm93ID0gTWF0aC5mbG9vcih0aGlzLmFjdGl2ZURhdGUuZ2V0TW9udGgoKSAvIDMpO1xuICAgIHRoaXMuYWN0aXZlTW9udGhDb2wgPSB0aGlzLmFjdGl2ZURhdGUuZ2V0TW9udGgoKSAlIDM7XG4gIH1cbn1cblxuZXhwb3J0IGludGVyZmFjZSBEYXlDZWxsQ29udGV4dCB7XG4gIHRpdGxlOiBzdHJpbmc7XG4gIGxhYmVsOiBzdHJpbmc7XG59XG5cbmV4cG9ydCBpbnRlcmZhY2UgTW9udGhDZWxsQ29udGV4dCB7XG4gIHRpdGxlOiBzdHJpbmc7XG4gIGxhYmVsOiBzdHJpbmc7XG4gIHN0YXJ0OiBEYXRlO1xufVxuXG5leHBvcnQgaW50ZXJmYWNlIERhdGVDZWxsQ29udGV4dCB7XG4gIHRpdGxlOiBzdHJpbmc7XG4gIGxhYmVsOiBzdHJpbmc7XG4gIHJlbDogJ2xhc3QnIHwgJ2N1cnJlbnQnIHwgJ25leHQnO1xuICB2YWx1ZTogRGF0ZTtcbn1cbiJdfQ==