/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { ChangeDetectionStrategy, Component, EventEmitter, Input, Output, ViewEncapsulation } from '@angular/core';
import { DateHelperService } from '../../../i18n/date-helper.service';
import { CandyDate } from '../candy-date';
var CalendarHeaderComponent = /** @class */ (function () {
    function CalendarHeaderComponent(dateHelper) {
        this.dateHelper = dateHelper;
        this.enablePrev = true;
        this.enableNext = true;
        this.showTimePicker = false;
        this.valueChange = new EventEmitter();
        this.panelModeChange = new EventEmitter();
        this.chooseDecade = new EventEmitter();
        this.chooseYear = new EventEmitter();
        this.chooseMonth = new EventEmitter();
        this.prefixCls = 'ant-calendar';
        this.yearToMonth = false; // Indicate whether should change to month panel when current is year panel (if referer=month, it should show month panel when choosed a year)
    }
    /**
     * @return {?}
     */
    CalendarHeaderComponent.prototype.ngOnInit = /**
     * @return {?}
     */
    function () {
        if (!this.value) {
            this.value = new CandyDate(); // Show today by default
        }
    };
    /**
     * @param {?} changes
     * @return {?}
     */
    CalendarHeaderComponent.prototype.ngOnChanges = /**
     * @param {?} changes
     * @return {?}
     */
    function (changes) {
        if (changes.value || changes.showTimePicker || changes.panelMode) {
            this.render();
        }
    };
    /**
     * @return {?}
     */
    CalendarHeaderComponent.prototype.previousYear = /**
     * @return {?}
     */
    function () {
        this.gotoYear(-1);
    };
    /**
     * @return {?}
     */
    CalendarHeaderComponent.prototype.nextYear = /**
     * @return {?}
     */
    function () {
        this.gotoYear(1);
    };
    /**
     * @return {?}
     */
    CalendarHeaderComponent.prototype.previousMonth = /**
     * @return {?}
     */
    function () {
        this.gotoMonth(-1);
    };
    /**
     * @return {?}
     */
    CalendarHeaderComponent.prototype.nextMonth = /**
     * @return {?}
     */
    function () {
        this.gotoMonth(1);
    };
    /**
     * @param {?} mode
     * @param {?=} value
     * @return {?}
     */
    CalendarHeaderComponent.prototype.changePanel = /**
     * @param {?} mode
     * @param {?=} value
     * @return {?}
     */
    function (mode, value) {
        this.panelModeChange.emit(mode);
        if (value) {
            this.changeValueFromInside(value);
        }
    };
    /**
     * @param {?} value
     * @return {?}
     */
    CalendarHeaderComponent.prototype.onChooseDecade = /**
     * @param {?} value
     * @return {?}
     */
    function (value) {
        this.changePanel('year', value);
        this.chooseDecade.emit(value);
    };
    /**
     * @param {?} value
     * @return {?}
     */
    CalendarHeaderComponent.prototype.onChooseYear = /**
     * @param {?} value
     * @return {?}
     */
    function (value) {
        this.changePanel(this.yearToMonth ? 'month' : 'date', value);
        this.yearToMonth = false; // Clear
        this.chooseYear.emit(value);
    };
    /**
     * @param {?} value
     * @return {?}
     */
    CalendarHeaderComponent.prototype.onChooseMonth = /**
     * @param {?} value
     * @return {?}
     */
    function (value) {
        this.changePanel('date', value);
        this.yearToMonth = false; // Clear
        this.chooseMonth.emit(value);
    };
    /**
     * @return {?}
     */
    CalendarHeaderComponent.prototype.changeToMonthPanel = /**
     * @return {?}
     */
    function () {
        this.changePanel('month');
        this.yearToMonth = true;
    };
    /**
     * @private
     * @return {?}
     */
    CalendarHeaderComponent.prototype.render = /**
     * @private
     * @return {?}
     */
    function () {
        if (this.value) {
            this.yearMonthDaySelectors = this.createYearMonthDaySelectors();
        }
    };
    /**
     * @private
     * @param {?} amount
     * @return {?}
     */
    CalendarHeaderComponent.prototype.gotoMonth = /**
     * @private
     * @param {?} amount
     * @return {?}
     */
    function (amount) {
        this.changeValueFromInside(this.value.addMonths(amount));
    };
    /**
     * @private
     * @param {?} amount
     * @return {?}
     */
    CalendarHeaderComponent.prototype.gotoYear = /**
     * @private
     * @param {?} amount
     * @return {?}
     */
    function (amount) {
        this.changeValueFromInside(this.value.addYears(amount));
    };
    /**
     * @private
     * @param {?} value
     * @return {?}
     */
    CalendarHeaderComponent.prototype.changeValueFromInside = /**
     * @private
     * @param {?} value
     * @return {?}
     */
    function (value) {
        if (this.value !== value) {
            this.value = value;
            this.valueChange.emit(this.value);
            this.render();
        }
    };
    /**
     * @private
     * @param {?} localeFormat
     * @return {?}
     */
    CalendarHeaderComponent.prototype.formatDateTime = /**
     * @private
     * @param {?} localeFormat
     * @return {?}
     */
    function (localeFormat) {
        return this.dateHelper.format(this.value.nativeDate, localeFormat);
    };
    /**
     * @private
     * @return {?}
     */
    CalendarHeaderComponent.prototype.createYearMonthDaySelectors = /**
     * @private
     * @return {?}
     */
    function () {
        var _this = this;
        /** @type {?} */
        var year;
        /** @type {?} */
        var month;
        /** @type {?} */
        var day;
        // NOTE: Compat for DatePipe formatting rules
        /** @type {?} */
        var yearFormat = this.locale.yearFormat;
        if (this.dateHelper.relyOnDatePipe) {
            yearFormat = ((/** @type {?} */ (this.dateHelper))).transCompatFormat(yearFormat);
        }
        year = {
            className: this.prefixCls + "-year-select",
            title: this.locale.yearSelect,
            onClick: (/**
             * @return {?}
             */
            function () { return _this.showTimePicker ? null : _this.changePanel('year'); }),
            label: this.formatDateTime(yearFormat)
        };
        month = {
            className: this.prefixCls + "-month-select",
            title: this.locale.monthSelect,
            onClick: (/**
             * @return {?}
             */
            function () { return _this.showTimePicker ? null : _this.changeToMonthPanel(); }),
            label: this.formatDateTime(this.locale.monthFormat || 'MMM')
        };
        // NOTE: Compat for DatePipe formatting rules
        /** @type {?} */
        var dayFormat = this.locale.dayFormat;
        if (this.dateHelper.relyOnDatePipe) {
            dayFormat = ((/** @type {?} */ (this.dateHelper))).transCompatFormat(dayFormat);
        }
        if (this.showTimePicker) {
            day = {
                className: this.prefixCls + "-day-select",
                label: this.formatDateTime(dayFormat)
            };
        }
        /** @type {?} */
        var result;
        if (this.locale.monthBeforeYear) {
            result = [month, (/** @type {?} */ (day)), year];
        }
        else {
            result = [year, month, (/** @type {?} */ (day))];
        }
        return result.filter((/**
         * @param {?} selector
         * @return {?}
         */
        function (selector) { return !!selector; }));
    };
    CalendarHeaderComponent.decorators = [
        { type: Component, args: [{
                    encapsulation: ViewEncapsulation.None,
                    changeDetection: ChangeDetectionStrategy.OnPush,
                    // tslint:disable-next-line:component-selector
                    selector: 'calendar-header',
                    template: "<div class=\"{{ prefixCls }}-header\">\n  <div style=\"position: relative;\">\n    <a *ngIf=\"enablePrev && !showTimePicker\"\n      class=\"{{ prefixCls }}-prev-year-btn\"\n      role=\"button\"\n      (click)=\"previousYear()\"\n      title=\"{{ locale.previousYear }}\"\n    ></a>\n    <a *ngIf=\"enablePrev && !showTimePicker\"\n      class=\"{{ prefixCls }}-prev-month-btn\"\n      role=\"button\"\n      (click)=\"previousMonth()\"\n      title=\"{{ locale.previousMonth }}\"\n    ></a>\n\n    <span class=\"{{ prefixCls }}-{{ locale.monthBeforeYear ? 'my-select' : 'ym-select' }}\">\n      <ng-container *ngFor=\"let selector of yearMonthDaySelectors\">\n        <a class=\"{{ selector.className }}\"\n          role=\"button\"\n          (click)=\"selector.onClick ? selector.onClick() : null\"\n          title=\"{{ selector.title || null }}\"\n        >\n          {{ selector.label }}\n        </a>\n      </ng-container>\n    </span>\n\n    <a *ngIf=\"enableNext && !showTimePicker\"\n      class=\"{{ prefixCls }}-next-month-btn\"\n      role=\"button\"\n      (click)=\"nextMonth()\"\n      title=\"{{ locale.nextMonth }}\"\n    ></a>\n    <a *ngIf=\"enableNext && !showTimePicker\"\n      class=\"{{ prefixCls }}-next-year-btn\"\n      role=\"button\"\n      (click)=\"nextYear()\"\n      title=\"{{ locale.nextYear }}\"\n    ></a>\n  </div>\n\n  <ng-container [ngSwitch]=\"panelMode\">\n    <ng-container *ngSwitchCase=\"'decade'\">\n      <decade-panel\n        [locale]=\"locale\"\n        [value]=\"value\"\n        (valueChange)=\"onChooseDecade($event)\"\n      ></decade-panel>\n    </ng-container>\n    <ng-container *ngSwitchCase=\"'year'\">\n      <year-panel\n        [locale]=\"locale\"\n        [value]=\"value\"\n        [disabledDate]=\"disabledYear\"\n        (valueChange)=\"onChooseYear($event)\"\n        (decadePanelShow)=\"changePanel('decade')\"\n      ></year-panel>\n    </ng-container>\n    <ng-container *ngSwitchCase=\"'month'\">\n      <month-panel\n        [locale]=\"locale\"\n        [value]=\"value\"\n        [disabledDate]=\"disabledMonth\"\n        (valueChange)=\"onChooseMonth($event)\"\n        (yearPanelShow)=\"changePanel('year')\"\n      ></month-panel>\n    </ng-container>\n  </ng-container>\n</div>"
                }] }
    ];
    /** @nocollapse */
    CalendarHeaderComponent.ctorParameters = function () { return [
        { type: DateHelperService }
    ]; };
    CalendarHeaderComponent.propDecorators = {
        locale: [{ type: Input }],
        enablePrev: [{ type: Input }],
        enableNext: [{ type: Input }],
        disabledMonth: [{ type: Input }],
        disabledYear: [{ type: Input }],
        showTimePicker: [{ type: Input }],
        value: [{ type: Input }],
        valueChange: [{ type: Output }],
        panelMode: [{ type: Input }],
        panelModeChange: [{ type: Output }],
        chooseDecade: [{ type: Output }],
        chooseYear: [{ type: Output }],
        chooseMonth: [{ type: Output }]
    };
    return CalendarHeaderComponent;
}());
export { CalendarHeaderComponent };
if (false) {
    /** @type {?} */
    CalendarHeaderComponent.prototype.locale;
    /** @type {?} */
    CalendarHeaderComponent.prototype.enablePrev;
    /** @type {?} */
    CalendarHeaderComponent.prototype.enableNext;
    /** @type {?} */
    CalendarHeaderComponent.prototype.disabledMonth;
    /** @type {?} */
    CalendarHeaderComponent.prototype.disabledYear;
    /** @type {?} */
    CalendarHeaderComponent.prototype.showTimePicker;
    /** @type {?} */
    CalendarHeaderComponent.prototype.value;
    /** @type {?} */
    CalendarHeaderComponent.prototype.valueChange;
    /** @type {?} */
    CalendarHeaderComponent.prototype.panelMode;
    /** @type {?} */
    CalendarHeaderComponent.prototype.panelModeChange;
    /** @type {?} */
    CalendarHeaderComponent.prototype.chooseDecade;
    /** @type {?} */
    CalendarHeaderComponent.prototype.chooseYear;
    /** @type {?} */
    CalendarHeaderComponent.prototype.chooseMonth;
    /** @type {?} */
    CalendarHeaderComponent.prototype.prefixCls;
    /** @type {?} */
    CalendarHeaderComponent.prototype.yearMonthDaySelectors;
    /**
     * @type {?}
     * @private
     */
    CalendarHeaderComponent.prototype.yearToMonth;
    /**
     * @type {?}
     * @private
     */
    CalendarHeaderComponent.prototype.dateHelper;
}
/**
 * @record
 */
export function YearMonthDaySelector() { }
if (false) {
    /** @type {?} */
    YearMonthDaySelector.prototype.className;
    /** @type {?|undefined} */
    YearMonthDaySelector.prototype.title;
    /** @type {?} */
    YearMonthDaySelector.prototype.label;
    /**
     * @return {?}
     */
    YearMonthDaySelector.prototype.onClick = function () { };
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiY2FsZW5kYXItaGVhZGVyLmNvbXBvbmVudC5qcyIsInNvdXJjZVJvb3QiOiJuZzovL25nLXpvcnJvLWFudGQvIiwic291cmNlcyI6WyJkYXRlLXBpY2tlci9saWIvY2FsZW5kYXIvY2FsZW5kYXItaGVhZGVyLmNvbXBvbmVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7O0FBQUEsT0FBTyxFQUFFLHVCQUF1QixFQUFFLFNBQVMsRUFBRSxZQUFZLEVBQUUsS0FBSyxFQUFxQixNQUFNLEVBQWlCLGlCQUFpQixFQUFFLE1BQU0sZUFBZSxDQUFDO0FBRXJKLE9BQU8sRUFBd0IsaUJBQWlCLEVBQUUsTUFBTSxtQ0FBbUMsQ0FBQztBQUc1RixPQUFPLEVBQUUsU0FBUyxFQUFFLE1BQU0sZUFBZSxDQUFDO0FBRTFDO0lBK0JFLGlDQUFvQixVQUE2QjtRQUE3QixlQUFVLEdBQVYsVUFBVSxDQUFtQjtRQXJCeEMsZUFBVSxHQUFZLElBQUksQ0FBQztRQUMzQixlQUFVLEdBQVksSUFBSSxDQUFDO1FBRzNCLG1CQUFjLEdBQVksS0FBSyxDQUFDO1FBR3RCLGdCQUFXLEdBQUcsSUFBSSxZQUFZLEVBQWEsQ0FBQztRQUc1QyxvQkFBZSxHQUFHLElBQUksWUFBWSxFQUFhLENBQUM7UUFFaEQsaUJBQVksR0FBRyxJQUFJLFlBQVksRUFBYSxDQUFDO1FBQzdDLGVBQVUsR0FBRyxJQUFJLFlBQVksRUFBYSxDQUFDO1FBQzNDLGdCQUFXLEdBQUcsSUFBSSxZQUFZLEVBQWEsQ0FBQztRQUUvRCxjQUFTLEdBQVcsY0FBYyxDQUFDO1FBRzNCLGdCQUFXLEdBQVksS0FBSyxDQUFDLENBQUMsOElBQThJO0lBRS9ILENBQUM7Ozs7SUFFdEQsMENBQVE7OztJQUFSO1FBQ0UsSUFBSSxDQUFDLElBQUksQ0FBQyxLQUFLLEVBQUU7WUFDZixJQUFJLENBQUMsS0FBSyxHQUFHLElBQUksU0FBUyxFQUFFLENBQUMsQ0FBQyx3QkFBd0I7U0FDdkQ7SUFDSCxDQUFDOzs7OztJQUVELDZDQUFX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QUE2QztpQkFDOUM7Ozs7Z0JBWDhCLGlCQUFpQjs7O3lCQWM3QyxLQUFLOzZCQUNMLEtBQUs7NkJBQ0wsS0FBSztnQ0FDTCxLQUFLOytCQUNMLEtBQUs7aUNBQ0wsS0FBSzt3QkFFTCxLQUFLOzhCQUNMLE1BQU07NEJBRU4sS0FBSztrQ0FDTCxNQUFNOytCQUVOLE1BQU07NkJBQ04sTUFBTTs4QkFDTixNQUFNOztJQTBJVCw4QkFBQztDQUFBLEFBbEtELElBa0tDO1NBMUpZLHVCQUF1Qjs7O0lBQ2xDLHlDQUF5Qzs7SUFDekMsNkNBQW9DOztJQUNwQyw2Q0FBb0M7O0lBQ3BDLGdEQUFnRDs7SUFDaEQsK0NBQStDOztJQUMvQyxpREFBeUM7O0lBRXpDLHdDQUEwQjs7SUFDMUIsOENBQStEOztJQUUvRCw0Q0FBOEI7O0lBQzlCLGtEQUFtRTs7SUFFbkUsK0NBQWdFOztJQUNoRSw2Q0FBOEQ7O0lBQzlELDhDQUErRDs7SUFFL0QsNENBQW1DOztJQUNuQyx3REFBOEM7Ozs7O0lBRTlDLDhDQUFxQzs7Ozs7SUFFekIsNkNBQXFDOzs7OztBQXFJbkQsMENBS0M7OztJQUpDLHlDQUFrQjs7SUFDbEIscUNBQWU7O0lBQ2YscUNBQWM7Ozs7SUFDZCx5REFBaUIiLCJzb3VyY2VzQ29udGVudCI6WyJpbXBvcnQgeyBDaGFuZ2VEZXRlY3Rpb25TdHJhdGVneSwgQ29tcG9uZW50LCBFdmVudEVtaXR0ZXIsIElucHV0LCBPbkNoYW5nZXMsIE9uSW5pdCwgT3V0cHV0LCBTaW1wbGVDaGFuZ2VzLCBWaWV3RW5jYXBzdWxhdGlvbiB9IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuXG5pbXBvcnQgeyBEYXRlSGVscGVyQnlEYXRlUGlwZSwgRGF0ZUhlbHBlclNlcnZpY2UgfSBmcm9tICcuLi8uLi8uLi9pMThuL2RhdGUtaGVscGVyLnNlcnZpY2UnO1xuaW1wb3J0IHsgTnpDYWxlbmRhckkxOG5JbnRlcmZhY2UgfSBmcm9tICcuLi8uLi8uLi9pMThuL256LWkxOG4uaW50ZXJmYWNlJztcbmltcG9ydCB7IFBhbmVsTW9kZSB9IGZyb20gJy4uLy4uL3N0YW5kYXJkLXR5cGVzJztcbmltcG9ydCB7IENhbmR5RGF0ZSB9IGZyb20gJy4uL2NhbmR5LWRhdGUnO1xuXG5AQ29tcG9uZW50KHtcbiAgZW5jYXBzdWxhdGlvbjogVmlld0VuY2Fwc3VsYXRpb24uTm9uZSxcbiAgY2hhbmdlRGV0ZWN0aW9uOiBDaGFuZ2VEZXRlY3Rpb25TdHJhdGVneS5PblB1c2gsXG4gIC8vIHRzbGludDpkaXNhYmxlLW5leHQtbGluZTpjb21wb25lbnQtc2VsZWN0b3JcbiAgc2VsZWN0b3I6ICdjYWxlbmRhci1oZWFkZXInLFxuICB0ZW1wbGF0ZVVybDogJ2NhbGVuZGFyLWhlYWRlci5jb21wb25lbnQuaHRtbCdcbn0pXG5cbmV4cG9ydCBjbGFzcyBDYWxlbmRhckhlYWRlckNvbXBvbmVudCBpbXBsZW1lbnRzIE9uSW5pdCwgT25DaGFuZ2VzIHtcbiAgQElucHV0KCkgbG9jYWxlOiBOekNhbGVuZGFySTE4bkludGVyZmFjZTtcbiAgQElucHV0KCkgZW5hYmxlUHJldjogYm9vbGVhbiA9IHRydWU7XG4gIEBJbnB1dCgpIGVuYWJsZU5leHQ6IGJvb2xlYW4gPSB0cnVlO1xuICBASW5wdXQoKSBkaXNhYmxlZE1vbnRoOiAoZGF0ZTogRGF0ZSkgPT4gYm9vbGVhbjtcbiAgQElucHV0KCkgZGlzYWJsZWRZZWFyOiAoZGF0ZTogRGF0ZSkgPT4gYm9vbGVhbjtcbiAgQElucHV0KCkgc2hvd1RpbWVQaWNrZXI6IGJvb2xlYW4gPSBmYWxzZTtcblxuICBASW5wdXQoKSB2YWx1ZTogQ2FuZHlEYXRlO1xuICBAT3V0cHV0KCkgcmVhZG9ubHkgdmFsdWVDaGFuZ2UgPSBuZXcgRXZlbnRFbWl0dGVyPENhbmR5RGF0ZT4oKTtcblxuICBASW5wdXQoKSBwYW5lbE1vZGU6IFBhbmVsTW9kZTtcbiAgQE91dHB1dCgpIHJlYWRvbmx5IHBhbmVsTW9kZUNoYW5nZSA9IG5ldyBFdmVudEVtaXR0ZXI8UGFuZWxNb2RlPigpO1xuXG4gIEBPdXRwdXQoKSByZWFkb25seSBjaG9vc2VEZWNhZGUgPSBuZXcgRXZlbnRFbWl0dGVyPENhbmR5RGF0ZT4oKTtcbiAgQE91dHB1dCgpIHJlYWRvbmx5IGNob29zZVllYXIgPSBuZXcgRXZlbnRFbWl0dGVyPENhbmR5RGF0ZT4oKTtcbiAgQE91dHB1dCgpIHJlYWRvbmx5IGNob29zZU1vbnRoID0gbmV3IEV2ZW50RW1pdHRlcjxDYW5keURhdGU+KCk7XG5cbiAgcHJlZml4Q2xzOiBzdHJpbmcgPSAnYW50LWNhbGVuZGFyJztcbiAgeWVhck1vbnRoRGF5U2VsZWN0b3JzOiBZZWFyTW9udGhEYXlTZWxlY3RvcltdO1xuXG4gIHByaXZhdGUgeWVhclRvTW9udGg6IGJvb2xlYW4gPSBmYWxzZTsgLy8gSW5kaWNhdGUgd2hldGhlciBzaG91bGQgY2hhbmdlIHRvIG1vbnRoIHBhbmVsIHdoZW4gY3VycmVudCBpcyB5ZWFyIHBhbmVsIChpZiByZWZlcmVyPW1vbnRoLCBpdCBzaG91bGQgc2hvdyBtb250aCBwYW5lbCB3aGVuIGNob29zZWQgYSB5ZWFyKVxuXG4gIGNvbnN0cnVjdG9yKHByaXZhdGUgZGF0ZUhlbHBlcjogRGF0ZUhlbHBlclNlcnZpY2UpIHsgfVxuXG4gIG5nT25Jbml0KCk6IHZvaWQge1xuICAgIGlmICghdGhpcy52YWx1ZSkge1xuICAgICAgdGhpcy52YWx1ZSA9IG5ldyBDYW5keURhdGUoKTsgLy8gU2hvdyB0b2RheSBieSBkZWZhdWx0XG4gICAgfVxuICB9XG5cbiAgbmdPbkNoYW5nZXMoY2hhbmdlczogU2ltcGxlQ2hhbmdlcyk6IHZvaWQge1xuICAgIGlmIChjaGFuZ2VzLnZhbHVlIHx8IGNoYW5nZXMuc2hvd1RpbWVQaWNrZXIgfHwgY2hhbmdlcy5wYW5lbE1vZGUpIHtcbiAgICAgIHRoaXMucmVuZGVyKCk7XG4gICAgfVxuICB9XG5cbiAgcHJldmlvdXNZZWFyKCk6IHZvaWQge1xuICAgIHRoaXMuZ290b1llYXIoLTEpO1xuICB9XG5cbiAgbmV4dFllYXIoKTogdm9pZCB7XG4gICAgdGhpcy5nb3RvWWVhcigxKTtcbiAgfVxuXG4gIHByZXZpb3VzTW9udGgoKTogdm9pZCB7XG4gICAgdGhpcy5nb3RvTW9udGgoLTEpO1xuICB9XG5cbiAgbmV4dE1vbnRoKCk6IHZvaWQge1xuICAgIHRoaXMuZ290b01vbnRoKDEpO1xuICB9XG5cbiAgY2hhbmdlUGFuZWwobW9kZTogUGFuZWxNb2RlLCB2YWx1ZT86IENhbmR5RGF0ZSk6IHZvaWQge1xuICAgIHRoaXMucGFuZWxNb2RlQ2hhbmdlLmVtaXQobW9kZSk7XG4gICAgaWYgKHZhbHVlKSB7XG4gICAgICB0aGlzLmNoYW5nZVZhbHVlRnJvbUluc2lkZSh2YWx1ZSk7XG4gICAgfVxuICB9XG5cbiAgb25DaG9vc2VEZWNhZGUodmFsdWU6IENhbmR5RGF0ZSk6IHZvaWQge1xuICAgIHRoaXMuY2hhbmdlUGFuZWwoJ3llYXInLCB2YWx1ZSk7XG4gICAgdGhpcy5jaG9vc2VEZWNhZGUuZW1pdCh2YWx1ZSk7XG4gIH1cblxuICBvbkNob29zZVllYXIodmFsdWU6IENhbmR5RGF0ZSk6IHZvaWQge1xuICAgIHRoaXMuY2hhbmdlUGFuZWwodGhpcy55ZWFyVG9Nb250aCA/ICdtb250aCcgOiAnZGF0ZScsIHZhbHVlKTtcbiAgICB0aGlzLnllYXJUb01vbnRoID0gZmFsc2U7IC8vIENsZWFyXG4gICAgdGhpcy5jaG9vc2VZZWFyLmVtaXQodmFsdWUpO1xuICB9XG5cbiAgb25DaG9vc2VNb250aCh2YWx1ZTogQ2FuZHlEYXRlKTogdm9pZCB7XG4gICAgdGhpcy5jaGFuZ2VQYW5lbCgnZGF0ZScsIHZhbHVlKTtcbiAgICB0aGlzLnllYXJUb01vbnRoID0gZmFsc2U7IC8vIENsZWFyXG4gICAgdGhpcy5jaG9vc2VNb250aC5lbWl0KHZhbHVlKTtcbiAgfVxuXG4gIGNoYW5nZVRvTW9udGhQYW5lbCgpOiB2b2lkIHtcbiAgICB0aGlzLmNoYW5nZVBhbmVsKCdtb250aCcpO1xuICAgIHRoaXMueWVhclRvTW9udGggPSB0cnVlO1xuICB9XG5cbiAgcHJpdmF0ZSByZW5kZXIoKTogdm9pZCB7XG4gICAgaWYgKHRoaXMudmFsdWUpIHtcbiAgICAgIHRoaXMueWVhck1vbnRoRGF5U2VsZWN0b3JzID0gdGhpcy5jcmVhdGVZZWFyTW9udGhEYXlTZWxlY3RvcnMoKTtcbiAgICB9XG4gIH1cblxuICBwcml2YXRlIGdvdG9Nb250aChhbW91bnQ6IG51bWJlcik6IHZvaWQge1xuICAgIHRoaXMuY2hhbmdlVmFsdWVGcm9tSW5zaWRlKHRoaXMudmFsdWUuYWRkTW9udGhzKGFtb3VudCkpO1xuICB9XG5cbiAgcHJpdmF0ZSBnb3RvWWVhcihhbW91bnQ6IG51bWJlcik6IHZvaWQge1xuICAgIHRoaXMuY2hhbmdlVmFsdWVGcm9tSW5zaWRlKHRoaXMudmFsdWUuYWRkWWVhcnMoYW1vdW50KSk7XG4gIH1cblxuICBwcml2YXRlIGNoYW5nZVZhbHVlRnJvbUluc2lkZSh2YWx1ZTogQ2FuZHlEYXRlKTogdm9pZCB7XG4gICAgaWYgKHRoaXMudmFsdWUgIT09IHZhbHVlKSB7XG4gICAgICB0aGlzLnZhbHVlID0gdmFsdWU7XG4gICAgICB0aGlzLnZhbHVlQ2hhbmdlLmVtaXQodGhpcy52YWx1ZSk7XG4gICAgICB0aGlzLnJlbmRlcigpO1xuICAgIH1cbiAgfVxuXG4gIHByaXZhdGUgZm9ybWF0RGF0ZVRpbWUobG9jYWxlRm9ybWF0OiBzdHJpbmcpOiBzdHJpbmcge1xuICAgIHJldHVybiB0aGlzLmRhdGVIZWxwZXIuZm9ybWF0KHRoaXMudmFsdWUubmF0aXZlRGF0ZSwgbG9jYWxlRm9ybWF0KTtcbiAgfVxuXG4gIHByaXZhdGUgY3JlYXRlWWVhck1vbnRoRGF5U2VsZWN0b3JzKCk6IFllYXJNb250aERheVNlbGVjdG9yW10ge1xuICAgIGxldCB5ZWFyOiBZZWFyTW9udGhEYXlTZWxlY3RvcjtcbiAgICBsZXQgbW9udGg6IFllYXJNb250aERheVNlbGVjdG9yO1xuICAgIGxldCBkYXk6IFllYXJNb250aERheVNlbGVjdG9yO1xuXG4gICAgLy8gTk9URTogQ29tcGF0IGZvciBEYXRlUGlwZSBmb3JtYXR0aW5nIHJ1bGVzXG4gICAgbGV0IHllYXJGb3JtYXQ6IHN0cmluZyA9IHRoaXMubG9jYWxlLnllYXJGb3JtYXQ7XG4gICAgaWYgKHRoaXMuZGF0ZUhlbHBlci5yZWx5T25EYXRlUGlwZSkge1xuICAgICAgeWVhckZvcm1hdCA9ICh0aGlzLmRhdGVIZWxwZXIgYXMgRGF0ZUhlbHBlckJ5RGF0ZVBpcGUpLnRyYW5zQ29tcGF0Rm9ybWF0KHllYXJGb3JtYXQpO1xuICAgIH1cbiAgICB5ZWFyID0ge1xuICAgICAgY2xhc3NOYW1lOiBgJHt0aGlzLnByZWZpeENsc30teWVhci1zZWxlY3RgLFxuICAgICAgdGl0bGU6IHRoaXMubG9jYWxlLnllYXJTZWxlY3QsXG4gICAgICBvbkNsaWNrOiAoKSA9PiB0aGlzLnNob3dUaW1lUGlja2VyID8gbnVsbCA6IHRoaXMuY2hhbmdlUGFuZWwoJ3llYXInKSxcbiAgICAgIGxhYmVsOiB0aGlzLmZvcm1hdERhdGVUaW1lKHllYXJGb3JtYXQpXG4gICAgfTtcblxuICAgIG1vbnRoID0ge1xuICAgICAgY2xhc3NOYW1lOiBgJHt0aGlzLnByZWZpeENsc30tbW9udGgtc2VsZWN0YCxcbiAgICAgIHRpdGxlOiB0aGlzLmxvY2FsZS5tb250aFNlbGVjdCxcbiAgICAgIG9uQ2xpY2s6ICgpID0+IHRoaXMuc2hvd1RpbWVQaWNrZXIgPyBudWxsIDogdGhpcy5jaGFuZ2VUb01vbnRoUGFuZWwoKSxcbiAgICAgIGxhYmVsOiB0aGlzLmZvcm1hdERhdGVUaW1lKHRoaXMubG9jYWxlLm1vbnRoRm9ybWF0IHx8ICdNTU0nKVxuICAgIH07XG5cbiAgICAvLyBOT1RFOiBDb21wYXQgZm9yIERhdGVQaXBlIGZvcm1hdHRpbmcgcnVsZXNcbiAgICBsZXQgZGF5Rm9ybWF0OiBzdHJpbmcgPSB0aGlzLmxvY2FsZS5kYXlGb3JtYXQ7XG4gICAgaWYgKHRoaXMuZGF0ZUhlbHBlci5yZWx5T25EYXRlUGlwZSkge1xuICAgICAgZGF5Rm9ybWF0ID0gKHRoaXMuZGF0ZUhlbHBlciBhcyBEYXRlSGVscGVyQnlEYXRlUGlwZSkudHJhbnNDb21wYXRGb3JtYXQoZGF5Rm9ybWF0KTtcbiAgICB9XG4gICAgaWYgKHRoaXMuc2hvd1RpbWVQaWNrZXIpIHtcbiAgICAgIGRheSA9IHtcbiAgICAgICAgY2xhc3NOYW1lOiBgJHt0aGlzLnByZWZpeENsc30tZGF5LXNlbGVjdGAsXG4gICAgICAgIGxhYmVsOiB0aGlzLmZvcm1hdERhdGVUaW1lKGRheUZvcm1hdClcbiAgICAgIH07XG4gICAgfVxuXG4gICAgbGV0IHJlc3VsdDogWWVhck1vbnRoRGF5U2VsZWN0b3JbXTtcblxuICAgIGlmICh0aGlzLmxvY2FsZS5tb250aEJlZm9yZVllYXIpIHtcbiAgICAgIHJlc3VsdCA9IFsgbW9udGgsIGRheSEsIHllYXIgXTtcbiAgICB9IGVsc2Uge1xuICAgICAgcmVzdWx0ID0gWyB5ZWFyLCBtb250aCwgZGF5ISBdO1xuICAgIH1cblxuICAgIHJldHVybiByZXN1bHQuZmlsdGVyKHNlbGVjdG9yID0+ICEhc2VsZWN0b3IpO1xuICB9XG59XG5cbmV4cG9ydCBpbnRlcmZhY2UgWWVhck1vbnRoRGF5U2VsZWN0b3Ige1xuICBjbGFzc05hbWU6IHN0cmluZztcbiAgdGl0bGU/OiBzdHJpbmc7XG4gIGxhYmVsOiBzdHJpbmc7XG4gIG9uQ2xpY2s/KCk6IHZvaWQ7XG59XG4iXX0=