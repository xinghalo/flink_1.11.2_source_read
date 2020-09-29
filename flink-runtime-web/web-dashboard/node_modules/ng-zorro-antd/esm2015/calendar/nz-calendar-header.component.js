/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { ChangeDetectionStrategy, Component, EventEmitter, Input, Output, ViewEncapsulation } from '@angular/core';
import setMonth from 'date-fns/set_month';
import { DateHelperService } from '../i18n/date-helper.service';
import { NzI18nService as I18n } from '../i18n/nz-i18n.service';
export class NzCalendarHeaderComponent {
    /**
     * @param {?} i18n
     * @param {?} dateHelper
     */
    constructor(i18n, dateHelper) {
        this.i18n = i18n;
        this.dateHelper = dateHelper;
        this.mode = 'month';
        this.modeChange = new EventEmitter();
        this.fullscreen = true;
        this.yearChange = new EventEmitter();
        this.monthChange = new EventEmitter();
        this._activeDate = new Date();
        this.yearOffset = 10;
        this.yearTotal = 20;
    }
    /**
     * @param {?} value
     * @return {?}
     */
    set activeDate(value) {
        this._activeDate = value;
        this.setUpYears();
    }
    /**
     * @return {?}
     */
    get activeDate() {
        return this._activeDate;
    }
    /**
     * @return {?}
     */
    get activeYear() {
        return this.activeDate.getFullYear();
    }
    /**
     * @return {?}
     */
    get activeMonth() {
        return this.activeDate.getMonth();
    }
    /**
     * @return {?}
     */
    get size() {
        return this.fullscreen ? 'default' : 'small';
    }
    /**
     * @return {?}
     */
    get yearTypeText() {
        return this.i18n.getLocale().Calendar.year;
    }
    /**
     * @return {?}
     */
    get monthTypeText() {
        return this.i18n.getLocale().Calendar.month;
    }
    /**
     * @return {?}
     */
    ngOnInit() {
        this.setUpYears();
        this.setUpMonths();
    }
    /**
     * @param {?} year
     * @return {?}
     */
    updateYear(year) {
        this.yearChange.emit(year);
        this.setUpYears(year);
    }
    /**
     * @private
     * @param {?=} year
     * @return {?}
     */
    setUpYears(year) {
        /** @type {?} */
        const start = (year || this.activeYear) - this.yearOffset;
        /** @type {?} */
        const end = start + this.yearTotal;
        this.years = [];
        for (let i = start; i < end; i++) {
            this.years.push({ label: `${i}`, value: i });
        }
    }
    /**
     * @private
     * @return {?}
     */
    setUpMonths() {
        this.months = [];
        for (let i = 0; i < 12; i++) {
            /** @type {?} */
            const dateInMonth = setMonth(this.activeDate, i);
            /** @type {?} */
            const monthText = this.dateHelper.format(dateInMonth, 'MMM');
            this.months.push({ label: monthText, value: i });
        }
    }
}
NzCalendarHeaderComponent.decorators = [
    { type: Component, args: [{
                encapsulation: ViewEncapsulation.None,
                changeDetection: ChangeDetectionStrategy.OnPush,
                selector: 'nz-calendar-header',
                template: "<nz-select class=\"ant-fullcalendar-year-select\" [nzSize]=\"size\" [nzDropdownMatchSelectWidth]=\"false\"\n           [ngModel]=\"activeYear\" (ngModelChange)=\"updateYear($event)\">\n  <nz-option *ngFor=\"let year of years\" [nzLabel]=\"year.label\" [nzValue]=\"year.value\"></nz-option>\n</nz-select>\n\n<nz-select *ngIf=\"mode === 'month'\" class=\"ant-fullcalendar-month-select\" [nzSize]=\"size\" [nzDropdownMatchSelectWidth]=\"false\"\n           [ngModel]=\"activeMonth\" (ngModelChange)=\"monthChange.emit($event)\">\n  <nz-option *ngFor=\"let month of months\" [nzLabel]=\"month.label\" [nzValue]=\"month.value\"></nz-option>\n</nz-select>\n\n<nz-radio-group [(ngModel)]=\"mode\" (ngModelChange)=\"modeChange.emit($event)\" [nzSize]=\"size\">\n  <label nz-radio-button nzValue=\"month\">{{ monthTypeText }}</label>\n  <label nz-radio-button nzValue=\"year\">{{ yearTypeText }}</label>\n</nz-radio-group>\n",
                host: {
                    '[style.display]': `'block'`,
                    '[class.ant-fullcalendar-header]': `true`
                }
            }] }
];
/** @nocollapse */
NzCalendarHeaderComponent.ctorParameters = () => [
    { type: I18n },
    { type: DateHelperService }
];
NzCalendarHeaderComponent.propDecorators = {
    mode: [{ type: Input }],
    modeChange: [{ type: Output }],
    fullscreen: [{ type: Input }],
    activeDate: [{ type: Input }],
    yearChange: [{ type: Output }],
    monthChange: [{ type: Output }]
};
if (false) {
    /** @type {?} */
    NzCalendarHeaderComponent.prototype.mode;
    /** @type {?} */
    NzCalendarHeaderComponent.prototype.modeChange;
    /** @type {?} */
    NzCalendarHeaderComponent.prototype.fullscreen;
    /** @type {?} */
    NzCalendarHeaderComponent.prototype.yearChange;
    /** @type {?} */
    NzCalendarHeaderComponent.prototype.monthChange;
    /** @type {?} */
    NzCalendarHeaderComponent.prototype._activeDate;
    /** @type {?} */
    NzCalendarHeaderComponent.prototype.yearOffset;
    /** @type {?} */
    NzCalendarHeaderComponent.prototype.yearTotal;
    /** @type {?} */
    NzCalendarHeaderComponent.prototype.years;
    /** @type {?} */
    NzCalendarHeaderComponent.prototype.months;
    /**
     * @type {?}
     * @private
     */
    NzCalendarHeaderComponent.prototype.i18n;
    /**
     * @type {?}
     * @private
     */
    NzCalendarHeaderComponent.prototype.dateHelper;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotY2FsZW5kYXItaGVhZGVyLmNvbXBvbmVudC5qcyIsInNvdXJjZVJvb3QiOiJuZzovL25nLXpvcnJvLWFudGQvIiwic291cmNlcyI6WyJjYWxlbmRhci9uei1jYWxlbmRhci1oZWFkZXIuY29tcG9uZW50LnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7QUFBQSxPQUFPLEVBQ0wsdUJBQXVCLEVBQ3ZCLFNBQVMsRUFDVCxZQUFZLEVBQ1osS0FBSyxFQUVMLE1BQU0sRUFDTixpQkFBaUIsRUFDbEIsTUFBTSxlQUFlLENBQUM7QUFDdkIsT0FBTyxRQUFRLE1BQU0sb0JBQW9CLENBQUM7QUFDMUMsT0FBTyxFQUFFLGlCQUFpQixFQUFFLE1BQU0sNkJBQTZCLENBQUM7QUFDaEUsT0FBTyxFQUFFLGFBQWEsSUFBSSxJQUFJLEVBQUUsTUFBTSx5QkFBeUIsQ0FBQztBQVloRSxNQUFNLE9BQU8seUJBQXlCOzs7OztJQTZDcEMsWUFBb0IsSUFBVSxFQUFVLFVBQTZCO1FBQWpELFNBQUksR0FBSixJQUFJLENBQU07UUFBVSxlQUFVLEdBQVYsVUFBVSxDQUFtQjtRQTVDNUQsU0FBSSxHQUFxQixPQUFPLENBQUM7UUFDdkIsZUFBVSxHQUFtQyxJQUFJLFlBQVksRUFBRSxDQUFDO1FBRTFFLGVBQVUsR0FBWSxJQUFJLENBQUM7UUFZakIsZUFBVSxHQUF5QixJQUFJLFlBQVksRUFBRSxDQUFDO1FBQ3RELGdCQUFXLEdBQXlCLElBQUksWUFBWSxFQUFFLENBQUM7UUFFMUUsZ0JBQVcsR0FBRyxJQUFJLElBQUksRUFBRSxDQUFDO1FBQ3pCLGVBQVUsR0FBVyxFQUFFLENBQUM7UUFDeEIsY0FBUyxHQUFXLEVBQUUsQ0FBQztJQXdCaUQsQ0FBQzs7Ozs7SUF2Q3pFLElBQ0ksVUFBVSxDQUFDLEtBQVc7UUFDeEIsSUFBSSxDQUFDLFdBQVcsR0FBRyxLQUFLLENBQUM7UUFDekIsSUFBSSxDQUFDLFVBQVUsRUFBRSxDQUFDO0lBQ3BCLENBQUM7Ozs7SUFFRCxJQUFJLFVBQVU7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OztJQUN4Qiw4Q0FBdUI7O0lBQ3ZCLDBDQUErQzs7SUFDL0MsMkNBQWdEOzs7OztJQXNCcEMseUNBQWtCOzs7OztJQUFFLCtDQUFxQyIsInNvdXJjZXNDb250ZW50IjpbImltcG9ydCB7XG4gIENoYW5nZURldGVjdGlvblN0cmF0ZWd5LFxuICBDb21wb25lbnQsXG4gIEV2ZW50RW1pdHRlcixcbiAgSW5wdXQsXG4gIE9uSW5pdCxcbiAgT3V0cHV0LFxuICBWaWV3RW5jYXBzdWxhdGlvblxufSBmcm9tICdAYW5ndWxhci9jb3JlJztcbmltcG9ydCBzZXRNb250aCBmcm9tICdkYXRlLWZucy9zZXRfbW9udGgnO1xuaW1wb3J0IHsgRGF0ZUhlbHBlclNlcnZpY2UgfSBmcm9tICcuLi9pMThuL2RhdGUtaGVscGVyLnNlcnZpY2UnO1xuaW1wb3J0IHsgTnpJMThuU2VydmljZSBhcyBJMThuIH0gZnJvbSAnLi4vaTE4bi9uei1pMThuLnNlcnZpY2UnO1xuXG5AQ29tcG9uZW50KHtcbiAgZW5jYXBzdWxhdGlvbjogVmlld0VuY2Fwc3VsYXRpb24uTm9uZSxcbiAgY2hhbmdlRGV0ZWN0aW9uOiBDaGFuZ2VEZXRlY3Rpb25TdHJhdGVneS5PblB1c2gsXG4gIHNlbGVjdG9yOiAnbnotY2FsZW5kYXItaGVhZGVyJyxcbiAgdGVtcGxhdGVVcmw6ICcuL256LWNhbGVuZGFyLWhlYWRlci5jb21wb25lbnQuaHRtbCcsXG4gIGhvc3Q6IHtcbiAgICAnW3N0eWxlLmRpc3BsYXldJzogYCdibG9jaydgLFxuICAgICdbY2xhc3MuYW50LWZ1bGxjYWxlbmRhci1oZWFkZXJdJzogYHRydWVgXG4gIH1cbn0pXG5leHBvcnQgY2xhc3MgTnpDYWxlbmRhckhlYWRlckNvbXBvbmVudCBpbXBsZW1lbnRzIE9uSW5pdCB7XG4gIEBJbnB1dCgpIG1vZGU6ICdtb250aCcgfCAneWVhcicgPSAnbW9udGgnO1xuICBAT3V0cHV0KCkgcmVhZG9ubHkgbW9kZUNoYW5nZTogRXZlbnRFbWl0dGVyPCdtb250aCcgfCAneWVhcic+ID0gbmV3IEV2ZW50RW1pdHRlcigpO1xuXG4gIEBJbnB1dCgpIGZ1bGxzY3JlZW46IGJvb2xlYW4gPSB0cnVlO1xuXG4gIEBJbnB1dCgpXG4gIHNldCBhY3RpdmVEYXRlKHZhbHVlOiBEYXRlKSB7XG4gICAgdGhpcy5fYWN0aXZlRGF0ZSA9IHZhbHVlO1xuICAgIHRoaXMuc2V0VXBZZWFycygpO1xuICB9XG5cbiAgZ2V0IGFjdGl2ZURhdGUoKTogRGF0ZSB7XG4gICAgcmV0dXJuIHRoaXMuX2FjdGl2ZURhdGU7XG4gIH1cblxuICBAT3V0cHV0KCkgcmVhZG9ubHkgeWVhckNoYW5nZTogRXZlbnRFbWl0dGVyPG51bWJlcj4gPSBuZXcgRXZlbnRFbWl0dGVyKCk7XG4gIEBPdXRwdXQoKSByZWFkb25seSBtb250aENoYW5nZTogRXZlbnRFbWl0dGVyPG51bWJlcj4gPSBuZXcgRXZlbnRFbWl0dGVyKCk7XG5cbiAgX2FjdGl2ZURhdGUgPSBuZXcgRGF0ZSgpO1xuICB5ZWFyT2Zmc2V0OiBudW1iZXIgPSAxMDtcbiAgeWVhclRvdGFsOiBudW1iZXIgPSAyMDtcbiAgeWVhcnM6IEFycmF5PHsgbGFiZWw6IHN0cmluZzsgdmFsdWU6IG51bWJlciB9PjtcbiAgbW9udGhzOiBBcnJheTx7IGxhYmVsOiBzdHJpbmc7IHZhbHVlOiBudW1iZXIgfT47XG5cbiAgZ2V0IGFjdGl2ZVllYXIoKTogbnVtYmVyIHtcbiAgICByZXR1cm4gdGhpcy5hY3RpdmVEYXRlLmdldEZ1bGxZZWFyKCk7XG4gIH1cblxuICBnZXQgYWN0aXZlTW9udGgoKTogbnVtYmVyIHtcbiAgICByZXR1cm4gdGhpcy5hY3RpdmVEYXRlLmdldE1vbnRoKCk7XG4gIH1cblxuICBnZXQgc2l6ZSgpOiBzdHJpbmcge1xuICAgIHJldHVybiB0aGlzLmZ1bGxzY3JlZW4gPyAnZGVmYXVsdCcgOiAnc21hbGwnO1xuICB9XG5cbiAgZ2V0IHllYXJUeXBlVGV4dCgpOiBzdHJpbmcge1xuICAgIHJldHVybiB0aGlzLmkxOG4uZ2V0TG9jYWxlKCkuQ2FsZW5kYXIueWVhcjtcbiAgfVxuXG4gIGdldCBtb250aFR5cGVUZXh0KCk6IHN0cmluZyB7XG4gICAgcmV0dXJuIHRoaXMuaTE4bi5nZXRMb2NhbGUoKS5DYWxlbmRhci5tb250aDtcbiAgfVxuXG4gIGNvbnN0cnVjdG9yKHByaXZhdGUgaTE4bjogSTE4biwgcHJpdmF0ZSBkYXRlSGVscGVyOiBEYXRlSGVscGVyU2VydmljZSkge31cblxuICBuZ09uSW5pdCgpOiB2b2lkIHtcbiAgICB0aGlzLnNldFVwWWVhcnMoKTtcbiAgICB0aGlzLnNldFVwTW9udGhzKCk7XG4gIH1cblxuICB1cGRhdGVZZWFyKHllYXI6IG51bWJlcik6IHZvaWQge1xuICAgIHRoaXMueWVhckNoYW5nZS5lbWl0KHllYXIpO1xuICAgIHRoaXMuc2V0VXBZZWFycyh5ZWFyKTtcbiAgfVxuXG4gIHByaXZhdGUgc2V0VXBZZWFycyh5ZWFyPzogbnVtYmVyKTogdm9pZCB7XG4gICAgY29uc3Qgc3RhcnQgPSAoeWVhciB8fCB0aGlzLmFjdGl2ZVllYXIpIC0gdGhpcy55ZWFyT2Zmc2V0O1xuICAgIGNvbnN0IGVuZCA9IHN0YXJ0ICsgdGhpcy55ZWFyVG90YWw7XG5cbiAgICB0aGlzLnllYXJzID0gW107XG4gICAgZm9yIChsZXQgaSA9IHN0YXJ0OyBpIDwgZW5kOyBpKyspIHtcbiAgICAgIHRoaXMueWVhcnMucHVzaCh7IGxhYmVsOiBgJHtpfWAsIHZhbHVlOiBpIH0pO1xuICAgIH1cbiAgfVxuXG4gIHByaXZhdGUgc2V0VXBNb250aHMoKTogdm9pZCB7XG4gICAgdGhpcy5tb250aHMgPSBbXTtcblxuICAgIGZvciAobGV0IGkgPSAwOyBpIDwgMTI7IGkrKykge1xuICAgICAgY29uc3QgZGF0ZUluTW9udGggPSBzZXRNb250aCh0aGlzLmFjdGl2ZURhdGUsIGkpO1xuICAgICAgY29uc3QgbW9udGhUZXh0ID0gdGhpcy5kYXRlSGVscGVyLmZvcm1hdChkYXRlSW5Nb250aCwgJ01NTScpO1xuICAgICAgdGhpcy5tb250aHMucHVzaCh7IGxhYmVsOiBtb250aFRleHQsIHZhbHVlOiBpIH0pO1xuICAgIH1cbiAgfVxufVxuIl19