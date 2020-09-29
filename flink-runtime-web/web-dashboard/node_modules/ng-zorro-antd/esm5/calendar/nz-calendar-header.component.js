/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { ChangeDetectionStrategy, Component, EventEmitter, Input, Output, ViewEncapsulation } from '@angular/core';
import setMonth from 'date-fns/set_month';
import { DateHelperService } from '../i18n/date-helper.service';
import { NzI18nService as I18n } from '../i18n/nz-i18n.service';
var NzCalendarHeaderComponent = /** @class */ (function () {
    function NzCalendarHeaderComponent(i18n, dateHelper) {
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
    Object.defineProperty(NzCalendarHeaderComponent.prototype, "activeDate", {
        get: /**
         * @return {?}
         */
        function () {
            return this._activeDate;
        },
        set: /**
         * @param {?} value
         * @return {?}
         */
        function (value) {
            this._activeDate = value;
            this.setUpYears();
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzCalendarHeaderComponent.prototype, "activeYear", {
        get: /**
         * @return {?}
         */
        function () {
            return this.activeDate.getFullYear();
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzCalendarHeaderComponent.prototype, "activeMonth", {
        get: /**
         * @return {?}
         */
        function () {
            return this.activeDate.getMonth();
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzCalendarHeaderComponent.prototype, "size", {
        get: /**
         * @return {?}
         */
        function () {
            return this.fullscreen ? 'default' : 'small';
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzCalendarHeaderComponent.prototype, "yearTypeText", {
        get: /**
         * @return {?}
         */
        function () {
            return this.i18n.getLocale().Calendar.year;
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzCalendarHeaderComponent.prototype, "monthTypeText", {
        get: /**
         * @return {?}
         */
        function () {
            return this.i18n.getLocale().Calendar.month;
        },
        enumerable: true,
        configurable: true
    });
    /**
     * @return {?}
     */
    NzCalendarHeaderComponent.prototype.ngOnInit = /**
     * @return {?}
     */
    function () {
        this.setUpYears();
        this.setUpMonths();
    };
    /**
     * @param {?} year
     * @return {?}
     */
    NzCalendarHeaderComponent.prototype.updateYear = /**
     * @param {?} year
     * @return {?}
     */
    function (year) {
        this.yearChange.emit(year);
        this.setUpYears(year);
    };
    /**
     * @private
     * @param {?=} year
     * @return {?}
     */
    NzCalendarHeaderComponent.prototype.setUpYears = /**
     * @private
     * @param {?=} year
     * @return {?}
     */
    function (year) {
        /** @type {?} */
        var start = (year || this.activeYear) - this.yearOffset;
        /** @type {?} */
        var end = start + this.yearTotal;
        this.years = [];
        for (var i = start; i < end; i++) {
            this.years.push({ label: "" + i, value: i });
        }
    };
    /**
     * @private
     * @return {?}
     */
    NzCalendarHeaderComponent.prototype.setUpMonths = /**
     * @private
     * @return {?}
     */
    function () {
        this.months = [];
        for (var i = 0; i < 12; i++) {
            /** @type {?} */
            var dateInMonth = setMonth(this.activeDate, i);
            /** @type {?} */
            var monthText = this.dateHelper.format(dateInMonth, 'MMM');
            this.months.push({ label: monthText, value: i });
        }
    };
    NzCalendarHeaderComponent.decorators = [
        { type: Component, args: [{
                    encapsulation: ViewEncapsulation.None,
                    changeDetection: ChangeDetectionStrategy.OnPush,
                    selector: 'nz-calendar-header',
                    template: "<nz-select class=\"ant-fullcalendar-year-select\" [nzSize]=\"size\" [nzDropdownMatchSelectWidth]=\"false\"\n           [ngModel]=\"activeYear\" (ngModelChange)=\"updateYear($event)\">\n  <nz-option *ngFor=\"let year of years\" [nzLabel]=\"year.label\" [nzValue]=\"year.value\"></nz-option>\n</nz-select>\n\n<nz-select *ngIf=\"mode === 'month'\" class=\"ant-fullcalendar-month-select\" [nzSize]=\"size\" [nzDropdownMatchSelectWidth]=\"false\"\n           [ngModel]=\"activeMonth\" (ngModelChange)=\"monthChange.emit($event)\">\n  <nz-option *ngFor=\"let month of months\" [nzLabel]=\"month.label\" [nzValue]=\"month.value\"></nz-option>\n</nz-select>\n\n<nz-radio-group [(ngModel)]=\"mode\" (ngModelChange)=\"modeChange.emit($event)\" [nzSize]=\"size\">\n  <label nz-radio-button nzValue=\"month\">{{ monthTypeText }}</label>\n  <label nz-radio-button nzValue=\"year\">{{ yearTypeText }}</label>\n</nz-radio-group>\n",
                    host: {
                        '[style.display]': "'block'",
                        '[class.ant-fullcalendar-header]': "true"
                    }
                }] }
    ];
    /** @nocollapse */
    NzCalendarHeaderComponent.ctorParameters = function () { return [
        { type: I18n },
        { type: DateHelperService }
    ]; };
    NzCalendarHeaderComponent.propDecorators = {
        mode: [{ type: Input }],
        modeChange: [{ type: Output }],
        fullscreen: [{ type: Input }],
        activeDate: [{ type: Input }],
        yearChange: [{ type: Output }],
        monthChange: [{ type: Output }]
    };
    return NzCalendarHeaderComponent;
}());
export { NzCalendarHeaderComponent };
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
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotY2FsZW5kYXItaGVhZGVyLmNvbXBvbmVudC5qcyIsInNvdXJjZVJvb3QiOiJuZzovL25nLXpvcnJvLWFudGQvIiwic291cmNlcyI6WyJjYWxlbmRhci9uei1jYWxlbmRhci1oZWFkZXIuY29tcG9uZW50LnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7QUFBQSxPQUFPLEVBQ0wsdUJBQXVCLEVBQ3ZCLFNBQVMsRUFDVCxZQUFZLEVBQ1osS0FBSyxFQUVMLE1BQU0sRUFDTixpQkFBaUIsRUFDbEIsTUFBTSxlQUFlLENBQUM7QUFDdkIsT0FBTyxRQUFRLE1BQU0sb0JBQW9CLENBQUM7QUFDMUMsT0FBTyxFQUFFLGlCQUFpQixFQUFFLE1BQU0sNkJBQTZCLENBQUM7QUFDaEUsT0FBTyxFQUFFLGFBQWEsSUFBSSxJQUFJLEVBQUUsTUFBTSx5QkFBeUIsQ0FBQztBQUVoRTtJQXVERSxtQ0FBb0IsSUFBVSxFQUFVLFVBQTZCO1FBQWpELFNBQUksR0FBSixJQUFJLENBQU07UUFBVSxlQUFVLEdBQVYsVUFBVSxDQUFtQjtRQTVDNUQsU0FBSSxHQUFxQixPQUFPLENBQUM7UUFDdkIsZUFBVSxHQUFtQyxJQUFJLFlBQVksRUFBRSxDQUFDO1FBRTFFLGVBQVUsR0FBWSxJQUFJLENBQUM7UUFZakIsZUFBVSxHQUF5QixJQUFJLFlBQVksRUFBRSxDQUFDO1FBQ3RELGdCQUFXLEdBQXlCLElBQUksWUFBWSxFQUFFLENBQUM7UUFFMUUsZ0JBQVcsR0FBRyxJQUFJLElBQUksRUFBRSxDQUFDO1FBQ3pCLGVBQVUsR0FBVyxFQUFFLENBQUM7UUFDeEIsY0FBUyxHQUFXLEVBQUUsQ0FBQztJQXdCaUQsQ0FBQztJQXZDekUsc0JBQ0ksaURBQVU7Ozs7UUFLZDtZQUNFLE9BQU8sSUFBSSxDQUFDLFdBQVcsQ0FBQztRQUMxQixDQUFDOzs7OztRQVJELFVBQ2UsS0FBVztZQUN4QixJQUFJLENBQUMsV0FBVyxHQUFHLEtBQUssQ0FBQztZQUN6QixJQUFJLENBQUMsVUFBVSxFQUFF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Q0FBQztDQUFBLEFBdEZELElBc0ZDO1NBNUVZLHlCQUF5Qjs7O0lBQ3BDLHlDQUEwQzs7SUFDMUMsK0NBQW1GOztJQUVuRiwrQ0FBb0M7O0lBWXBDLCtDQUF5RTs7SUFDekUsZ0RBQTBFOztJQUUxRSxnREFBeUI7O0lBQ3pCLCtDQUF3Qjs7SUFDeEIsOENBQXVCOztJQUN2QiwwQ0FBK0M7O0lBQy9DLDJDQUFnRDs7Ozs7SUFzQnBDLHlDQUFrQjs7Ozs7SUFBRSwrQ0FBcUMiLCJzb3VyY2VzQ29udGVudCI6WyJpbXBvcnQge1xuICBDaGFuZ2VEZXRlY3Rpb25TdHJhdGVneSxcbiAgQ29tcG9uZW50LFxuICBFdmVudEVtaXR0ZXIsXG4gIElucHV0LFxuICBPbkluaXQsXG4gIE91dHB1dCxcbiAgVmlld0VuY2Fwc3VsYXRpb25cbn0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5pbXBvcnQgc2V0TW9udGggZnJvbSAnZGF0ZS1mbnMvc2V0X21vbnRoJztcbmltcG9ydCB7IERhdGVIZWxwZXJTZXJ2aWNlIH0gZnJvbSAnLi4vaTE4bi9kYXRlLWhlbHBlci5zZXJ2aWNlJztcbmltcG9ydCB7IE56STE4blNlcnZpY2UgYXMgSTE4biB9IGZyb20gJy4uL2kxOG4vbnotaTE4bi5zZXJ2aWNlJztcblxuQENvbXBvbmVudCh7XG4gIGVuY2Fwc3VsYXRpb246IFZpZXdFbmNhcHN1bGF0aW9uLk5vbmUsXG4gIGNoYW5nZURldGVjdGlvbjogQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3kuT25QdXNoLFxuICBzZWxlY3RvcjogJ256LWNhbGVuZGFyLWhlYWRlcicsXG4gIHRlbXBsYXRlVXJsOiAnLi9uei1jYWxlbmRhci1oZWFkZXIuY29tcG9uZW50Lmh0bWwnLFxuICBob3N0OiB7XG4gICAgJ1tzdHlsZS5kaXNwbGF5XSc6IGAnYmxvY2snYCxcbiAgICAnW2NsYXNzLmFudC1mdWxsY2FsZW5kYXItaGVhZGVyXSc6IGB0cnVlYFxuICB9XG59KVxuZXhwb3J0IGNsYXNzIE56Q2FsZW5kYXJIZWFkZXJDb21wb25lbnQgaW1wbGVtZW50cyBPbkluaXQge1xuICBASW5wdXQoKSBtb2RlOiAnbW9udGgnIHwgJ3llYXInID0gJ21vbnRoJztcbiAgQE91dHB1dCgpIHJlYWRvbmx5IG1vZGVDaGFuZ2U6IEV2ZW50RW1pdHRlcjwnbW9udGgnIHwgJ3llYXInPiA9IG5ldyBFdmVudEVtaXR0ZXIoKTtcblxuICBASW5wdXQoKSBmdWxsc2NyZWVuOiBib29sZWFuID0gdHJ1ZTtcblxuICBASW5wdXQoKVxuICBzZXQgYWN0aXZlRGF0ZSh2YWx1ZTogRGF0ZSkge1xuICAgIHRoaXMuX2FjdGl2ZURhdGUgPSB2YWx1ZTtcbiAgICB0aGlzLnNldFVwWWVhcnMoKTtcbiAgfVxuXG4gIGdldCBhY3RpdmVEYXRlKCk6IERhdGUge1xuICAgIHJldHVybiB0aGlzLl9hY3RpdmVEYXRlO1xuICB9XG5cbiAgQE91dHB1dCgpIHJlYWRvbmx5IHllYXJDaGFuZ2U6IEV2ZW50RW1pdHRlcjxudW1iZXI+ID0gbmV3IEV2ZW50RW1pdHRlcigpO1xuICBAT3V0cHV0KCkgcmVhZG9ubHkgbW9udGhDaGFuZ2U6IEV2ZW50RW1pdHRlcjxudW1iZXI+ID0gbmV3IEV2ZW50RW1pdHRlcigpO1xuXG4gIF9hY3RpdmVEYXRlID0gbmV3IERhdGUoKTtcbiAgeWVhck9mZnNldDogbnVtYmVyID0gMTA7XG4gIHllYXJUb3RhbDogbnVtYmVyID0gMjA7XG4gIHllYXJzOiBBcnJheTx7IGxhYmVsOiBzdHJpbmc7IHZhbHVlOiBudW1iZXIgfT47XG4gIG1vbnRoczogQXJyYXk8eyBsYWJlbDogc3RyaW5nOyB2YWx1ZTogbnVtYmVyIH0+O1xuXG4gIGdldCBhY3RpdmVZZWFyKCk6IG51bWJlciB7XG4gICAgcmV0dXJuIHRoaXMuYWN0aXZlRGF0ZS5nZXRGdWxsWWVhcigpO1xuICB9XG5cbiAgZ2V0IGFjdGl2ZU1vbnRoKCk6IG51bWJlciB7XG4gICAgcmV0dXJuIHRoaXMuYWN0aXZlRGF0ZS5nZXRNb250aCgpO1xuICB9XG5cbiAgZ2V0IHNpemUoKTogc3RyaW5nIHtcbiAgICByZXR1cm4gdGhpcy5mdWxsc2NyZWVuID8gJ2RlZmF1bHQnIDogJ3NtYWxsJztcbiAgfVxuXG4gIGdldCB5ZWFyVHlwZVRleHQoKTogc3RyaW5nIHtcbiAgICByZXR1cm4gdGhpcy5pMThuLmdldExvY2FsZSgpLkNhbGVuZGFyLnllYXI7XG4gIH1cblxuICBnZXQgbW9udGhUeXBlVGV4dCgpOiBzdHJpbmcge1xuICAgIHJldHVybiB0aGlzLmkxOG4uZ2V0TG9jYWxlKCkuQ2FsZW5kYXIubW9udGg7XG4gIH1cblxuICBjb25zdHJ1Y3Rvcihwcml2YXRlIGkxOG46IEkxOG4sIHByaXZhdGUgZGF0ZUhlbHBlcjogRGF0ZUhlbHBlclNlcnZpY2UpIHt9XG5cbiAgbmdPbkluaXQoKTogdm9pZCB7XG4gICAgdGhpcy5zZXRVcFllYXJzKCk7XG4gICAgdGhpcy5zZXRVcE1vbnRocygpO1xuICB9XG5cbiAgdXBkYXRlWWVhcih5ZWFyOiBudW1iZXIpOiB2b2lkIHtcbiAgICB0aGlzLnllYXJDaGFuZ2UuZW1pdCh5ZWFyKTtcbiAgICB0aGlzLnNldFVwWWVhcnMoeWVhcik7XG4gIH1cblxuICBwcml2YXRlIHNldFVwWWVhcnMoeWVhcj86IG51bWJlcik6IHZvaWQge1xuICAgIGNvbnN0IHN0YXJ0ID0gKHllYXIgfHwgdGhpcy5hY3RpdmVZZWFyKSAtIHRoaXMueWVhck9mZnNldDtcbiAgICBjb25zdCBlbmQgPSBzdGFydCArIHRoaXMueWVhclRvdGFsO1xuXG4gICAgdGhpcy55ZWFycyA9IFtdO1xuICAgIGZvciAobGV0IGkgPSBzdGFydDsgaSA8IGVuZDsgaSsrKSB7XG4gICAgICB0aGlzLnllYXJzLnB1c2goeyBsYWJlbDogYCR7aX1gLCB2YWx1ZTogaSB9KTtcbiAgICB9XG4gIH1cblxuICBwcml2YXRlIHNldFVwTW9udGhzKCk6IHZvaWQge1xuICAgIHRoaXMubW9udGhzID0gW107XG5cbiAgICBmb3IgKGxldCBpID0gMDsgaSA8IDEyOyBpKyspIHtcbiAgICAgIGNvbnN0IGRhdGVJbk1vbnRoID0gc2V0TW9udGgodGhpcy5hY3RpdmVEYXRlLCBpKTtcbiAgICAgIGNvbnN0IG1vbnRoVGV4dCA9IHRoaXMuZGF0ZUhlbHBlci5mb3JtYXQoZGF0ZUluTW9udGgsICdNTU0nKTtcbiAgICAgIHRoaXMubW9udGhzLnB1c2goeyBsYWJlbDogbW9udGhUZXh0LCB2YWx1ZTogaSB9KTtcbiAgICB9XG4gIH1cbn1cbiJdfQ==