/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { ChangeDetectionStrategy, Component, EventEmitter, Input, Output, ViewEncapsulation } from '@angular/core';
import { DateHelperService } from '../../../i18n/date-helper.service';
import { CandyDate } from '../candy-date';
var TodayButtonComponent = /** @class */ (function () {
    function TodayButtonComponent(dateHelper) {
        this.dateHelper = dateHelper;
        this.hasTimePicker = false;
        this.clickToday = new EventEmitter();
        this.prefixCls = 'ant-calendar';
        this.isDisabled = false;
        this.now = new CandyDate();
    }
    /**
     * @return {?}
     */
    TodayButtonComponent.prototype.ngOnInit = /**
     * @return {?}
     */
    function () { };
    /**
     * @param {?} changes
     * @return {?}
     */
    TodayButtonComponent.prototype.ngOnChanges = /**
     * @param {?} changes
     * @return {?}
     */
    function (changes) {
        if (changes.disabledDate) {
            this.isDisabled = this.disabledDate && this.disabledDate(this.now.nativeDate);
        }
        if (changes.locale) {
            // NOTE: Compat for DatePipe formatting rules
            /** @type {?} */
            var dateFormat = this.locale.dateFormat;
            if (this.dateHelper.relyOnDatePipe) {
                dateFormat = ((/** @type {?} */ (this.dateHelper))).transCompatFormat(dateFormat);
            }
            this.title = this.dateHelper.format(this.now.nativeDate, dateFormat);
        }
    };
    /**
     * @return {?}
     */
    TodayButtonComponent.prototype.onClickToday = /**
     * @return {?}
     */
    function () {
        this.clickToday.emit(this.now.clone()); // To prevent the "now" being modified from outside, we use clone
    };
    TodayButtonComponent.decorators = [
        { type: Component, args: [{
                    encapsulation: ViewEncapsulation.None,
                    changeDetection: ChangeDetectionStrategy.OnPush,
                    // tslint:disable-next-line:component-selector
                    selector: 'today-button',
                    template: "<a\n  class=\"{{ prefixCls }}-today-btn {{ isDisabled ? prefixCls + '-today-btn-disabled' : '' }}\"\n  role=\"button\"\n  (click)=\"isDisabled ? null : onClickToday()\"\n  title=\"{{ title }}\"\n>\n  {{ hasTimePicker ? locale.now : locale.today }}\n</a>"
                }] }
    ];
    /** @nocollapse */
    TodayButtonComponent.ctorParameters = function () { return [
        { type: DateHelperService }
    ]; };
    TodayButtonComponent.propDecorators = {
        locale: [{ type: Input }],
        hasTimePicker: [{ type: Input }],
        disabledDate: [{ type: Input }],
        clickToday: [{ type: Output }]
    };
    return TodayButtonComponent;
}());
export { TodayButtonComponent };
if (false) {
    /** @type {?} */
    TodayButtonComponent.prototype.locale;
    /** @type {?} */
    TodayButtonComponent.prototype.hasTimePicker;
    /** @type {?} */
    TodayButtonComponent.prototype.disabledDate;
    /** @type {?} */
    TodayButtonComponent.prototype.clickToday;
    /** @type {?} */
    TodayButtonComponent.prototype.prefixCls;
    /** @type {?} */
    TodayButtonComponent.prototype.isDisabled;
    /** @type {?} */
    TodayButtonComponent.prototype.title;
    /**
     * @type {?}
     * @private
     */
    TodayButtonComponent.prototype.now;
    /**
     * @type {?}
     * @private
     */
    TodayButtonComponent.prototype.dateHelper;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoidG9kYXktYnV0dG9uLmNvbXBvbmVudC5qcyIsInNvdXJjZVJvb3QiOiJuZzovL25nLXpvcnJvLWFudGQvIiwic291cmNlcyI6WyJkYXRlLXBpY2tlci9saWIvY2FsZW5kYXIvdG9kYXktYnV0dG9uLmNvbXBvbmVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7O0FBQUEsT0FBTyxFQUFFLHVCQUF1QixFQUFFLFNBQVMsRUFBRSxZQUFZLEVBQUUsS0FBSyxFQUFxQixNQUFNLEVBQWlCLGlCQUFpQixFQUFFLE1BQU0sZUFBZSxDQUFDO0FBRXJKLE9BQU8sRUFBd0IsaUJBQWlCLEVBQUUsTUFBTSxtQ0FBbUMsQ0FBQztBQUU1RixPQUFPLEVBQUUsU0FBUyxFQUFFLE1BQU0sZUFBZSxDQUFDO0FBRTFDO0lBcUJFLDhCQUFvQixVQUE2QjtRQUE3QixlQUFVLEdBQVYsVUFBVSxDQUFtQjtRQVh4QyxrQkFBYSxHQUFZLEtBQUssQ0FBQztRQUdyQixlQUFVLEdBQUcsSUFBSSxZQUFZLEVBQWEsQ0FBQztRQUU5RCxjQUFTLEdBQVcsY0FBYyxDQUFDO1FBQ25DLGVBQVUsR0FBWSxLQUFLLENBQUM7UUFHcEIsUUFBRyxHQUFjLElBQUksU0FBUyxFQUFFLENBQUM7SUFFWSxDQUFDOzs7O0lBRXRELHVDQUFROzs7SUFBUixjQUFtQixDQUFDOzs7OztJQUVwQiwwQ0FBVzs7OztJQUFYLFVBQVksT0FBc0I7UUFDaEMsSUFBSSxPQUFPLENBQUMsWUFBWSxFQUFFO1lBQ3hCLElBQUksQ0FBQyxVQUFVLEdBQUcsSUFBSSxDQUFDLFlBQVksSUFBSSxJQUFJLENBQUMsWUFBWSxDQUFDLElBQUksQ0FBQyxHQUFHLENBQUMsVUFBVSxDQUFDLENBQUM7U0FDL0U7UUFDRCxJQUFJLE9BQU8sQ0FBQyxNQUFNLEVBQUU7OztnQkFFZCxVQUFVLEdBQVcsSUFBSSxDQUFDLE1BQU0sQ0FBQyxVQUFVO1lBQy9DLElBQUksSUFBSSxDQUFDLFVBQVUsQ0FBQyxjQUFjLEVBQUU7Z0JBQ2xDLFVBQVUsR0FBRyxDQUFDLG1CQUFBLElBQUksQ0FBQyxVQUFVLEVBQXdCLENBQUMsQ0FBQyxpQkFBaUIsQ0FBQyxVQUFVLENBQUMsQ0FBQzthQUN0RjtZQUNELElBQUksQ0FBQyxLQUFLLEdBQUcsSUFBSSxDQUFDLFVBQVUsQ0FBQyxNQUFNLENBQUMsSUFBSSxDQUFDLEdBQUcsQ0FBQyxVQUFVLEVBQUUsVUFBVSxDQUFDLENBQUM7U0FDdEU7SUFDSCxDQUFDOzs7O0lBRUQsMkNBQVk7OztJQUFaO1FBQ0UsSUFBSSxDQUFDLFVBQVUsQ0FBQyxJQUFJLENBQUMsSUFBSSxDQUFDLEdBQUcsQ0FBQyxLQUFLLEVBQUUsQ0FBQyxDQUFDLENBQUMsaUVBQWlFO0lBQzNHLENBQUM7O2dCQXpDRixTQUFTLFNBQUM7b0JBQ1QsYUFBYSxFQUFFLGlCQUFpQixDQUFDLElBQUk7b0JBQ3JDLGVBQWUsRUFBRSx1QkFBdUIsQ0FBQyxNQUFNOztvQkFFL0MsUUFBUSxFQUFFLGNBQWM7b0JBQ3hCLHlRQUEwQztpQkFDM0M7Ozs7Z0JBVjhCLGlCQUFpQjs7O3lCQWE3QyxLQUFLO2dDQUNMLEtBQUs7K0JBQ0wsS0FBSzs2QkFFTCxNQUFNOztJQTZCVCwyQkFBQztDQUFBLEFBMUNELElBMENDO1NBbENZLG9CQUFvQjs7O0lBQy9CLHNDQUF5Qzs7SUFDekMsNkNBQXdDOztJQUN4Qyw0Q0FBNEM7O0lBRTVDLDBDQUE4RDs7SUFFOUQseUNBQW1DOztJQUNuQywwQ0FBNEI7O0lBQzVCLHFDQUFjOzs7OztJQUVkLG1DQUF5Qzs7Ozs7SUFFN0IsMENBQXFDIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHsgQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3ksIENvbXBvbmVudCwgRXZlbnRFbWl0dGVyLCBJbnB1dCwgT25DaGFuZ2VzLCBPbkluaXQsIE91dHB1dCwgU2ltcGxlQ2hhbmdlcywgVmlld0VuY2Fwc3VsYXRpb24gfSBmcm9tICdAYW5ndWxhci9jb3JlJztcblxuaW1wb3J0IHsgRGF0ZUhlbHBlckJ5RGF0ZVBpcGUsIERhdGVIZWxwZXJTZXJ2aWNlIH0gZnJvbSAnLi4vLi4vLi4vaTE4bi9kYXRlLWhlbHBlci5zZXJ2aWNlJztcbmltcG9ydCB7IE56Q2FsZW5kYXJJMThuSW50ZXJmYWNlIH0gZnJvbSAnLi4vLi4vLi4vaTE4bi9uei1pMThuLmludGVyZmFjZSc7XG5pbXBvcnQgeyBDYW5keURhdGUgfSBmcm9tICcuLi9jYW5keS1kYXRlJztcblxuQENvbXBvbmVudCh7XG4gIGVuY2Fwc3VsYXRpb246IFZpZXdFbmNhcHN1bGF0aW9uLk5vbmUsXG4gIGNoYW5nZURldGVjdGlvbjogQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3kuT25QdXNoLFxuICAvLyB0c2xpbnQ6ZGlzYWJsZS1uZXh0LWxpbmU6Y29tcG9uZW50LXNlbGVjdG9yXG4gIHNlbGVjdG9yOiAndG9kYXktYnV0dG9uJyxcbiAgdGVtcGxhdGVVcmw6ICd0b2RheS1idXR0b24uY29tcG9uZW50Lmh0bWwnXG59KVxuXG5leHBvcnQgY2xhc3MgVG9kYXlCdXR0b25Db21wb25lbnQgaW1wbGVtZW50cyBPbkluaXQsIE9uQ2hhbmdlcyB7XG4gIEBJbnB1dCgpIGxvY2FsZTogTnpDYWxlbmRhckkxOG5JbnRlcmZhY2U7XG4gIEBJbnB1dCgpIGhhc1RpbWVQaWNrZXI6IGJvb2xlYW4gPSBmYWxzZTtcbiAgQElucHV0KCkgZGlzYWJsZWREYXRlOiAoZDogRGF0ZSkgPT4gYm9vbGVhbjtcblxuICBAT3V0cHV0KCkgcmVhZG9ubHkgY2xpY2tUb2RheSA9IG5ldyBFdmVudEVtaXR0ZXI8Q2FuZHlEYXRlPigpO1xuXG4gIHByZWZpeENsczogc3RyaW5nID0gJ2FudC1jYWxlbmRhcic7XG4gIGlzRGlzYWJsZWQ6IGJvb2xlYW4gPSBmYWxzZTtcbiAgdGl0bGU6IHN0cmluZztcblxuICBwcml2YXRlIG5vdzogQ2FuZHlEYXRlID0gbmV3IENhbmR5RGF0ZSgpO1xuXG4gIGNvbnN0cnVjdG9yKHByaXZhdGUgZGF0ZUhlbHBlcjogRGF0ZUhlbHBlclNlcnZpY2UpIHsgfVxuXG4gIG5nT25Jbml0KCk6IHZvaWQgeyB9XG5cbiAgbmdPbkNoYW5nZXMoY2hhbmdlczogU2ltcGxlQ2hhbmdlcyk6IHZvaWQge1xuICAgIGlmIChjaGFuZ2VzLmRpc2FibGVkRGF0ZSkge1xuICAgICAgdGhpcy5pc0Rpc2FibGVkID0gdGhpcy5kaXNhYmxlZERhdGUgJiYgdGhpcy5kaXNhYmxlZERhdGUodGhpcy5ub3cubmF0aXZlRGF0ZSk7XG4gICAgfVxuICAgIGlmIChjaGFuZ2VzLmxvY2FsZSkge1xuICAgICAgLy8gTk9URTogQ29tcGF0IGZvciBEYXRlUGlwZSBmb3JtYXR0aW5nIHJ1bGVzXG4gICAgICBsZXQgZGF0ZUZvcm1hdDogc3RyaW5nID0gdGhpcy5sb2NhbGUuZGF0ZUZvcm1hdDtcbiAgICAgIGlmICh0aGlzLmRhdGVIZWxwZXIucmVseU9uRGF0ZVBpcGUpIHtcbiAgICAgICAgZGF0ZUZvcm1hdCA9ICh0aGlzLmRhdGVIZWxwZXIgYXMgRGF0ZUhlbHBlckJ5RGF0ZVBpcGUpLnRyYW5zQ29tcGF0Rm9ybWF0KGRhdGVGb3JtYXQpO1xuICAgICAgfVxuICAgICAgdGhpcy50aXRsZSA9IHRoaXMuZGF0ZUhlbHBlci5mb3JtYXQodGhpcy5ub3cubmF0aXZlRGF0ZSwgZGF0ZUZvcm1hdCk7XG4gICAgfVxuICB9XG5cbiAgb25DbGlja1RvZGF5KCk6IHZvaWQge1xuICAgIHRoaXMuY2xpY2tUb2RheS5lbWl0KHRoaXMubm93LmNsb25lKCkpOyAvLyBUbyBwcmV2ZW50IHRoZSBcIm5vd1wiIGJlaW5nIG1vZGlmaWVkIGZyb20gb3V0c2lkZSwgd2UgdXNlIGNsb25lXG4gIH1cbn1cbiJdfQ==