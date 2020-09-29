/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { ChangeDetectionStrategy, Component, EventEmitter, Input, Output, ViewEncapsulation } from '@angular/core';
export class TimePickerButtonComponent {
    constructor() {
        this.timePickerDisabled = false;
        this.showTimePicker = false;
        this.showTimePickerChange = new EventEmitter();
        this.prefixCls = 'ant-calendar';
    }
    /**
     * @return {?}
     */
    onClick() {
        this.showTimePicker = !this.showTimePicker;
        this.showTimePickerChange.emit(this.showTimePicker);
    }
}
TimePickerButtonComponent.decorators = [
    { type: Component, args: [{
                encapsulation: ViewEncapsulation.None,
                changeDetection: ChangeDetectionStrategy.OnPush,
                // tslint:disable-next-line:component-selector
                selector: 'time-picker-button',
                template: "<a\n  class=\"{{ prefixCls }}-time-picker-btn {{ timePickerDisabled ? prefixCls + '-time-picker-btn-disabled' : '' }}\"\n  role=\"button\"\n  (click)=\"timePickerDisabled ? null : onClick()\"\n>\n  {{ showTimePicker ? locale.dateSelect : locale.timeSelect }}\n</a>"
            }] }
];
TimePickerButtonComponent.propDecorators = {
    locale: [{ type: Input }],
    timePickerDisabled: [{ type: Input }],
    showTimePicker: [{ type: Input }],
    showTimePickerChange: [{ type: Output }]
};
if (false) {
    /** @type {?} */
    TimePickerButtonComponent.prototype.locale;
    /** @type {?} */
    TimePickerButtonComponent.prototype.timePickerDisabled;
    /** @type {?} */
    TimePickerButtonComponent.prototype.showTimePicker;
    /** @type {?} */
    TimePickerButtonComponent.prototype.showTimePickerChange;
    /** @type {?} */
    TimePickerButtonComponent.prototype.prefixCls;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoidGltZS1waWNrZXItYnV0dG9uLmNvbXBvbmVudC5qcyIsInNvdXJjZVJvb3QiOiJuZzovL25nLXpvcnJvLWFudGQvIiwic291cmNlcyI6WyJkYXRlLXBpY2tlci9saWIvY2FsZW5kYXIvdGltZS1waWNrZXItYnV0dG9uLmNvbXBvbmVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7O0FBQUEsT0FBTyxFQUFFLHVCQUF1QixFQUFFLFNBQVMsRUFBRSxZQUFZLEVBQUUsS0FBSyxFQUFFLE1BQU0sRUFBRSxpQkFBaUIsRUFBRSxNQUFNLGVBQWUsQ0FBQztBQVluSCxNQUFNLE9BQU8seUJBQXlCO0lBUnRDO1FBVVcsdUJBQWtCLEdBQVksS0FBSyxDQUFDO1FBRXBDLG1CQUFjLEdBQVksS0FBSyxDQUFDO1FBQ3RCLHlCQUFvQixHQUFHLElBQUksWUFBWSxFQUFXLENBQUM7UUFFdEUsY0FBUyxHQUFXLGNBQWMsQ0FBQztJQU1yQyxDQUFDOzs7O0lBSkMsT0FBTztRQUNMLElBQUksQ0FBQyxjQUFjLEdBQUcsQ0FBQyxJQUFJLENBQUMsY0FBYyxDQUFDO1FBQzNDLElBQUksQ0FBQyxvQkFBb0IsQ0FBQyxJQUFJLENBQUMsSUFBSSxDQUFDLGNBQWMsQ0FBQyxDQUFDO0lBQ3RELENBQUM7OztZQXBCRixTQUFTLFNBQUM7Z0JBQ1QsYUFBYSxFQUFFLGlCQUFpQixDQUFDLElBQUk7Z0JBQ3JDLGVBQWUsRUFBRSx1QkFBdUIsQ0FBQyxNQUFNOztnQkFFL0MsUUFBUSxFQUFFLG9CQUFvQjtnQkFDOUIsb1JBQWdEO2FBQ2pEOzs7cUJBR0UsS0FBSztpQ0FDTCxLQUFLOzZCQUVMLEtBQUs7bUNBQ0wsTUFBTTs7OztJQUpQLDJDQUF5Qzs7SUFDekMsdURBQTZDOztJQUU3QyxtREFBeUM7O0lBQ3pDLHlEQUFzRTs7SUFFdEUsOENBQW1DIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHsgQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3ksIENvbXBvbmVudCwgRXZlbnRFbWl0dGVyLCBJbnB1dCwgT3V0cHV0LCBWaWV3RW5jYXBzdWxhdGlvbiB9IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuXG5pbXBvcnQgeyBOekNhbGVuZGFySTE4bkludGVyZmFjZSB9IGZyb20gJy4uLy4uLy4uL2kxOG4vbnotaTE4bi5pbnRlcmZhY2UnO1xuXG5AQ29tcG9uZW50KHtcbiAgZW5jYXBzdWxhdGlvbjogVmlld0VuY2Fwc3VsYXRpb24uTm9uZSxcbiAgY2hhbmdlRGV0ZWN0aW9uOiBDaGFuZ2VEZXRlY3Rpb25TdHJhdGVneS5PblB1c2gsXG4gIC8vIHRzbGludDpkaXNhYmxlLW5leHQtbGluZTpjb21wb25lbnQtc2VsZWN0b3JcbiAgc2VsZWN0b3I6ICd0aW1lLXBpY2tlci1idXR0b24nLFxuICB0ZW1wbGF0ZVVybDogJ3RpbWUtcGlja2VyLWJ1dHRvbi5jb21wb25lbnQuaHRtbCdcbn0pXG5cbmV4cG9ydCBjbGFzcyBUaW1lUGlja2VyQnV0dG9uQ29tcG9uZW50IHtcbiAgQElucHV0KCkgbG9jYWxlOiBOekNhbGVuZGFySTE4bkludGVyZmFjZTtcbiAgQElucHV0KCkgdGltZVBpY2tlckRpc2FibGVkOiBib29sZWFuID0gZmFsc2U7XG5cbiAgQElucHV0KCkgc2hvd1RpbWVQaWNrZXI6IGJvb2xlYW4gPSBmYWxzZTtcbiAgQE91dHB1dCgpIHJlYWRvbmx5IHNob3dUaW1lUGlja2VyQ2hhbmdlID0gbmV3IEV2ZW50RW1pdHRlcjxib29sZWFuPigpO1xuXG4gIHByZWZpeENsczogc3RyaW5nID0gJ2FudC1jYWxlbmRhcic7XG5cbiAgb25DbGljaygpOiB2b2lkIHtcbiAgICB0aGlzLnNob3dUaW1lUGlja2VyID0gIXRoaXMuc2hvd1RpbWVQaWNrZXI7XG4gICAgdGhpcy5zaG93VGltZVBpY2tlckNoYW5nZS5lbWl0KHRoaXMuc2hvd1RpbWVQaWNrZXIpO1xuICB9XG59XG4iXX0=