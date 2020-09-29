/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { ChangeDetectionStrategy, Component, EventEmitter, Input, Output, ViewEncapsulation } from '@angular/core';
import { DateHelperService } from '../../../i18n/date-helper.service';
import { CandyDate } from '../candy-date';
export class CalendarInputComponent {
    /**
     * @param {?} dateHelper
     */
    constructor(dateHelper) {
        this.dateHelper = dateHelper;
        this.valueChange = new EventEmitter();
        this.prefixCls = 'ant-calendar';
        this.invalidInputClass = '';
    }
    /**
     * @return {?}
     */
    ngOnInit() { }
    /**
     * @param {?} event
     * @return {?}
     */
    onInputKeyup(event) {
        /** @type {?} */
        const date = this.checkValidInputDate(event);
        if (!date || (this.disabledDate && this.disabledDate(date.nativeDate))) {
            return;
        }
        if (!date.isSame(this.value, 'second')) { // Not same with original value
            this.value = date;
            this.valueChange.emit(this.value);
        }
    }
    /**
     * @param {?} value
     * @return {?}
     */
    toReadableInput(value) {
        return value ? this.dateHelper.format(value.nativeDate, this.format) : '';
    }
    /**
     * @private
     * @param {?} event
     * @return {?}
     */
    checkValidInputDate(event) {
        /** @type {?} */
        const input = ((/** @type {?} */ (event.target))).value;
        /** @type {?} */
        const date = new CandyDate(input);
        this.invalidInputClass = '';
        if (date.isInvalid() || input !== this.toReadableInput(date)) { // Should also match the input format exactly
            this.invalidInputClass = `${this.prefixCls}-input-invalid`;
            return null;
        }
        return date;
    }
}
CalendarInputComponent.decorators = [
    { type: Component, args: [{
                encapsulation: ViewEncapsulation.None,
                changeDetection: ChangeDetectionStrategy.OnPush,
                // tslint:disable-next-line:component-selector
                selector: 'calendar-input',
                template: "<div class=\"{{ prefixCls }}-input-wrap\">\n  <div class=\"{{ prefixCls }}-date-input-wrap\">\n    <input\n      class=\"{{ prefixCls }}-input {{ invalidInputClass }}\"\n      placeholder=\"{{ placeholder || locale.dateSelect }}\"\n      value=\"{{ toReadableInput(value) }}\"\n      (keyup)=\"onInputKeyup($event)\"\n    />\n  </div>\n  <a class=\"{{ prefixCls }}-clear-btn\" role=\"button\" title=\"{{ locale.clear }}\">\n    <!--<i nz-icon type=\"close\"></i>-->\n  </a>\n</div>"
            }] }
];
/** @nocollapse */
CalendarInputComponent.ctorParameters = () => [
    { type: DateHelperService }
];
CalendarInputComponent.propDecorators = {
    locale: [{ type: Input }],
    format: [{ type: Input }],
    placeholder: [{ type: Input }],
    disabledDate: [{ type: Input }],
    value: [{ type: Input }],
    valueChange: [{ type: Output }]
};
if (false) {
    /** @type {?} */
    CalendarInputComponent.prototype.locale;
    /** @type {?} */
    CalendarInputComponent.prototype.format;
    /** @type {?} */
    CalendarInputComponent.prototype.placeholder;
    /** @type {?} */
    CalendarInputComponent.prototype.disabledDate;
    /** @type {?} */
    CalendarInputComponent.prototype.value;
    /** @type {?} */
    CalendarInputComponent.prototype.valueChange;
    /** @type {?} */
    CalendarInputComponent.prototype.prefixCls;
    /** @type {?} */
    CalendarInputComponent.prototype.invalidInputClass;
    /**
     * @type {?}
     * @private
     */
    CalendarInputComponent.prototype.dateHelper;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiY2FsZW5kYXItaW5wdXQuY29tcG9uZW50LmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbImRhdGUtcGlja2VyL2xpYi9jYWxlbmRhci9jYWxlbmRhci1pbnB1dC5jb21wb25lbnQudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7OztBQUFBLE9BQU8sRUFBRSx1QkFBdUIsRUFBRSxTQUFTLEVBQUUsWUFBWSxFQUFFLEtBQUssRUFBVSxNQUFNLEVBQUUsaUJBQWlCLEVBQUUsTUFBTSxlQUFlLENBQUM7QUFFM0gsT0FBTyxFQUFFLGlCQUFpQixFQUFFLE1BQU0sbUNBQW1DLENBQUM7QUFFdEUsT0FBTyxFQUFFLFNBQVMsRUFBRSxNQUFNLGVBQWUsQ0FBQztBQVUxQyxNQUFNLE9BQU8sc0JBQXNCOzs7O0lBWWpDLFlBQW9CLFVBQTZCO1FBQTdCLGVBQVUsR0FBVixVQUFVLENBQW1CO1FBTDlCLGdCQUFXLEdBQUcsSUFBSSxZQUFZLEVBQWEsQ0FBQztRQUUvRCxjQUFTLEdBQVcsY0FBYyxDQUFDO1FBQ25DLHNCQUFpQixHQUFXLEVBQUUsQ0FBQztJQUVzQixDQUFDOzs7O0lBRXRELFFBQVEsS0FBVyxDQUFDOzs7OztJQUVwQixZQUFZLENBQUMsS0FBWTs7Y0FDakIsSUFBSSxHQUFHLElBQUksQ0FBQyxtQkFBbUIsQ0FBQyxLQUFLLENBQUM7UUFFNUMsSUFBSSxDQUFDLElBQUksSUFBSSxDQUFDLElBQUksQ0FBQyxZQUFZLElBQUksSUFBSSxDQUFDLFlBQVksQ0FBQyxJQUFJLENBQUMsVUFBVSxDQUFDLENBQUMsRUFBRTtZQUN0RSxPQUFRO1NBQ1Q7UUFFRCxJQUFJLENBQUMsSUFBSSxDQUFDLE1BQU0sQ0FBQyxJQUFJLENBQUMsS0FBSyxFQUFFLFFBQVEsQ0FBQyxFQUFFLEVBQUUsK0JBQStCO1lBQ3ZFLElBQUksQ0FBQyxLQUFLLEdBQUcsSUFBSSxDQUFDO1lBQ2xCLElBQUksQ0FBQyxXQUFXLENBQUMsSUFBSSxDQUFDLElBQUksQ0FBQyxLQUFLLENBQUMsQ0FBQztTQUNuQztJQUNILENBQUM7Ozs7O0lBRUQsZUFBZSxDQUFDLEtBQWdCO1FBQzlCLE9BQU8sS0FBSyxDQUFDLENBQUMsQ0FBQyxJQUFJLENBQUMsVUFBVSxDQUFDLE1BQU0sQ0FBQyxLQUFLLENBQUMsVUFBVSxFQUFFLElBQUksQ0FBQyxNQUFNLENBQUMsQ0FBQyxDQUFDLENBQUMsRUFBRSxDQUFDO0lBQzVFLENBQUM7Ozs7OztJQUVPLG1CQUFtQixDQUFDLEtBQVk7O2NBQ2hDLEtBQUssR0FBRyxDQUFDLG1CQUFBLEtBQUssQ0FBQyxNQUFNLEVBQW9CLENBQUMsQ0FBQyxLQUFLOztjQUNoRCxJQUFJLEdBQUcsSUFBSSxTQUFTLENBQUMsS0FBSyxDQUFDO1FBRWpDLElBQUksQ0FBQyxpQkFBaUIsR0FBRyxFQUFFLENBQUM7UUFDNUIsSUFBSSxJQUFJLENBQUMsU0FBUyxFQUFFLElBQUksS0FBSyxLQUFLLElBQUksQ0FBQyxlQUFlLENBQUMsSUFBSSxDQUFDLEVBQUUsRUFBRSw2Q0FBNkM7WUFDM0csSUFBSSxDQUFDLGlCQUFpQixHQUFHLEdBQUcsSUFBSSxDQUFDLFNBQVMsZ0JBQWdCLENBQUM7WUFDM0QsT0FBTyxJQUFJLENBQUM7U0FDYjtRQUVELE9BQU8sSUFBSSxDQUFDO0lBQ2QsQ0FBQzs7O1lBcERGLFNBQVMsU0FBQztnQkFDVCxhQUFhLEVBQUUsaUJBQWlCLENBQUMsSUFBSTtnQkFDckMsZUFBZSxFQUFFLHVCQUF1QixDQUFDLE1BQU07O2dCQUUvQyxRQUFRLEVBQUUsZ0JBQWdCO2dCQUMxQiw2ZUFBNEM7YUFDN0M7Ozs7WUFWUSxpQkFBaUI7OztxQkFhdkIsS0FBSztxQkFDTCxLQUFLOzBCQUNMLEtBQUs7MkJBQ0wsS0FBSztvQkFFTCxLQUFLOzBCQUNMLE1BQU07Ozs7SUFOUCx3Q0FBeUM7O0lBQ3pDLHdDQUF3Qjs7SUFDeEIsNkNBQTZCOztJQUM3Qiw4Q0FBNEM7O0lBRTVDLHVDQUEwQjs7SUFDMUIsNkNBQStEOztJQUUvRCwyQ0FBbUM7O0lBQ25DLG1EQUErQjs7Ozs7SUFFbkIsNENBQXFDIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHsgQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3ksIENvbXBvbmVudCwgRXZlbnRFbWl0dGVyLCBJbnB1dCwgT25Jbml0LCBPdXRwdXQsIFZpZXdFbmNhcHN1bGF0aW9uIH0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5cbmltcG9ydCB7IERhdGVIZWxwZXJTZXJ2aWNlIH0gZnJvbSAnLi4vLi4vLi4vaTE4bi9kYXRlLWhlbHBlci5zZXJ2aWNlJztcbmltcG9ydCB7IE56Q2FsZW5kYXJJMThuSW50ZXJmYWNlIH0gZnJvbSAnLi4vLi4vLi4vaTE4bi9uei1pMThuLmludGVyZmFjZSc7XG5pbXBvcnQgeyBDYW5keURhdGUgfSBmcm9tICcuLi9jYW5keS1kYXRlJztcblxuQENvbXBvbmVudCh7XG4gIGVuY2Fwc3VsYXRpb246IFZpZXdFbmNhcHN1bGF0aW9uLk5vbmUsXG4gIGNoYW5nZURldGVjdGlvbjogQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3kuT25QdXNoLFxuICAvLyB0c2xpbnQ6ZGlzYWJsZS1uZXh0LWxpbmU6Y29tcG9uZW50LXNlbGVjdG9yXG4gIHNlbGVjdG9yOiAnY2FsZW5kYXItaW5wdXQnLFxuICB0ZW1wbGF0ZVVybDogJ2NhbGVuZGFyLWlucHV0LmNvbXBvbmVudC5odG1sJ1xufSlcblxuZXhwb3J0IGNsYXNzIENhbGVuZGFySW5wdXRDb21wb25lbnQgaW1wbGVtZW50cyBPbkluaXQge1xuICBASW5wdXQoKSBsb2NhbGU6IE56Q2FsZW5kYXJJMThuSW50ZXJmYWNlO1xuICBASW5wdXQoKSBmb3JtYXQ6IHN0cmluZztcbiAgQElucHV0KCkgcGxhY2Vob2xkZXI6IHN0cmluZztcbiAgQElucHV0KCkgZGlzYWJsZWREYXRlOiAoZDogRGF0ZSkgPT4gYm9vbGVhbjtcblxuICBASW5wdXQoKSB2YWx1ZTogQ2FuZHlEYXRlO1xuICBAT3V0cHV0KCkgcmVhZG9ubHkgdmFsdWVDaGFuZ2UgPSBuZXcgRXZlbnRFbWl0dGVyPENhbmR5RGF0ZT4oKTtcblxuICBwcmVmaXhDbHM6IHN0cmluZyA9ICdhbnQtY2FsZW5kYXInO1xuICBpbnZhbGlkSW5wdXRDbGFzczogc3RyaW5nID0gJyc7XG5cbiAgY29uc3RydWN0b3IocHJpdmF0ZSBkYXRlSGVscGVyOiBEYXRlSGVscGVyU2VydmljZSkgeyB9XG5cbiAgbmdPbkluaXQoKTogdm9pZCB7IH1cblxuICBvbklucHV0S2V5dXAoZXZlbnQ6IEV2ZW50KTogdm9pZCB7XG4gICAgY29uc3QgZGF0ZSA9IHRoaXMuY2hlY2tWYWxpZElucHV0RGF0ZShldmVudCk7XG5cbiAgICBpZiAoIWRhdGUgfHwgKHRoaXMuZGlzYWJsZWREYXRlICYmIHRoaXMuZGlzYWJsZWREYXRlKGRhdGUubmF0aXZlRGF0ZSkpKSB7XG4gICAgICByZXR1cm4gO1xuICAgIH1cblxuICAgIGlmICghZGF0ZS5pc1NhbWUodGhpcy52YWx1ZSwgJ3NlY29uZCcpKSB7IC8vIE5vdCBzYW1lIHdpdGggb3JpZ2luYWwgdmFsdWVcbiAgICAgIHRoaXMudmFsdWUgPSBkYXRlO1xuICAgICAgdGhpcy52YWx1ZUNoYW5nZS5lbWl0KHRoaXMudmFsdWUpO1xuICAgIH1cbiAgfVxuXG4gIHRvUmVhZGFibGVJbnB1dCh2YWx1ZTogQ2FuZHlEYXRlKTogc3RyaW5nIHtcbiAgICByZXR1cm4gdmFsdWUgPyB0aGlzLmRhdGVIZWxwZXIuZm9ybWF0KHZhbHVlLm5hdGl2ZURhdGUsIHRoaXMuZm9ybWF0KSA6ICcnO1xuICB9XG5cbiAgcHJpdmF0ZSBjaGVja1ZhbGlkSW5wdXREYXRlKGV2ZW50OiBFdmVudCk6IENhbmR5RGF0ZSB8IG51bGwge1xuICAgIGNvbnN0IGlucHV0ID0gKGV2ZW50LnRhcmdldCBhcyBIVE1MSW5wdXRFbGVtZW50KS52YWx1ZTtcbiAgICBjb25zdCBkYXRlID0gbmV3IENhbmR5RGF0ZShpbnB1dCk7XG5cbiAgICB0aGlzLmludmFsaWRJbnB1dENsYXNzID0gJyc7XG4gICAgaWYgKGRhdGUuaXNJbnZhbGlkKCkgfHwgaW5wdXQgIT09IHRoaXMudG9SZWFkYWJsZUlucHV0KGRhdGUpKSB7IC8vIFNob3VsZCBhbHNvIG1hdGNoIHRoZSBpbnB1dCBmb3JtYXQgZXhhY3RseVxuICAgICAgdGhpcy5pbnZhbGlkSW5wdXRDbGFzcyA9IGAke3RoaXMucHJlZml4Q2xzfS1pbnB1dC1pbnZhbGlkYDtcbiAgICAgIHJldHVybiBudWxsO1xuICAgIH1cblxuICAgIHJldHVybiBkYXRlO1xuICB9XG59XG4iXX0=