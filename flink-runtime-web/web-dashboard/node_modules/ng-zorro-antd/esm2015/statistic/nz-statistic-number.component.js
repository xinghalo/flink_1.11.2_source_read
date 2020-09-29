/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { getLocaleNumberSymbol, NumberSymbol } from '@angular/common';
import { ChangeDetectionStrategy, Component, Inject, Input, LOCALE_ID, TemplateRef, ViewEncapsulation } from '@angular/core';
export class NzStatisticNumberComponent {
    /**
     * @param {?} locale_id
     */
    constructor(locale_id) {
        this.locale_id = locale_id;
        this.displayInt = '';
        this.displayDecimal = '';
    }
    /**
     * @return {?}
     */
    ngOnChanges() {
        this.formatNumber();
    }
    /**
     * @private
     * @return {?}
     */
    formatNumber() {
        /** @type {?} */
        const decimalSeparator = typeof this.nzValue === 'number' ? '.' : getLocaleNumberSymbol(this.locale_id, NumberSymbol.Decimal);
        /** @type {?} */
        const value = String(this.nzValue);
        const [int, decimal] = value.split(decimalSeparator);
        this.displayInt = int;
        this.displayDecimal = decimal ? `${decimalSeparator}${decimal}` : '';
    }
}
NzStatisticNumberComponent.decorators = [
    { type: Component, args: [{
                changeDetection: ChangeDetectionStrategy.OnPush,
                encapsulation: ViewEncapsulation.None,
                preserveWhitespaces: false,
                selector: 'nz-statistic-number',
                template: "<ng-container\n  *ngIf=\"nzValueTemplate\"\n  [ngTemplateOutlet]=\"nzValueTemplate\"\n  [ngTemplateOutletContext]=\"{ $implicit: nzValue }\">\n</ng-container>\n<ng-container *ngIf=\"!nzValueTemplate\">\n  <span *ngIf=\"displayInt\" class=\"ant-statistic-content-value-int\">{{ displayInt }}</span>\n  <span *ngIf=\"displayDecimal\" class=\"ant-statistic-content-value-decimal\">{{ displayDecimal }}</span>\n</ng-container>\n",
                host: {
                    class: 'ant-statistic-content-value'
                },
                styles: ['nz-number { display: inline }']
            }] }
];
/** @nocollapse */
NzStatisticNumberComponent.ctorParameters = () => [
    { type: String, decorators: [{ type: Inject, args: [LOCALE_ID,] }] }
];
NzStatisticNumberComponent.propDecorators = {
    nzValue: [{ type: Input }],
    nzValueTemplate: [{ type: Input }]
};
if (false) {
    /** @type {?} */
    NzStatisticNumberComponent.prototype.nzValue;
    /** @type {?} */
    NzStatisticNumberComponent.prototype.nzValueTemplate;
    /** @type {?} */
    NzStatisticNumberComponent.prototype.displayInt;
    /** @type {?} */
    NzStatisticNumberComponent.prototype.displayDecimal;
    /**
     * @type {?}
     * @private
     */
    NzStatisticNumberComponent.prototype.locale_id;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotc3RhdGlzdGljLW51bWJlci5jb21wb25lbnQuanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsic3RhdGlzdGljL256LXN0YXRpc3RpYy1udW1iZXIuY29tcG9uZW50LnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7QUFBQSxPQUFPLEVBQUUscUJBQXFCLEVBQUUsWUFBWSxFQUFFLE1BQU0saUJBQWlCLENBQUM7QUFDdEUsT0FBTyxFQUNMLHVCQUF1QixFQUN2QixTQUFTLEVBQ1QsTUFBTSxFQUNOLEtBQUssRUFDTCxTQUFTLEVBRVQsV0FBVyxFQUNYLGlCQUFpQixFQUNsQixNQUFNLGVBQWUsQ0FBQztBQWN2QixNQUFNLE9BQU8sMEJBQTBCOzs7O0lBT3JDLFlBQXVDLFNBQWlCO1FBQWpCLGNBQVMsR0FBVCxTQUFTLENBQVE7UUFIeEQsZUFBVSxHQUFHLEVBQUUsQ0FBQztRQUNoQixtQkFBYyxHQUFHLEVBQUUsQ0FBQztJQUV1QyxDQUFDOzs7O0lBRTVELFdBQVc7UUFDVCxJQUFJLENBQUMsWUFBWSxFQUFFLENBQUM7SUFDdEIsQ0FBQzs7Ozs7SUFFTyxZQUFZOztjQUNaLGdCQUFnQixHQUNwQixPQUFPLElBQUksQ0FBQyxPQUFPLEtBQUssUUFBUSxDQUFDLENBQUMsQ0FBQyxHQUFHLENBQUMsQ0FBQyxDQUFDLHFCQUFxQixDQUFDLElBQUksQ0FBQyxTQUFTLEVBQUUsWUFBWSxDQUFDLE9BQU8sQ0FBQzs7Y0FDaEcsS0FBSyxHQUFHLE1BQU0sQ0FBQyxJQUFJLENBQUMsT0FBTyxDQUFDO2NBQzVCLENBQUMsR0FBRyxFQUFFLE9BQU8sQ0FBQyxHQUFHLEtBQUssQ0FBQyxLQUFLLENBQUMsZ0JBQWdCLENBQUM7UUFFcEQsSUFBSSxDQUFDLFVBQVUsR0FBRyxHQUFHLENBQUM7UUFDdEIsSUFBSSxDQUFDLGNBQWMsR0FBRyxPQUFPLENBQUMsQ0FBQyxDQUFDLEdBQUcsZ0JBQWdCLEdBQUcsT0FBTyxFQUFFLENBQUMsQ0FBQyxDQUFDLEVBQUUsQ0FBQztJQUN2RSxDQUFDOzs7WUFoQ0YsU0FBUyxTQUFDO2dCQUNULGVBQWUsRUFBRSx1QkFBdUIsQ0FBQyxNQUFNO2dCQUMvQyxhQUFhLEVBQUUsaUJBQWlCLENBQUMsSUFBSTtnQkFDckMsbUJBQW1CLEVBQUUsS0FBSztnQkFDMUIsUUFBUSxFQUFFLHFCQUFxQjtnQkFDL0Isb2JBQW1EO2dCQUNuRCxJQUFJLEVBQUU7b0JBQ0osS0FBSyxFQUFFLDZCQUE2QjtpQkFDckM7eUJBQ1EsK0JBQStCO2FBQ3pDOzs7O3lDQVFjLE1BQU0sU0FBQyxTQUFTOzs7c0JBTjVCLEtBQUs7OEJBQ0wsS0FBSzs7OztJQUROLDZDQUF1Qzs7SUFDdkMscURBQTJFOztJQUUzRSxnREFBZ0I7O0lBQ2hCLG9EQUFvQjs7Ozs7SUFFUiwrQ0FBNEMiLCJzb3VyY2VzQ29udGVudCI6WyJpbXBvcnQgeyBnZXRMb2NhbGVOdW1iZXJTeW1ib2wsIE51bWJlclN5bWJvbCB9IGZyb20gJ0Bhbmd1bGFyL2NvbW1vbic7XG5pbXBvcnQge1xuICBDaGFuZ2VEZXRlY3Rpb25TdHJhdGVneSxcbiAgQ29tcG9uZW50LFxuICBJbmplY3QsXG4gIElucHV0LFxuICBMT0NBTEVfSUQsXG4gIE9uQ2hhbmdlcyxcbiAgVGVtcGxhdGVSZWYsXG4gIFZpZXdFbmNhcHN1bGF0aW9uXG59IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuaW1wb3J0IHsgTnpTdGF0aXN0aWNWYWx1ZVR5cGUgfSBmcm9tICcuL256LXN0YXRpc3RpYy1kZWZpbml0aW9ucyc7XG5cbkBDb21wb25lbnQoe1xuICBjaGFuZ2VEZXRlY3Rpb246IENoYW5nZURldGVjdGlvblN0cmF0ZWd5Lk9uUHVzaCxcbiAgZW5jYXBzdWxhdGlvbjogVmlld0VuY2Fwc3VsYXRpb24uTm9uZSxcbiAgcHJlc2VydmVXaGl0ZXNwYWNlczogZmFsc2UsXG4gIHNlbGVjdG9yOiAnbnotc3RhdGlzdGljLW51bWJlcicsXG4gIHRlbXBsYXRlVXJsOiAnLi9uei1zdGF0aXN0aWMtbnVtYmVyLmNvbXBvbmVudC5odG1sJyxcbiAgaG9zdDoge1xuICAgIGNsYXNzOiAnYW50LXN0YXRpc3RpYy1jb250ZW50LXZhbHVlJ1xuICB9LFxuICBzdHlsZXM6IFsnbnotbnVtYmVyIHsgZGlzcGxheTogaW5saW5lIH0nXVxufSlcbmV4cG9ydCBjbGFzcyBOelN0YXRpc3RpY051bWJlckNvbXBvbmVudCBpbXBsZW1lbnRzIE9uQ2hhbmdlcyB7XG4gIEBJbnB1dCgpIG56VmFsdWU6IE56U3RhdGlzdGljVmFsdWVUeXBlO1xuICBASW5wdXQoKSBuelZhbHVlVGVtcGxhdGU6IFRlbXBsYXRlUmVmPHsgJGltcGxpY2l0OiBOelN0YXRpc3RpY1ZhbHVlVHlwZSB9PjtcblxuICBkaXNwbGF5SW50ID0gJyc7XG4gIGRpc3BsYXlEZWNpbWFsID0gJyc7XG5cbiAgY29uc3RydWN0b3IoQEluamVjdChMT0NBTEVfSUQpIHByaXZhdGUgbG9jYWxlX2lkOiBzdHJpbmcpIHt9XG5cbiAgbmdPbkNoYW5nZXMoKTogdm9pZCB7XG4gICAgdGhpcy5mb3JtYXROdW1iZXIoKTtcbiAgfVxuXG4gIHByaXZhdGUgZm9ybWF0TnVtYmVyKCk6IHZvaWQge1xuICAgIGNvbnN0IGRlY2ltYWxTZXBhcmF0b3I6IHN0cmluZyA9XG4gICAgICB0eXBlb2YgdGhpcy5uelZhbHVlID09PSAnbnVtYmVyJyA/ICcuJyA6IGdldExvY2FsZU51bWJlclN5bWJvbCh0aGlzLmxvY2FsZV9pZCwgTnVtYmVyU3ltYm9sLkRlY2ltYWwpO1xuICAgIGNvbnN0IHZhbHVlID0gU3RyaW5nKHRoaXMubnpWYWx1ZSk7XG4gICAgY29uc3QgW2ludCwgZGVjaW1hbF0gPSB2YWx1ZS5zcGxpdChkZWNpbWFsU2VwYXJhdG9yKTtcblxuICAgIHRoaXMuZGlzcGxheUludCA9IGludDtcbiAgICB0aGlzLmRpc3BsYXlEZWNpbWFsID0gZGVjaW1hbCA/IGAke2RlY2ltYWxTZXBhcmF0b3J9JHtkZWNpbWFsfWAgOiAnJztcbiAgfVxufVxuIl19