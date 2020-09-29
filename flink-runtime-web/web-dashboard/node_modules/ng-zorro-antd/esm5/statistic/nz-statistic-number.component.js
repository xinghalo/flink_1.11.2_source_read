/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import * as tslib_1 from "tslib";
import { getLocaleNumberSymbol, NumberSymbol } from '@angular/common';
import { ChangeDetectionStrategy, Component, Inject, Input, LOCALE_ID, TemplateRef, ViewEncapsulation } from '@angular/core';
var NzStatisticNumberComponent = /** @class */ (function () {
    function NzStatisticNumberComponent(locale_id) {
        this.locale_id = locale_id;
        this.displayInt = '';
        this.displayDecimal = '';
    }
    /**
     * @return {?}
     */
    NzStatisticNumberComponent.prototype.ngOnChanges = /**
     * @return {?}
     */
    function () {
        this.formatNumber();
    };
    /**
     * @private
     * @return {?}
     */
    NzStatisticNumberComponent.prototype.formatNumber = /**
     * @private
     * @return {?}
     */
    function () {
        /** @type {?} */
        var decimalSeparator = typeof this.nzValue === 'number' ? '.' : getLocaleNumberSymbol(this.locale_id, NumberSymbol.Decimal);
        /** @type {?} */
        var value = String(this.nzValue);
        var _a = tslib_1.__read(value.split(decimalSeparator), 2), int = _a[0], decimal = _a[1];
        this.displayInt = int;
        this.displayDecimal = decimal ? "" + decimalSeparator + decimal : '';
    };
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
    NzStatisticNumberComponent.ctorParameters = function () { return [
        { type: String, decorators: [{ type: Inject, args: [LOCALE_ID,] }] }
    ]; };
    NzStatisticNumberComponent.propDecorators = {
        nzValue: [{ type: Input }],
        nzValueTemplate: [{ type: Input }]
    };
    return NzStatisticNumberComponent;
}());
export { NzStatisticNumberComponent };
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
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotc3RhdGlzdGljLW51bWJlci5jb21wb25lbnQuanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsic3RhdGlzdGljL256LXN0YXRpc3RpYy1udW1iZXIuY29tcG9uZW50LnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7O0FBQUEsT0FBTyxFQUFFLHFCQUFxQixFQUFFLFlBQVksRUFBRSxNQUFNLGlCQUFpQixDQUFDO0FBQ3RFLE9BQU8sRUFDTCx1QkFBdUIsRUFDdkIsU0FBUyxFQUNULE1BQU0sRUFDTixLQUFLLEVBQ0wsU0FBUyxFQUVULFdBQVcsRUFDWCxpQkFBaUIsRUFDbEIsTUFBTSxlQUFlLENBQUM7QUFHdkI7SUFrQkUsb0NBQXVDLFNBQWlCO1FBQWpCLGNBQVMsR0FBVCxTQUFTLENBQVE7UUFIeEQsZUFBVSxHQUFHLEVBQUUsQ0FBQztRQUNoQixtQkFBYyxHQUFHLEVBQUUsQ0FBQztJQUV1QyxDQUFDOzs7O0lBRTVELGdEQUFXOzs7SUFBWDtRQUNFLElBQUksQ0FBQyxZQUFZLEVBQUUsQ0FBQztJQUN0QixDQUFDOzs7OztJQUVPLGlEQUFZOzs7O0lBQXBCOztZQUNRLGdCQUFnQixHQUNwQixPQUFPLElBQUksQ0FBQyxPQUFPLEtBQUssUUFBUSxDQUFDLENBQUMsQ0FBQyxHQUFHLENBQUMsQ0FBQyxDQUFDLHFCQUFxQixDQUFDLElBQUksQ0FBQyxTQUFTLEVBQUUsWUFBWSxDQUFDLE9BQU8sQ0FBQzs7WUFDaEcsS0FBSyxHQUFHLE1BQU0sQ0FBQyxJQUFJLENBQUMsT0FBTyxDQUFDO1FBQzVCLElBQUEscURBQThDLEVBQTdDLFdBQUcsRUFBRSxlQUF3QztRQUVwRCxJQUFJLENBQUMsVUFBVSxHQUFHLEdBQUcsQ0FBQztRQUN0QixJQUFJLENBQUMsY0FBYyxHQUFHLE9BQU8sQ0FBQyxDQUFDLENBQUMsS0FBRyxnQkFBZ0IsR0FBRyxPQUFTLENBQUMsQ0FBQyxDQUFDLEVBQUUsQ0FBQztJQUN2RSxDQUFDOztnQkFoQ0YsU0FBUyxTQUFDO29CQUNULGVBQWUsRUFBRSx1QkFBdUIsQ0FBQyxNQUFNO29CQUMvQyxhQUFhLEVBQUUsaUJBQWlCLENBQUMsSUFBSTtvQkFDckMsbUJBQW1CLEVBQUUsS0FBSztvQkFDMUIsUUFBUSxFQUFFLHFCQUFxQjtvQkFDL0Isb2JBQW1EO29CQUNuRCxJQUFJLEVBQUU7d0JBQ0osS0FBSyxFQUFFLDZCQUE2QjtxQkFDckM7NkJBQ1EsK0JBQStCO2lCQUN6Qzs7Ozs2Q0FRYyxNQUFNLFNBQUMsU0FBUzs7OzBCQU41QixLQUFLO2tDQUNMLEtBQUs7O0lBb0JSLGlDQUFDO0NBQUEsQUFqQ0QsSUFpQ0M7U0F0QlksMEJBQTBCOzs7SUFDckMsNkNBQXVDOztJQUN2QyxxREFBMkU7O0lBRTNFLGdEQUFnQjs7SUFDaEIsb0RBQW9COzs7OztJQUVSLCtDQUE0QyIsInNvdXJjZXNDb250ZW50IjpbImltcG9ydCB7IGdldExvY2FsZU51bWJlclN5bWJvbCwgTnVtYmVyU3ltYm9sIH0gZnJvbSAnQGFuZ3VsYXIvY29tbW9uJztcbmltcG9ydCB7XG4gIENoYW5nZURldGVjdGlvblN0cmF0ZWd5LFxuICBDb21wb25lbnQsXG4gIEluamVjdCxcbiAgSW5wdXQsXG4gIExPQ0FMRV9JRCxcbiAgT25DaGFuZ2VzLFxuICBUZW1wbGF0ZVJlZixcbiAgVmlld0VuY2Fwc3VsYXRpb25cbn0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5pbXBvcnQgeyBOelN0YXRpc3RpY1ZhbHVlVHlwZSB9IGZyb20gJy4vbnotc3RhdGlzdGljLWRlZmluaXRpb25zJztcblxuQENvbXBvbmVudCh7XG4gIGNoYW5nZURldGVjdGlvbjogQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3kuT25QdXNoLFxuICBlbmNhcHN1bGF0aW9uOiBWaWV3RW5jYXBzdWxhdGlvbi5Ob25lLFxuICBwcmVzZXJ2ZVdoaXRlc3BhY2VzOiBmYWxzZSxcbiAgc2VsZWN0b3I6ICduei1zdGF0aXN0aWMtbnVtYmVyJyxcbiAgdGVtcGxhdGVVcmw6ICcuL256LXN0YXRpc3RpYy1udW1iZXIuY29tcG9uZW50Lmh0bWwnLFxuICBob3N0OiB7XG4gICAgY2xhc3M6ICdhbnQtc3RhdGlzdGljLWNvbnRlbnQtdmFsdWUnXG4gIH0sXG4gIHN0eWxlczogWyduei1udW1iZXIgeyBkaXNwbGF5OiBpbmxpbmUgfSddXG59KVxuZXhwb3J0IGNsYXNzIE56U3RhdGlzdGljTnVtYmVyQ29tcG9uZW50IGltcGxlbWVudHMgT25DaGFuZ2VzIHtcbiAgQElucHV0KCkgbnpWYWx1ZTogTnpTdGF0aXN0aWNWYWx1ZVR5cGU7XG4gIEBJbnB1dCgpIG56VmFsdWVUZW1wbGF0ZTogVGVtcGxhdGVSZWY8eyAkaW1wbGljaXQ6IE56U3RhdGlzdGljVmFsdWVUeXBlIH0+O1xuXG4gIGRpc3BsYXlJbnQgPSAnJztcbiAgZGlzcGxheURlY2ltYWwgPSAnJztcblxuICBjb25zdHJ1Y3RvcihASW5qZWN0KExPQ0FMRV9JRCkgcHJpdmF0ZSBsb2NhbGVfaWQ6IHN0cmluZykge31cblxuICBuZ09uQ2hhbmdlcygpOiB2b2lkIHtcbiAgICB0aGlzLmZvcm1hdE51bWJlcigpO1xuICB9XG5cbiAgcHJpdmF0ZSBmb3JtYXROdW1iZXIoKTogdm9pZCB7XG4gICAgY29uc3QgZGVjaW1hbFNlcGFyYXRvcjogc3RyaW5nID1cbiAgICAgIHR5cGVvZiB0aGlzLm56VmFsdWUgPT09ICdudW1iZXInID8gJy4nIDogZ2V0TG9jYWxlTnVtYmVyU3ltYm9sKHRoaXMubG9jYWxlX2lkLCBOdW1iZXJTeW1ib2wuRGVjaW1hbCk7XG4gICAgY29uc3QgdmFsdWUgPSBTdHJpbmcodGhpcy5uelZhbHVlKTtcbiAgICBjb25zdCBbaW50LCBkZWNpbWFsXSA9IHZhbHVlLnNwbGl0KGRlY2ltYWxTZXBhcmF0b3IpO1xuXG4gICAgdGhpcy5kaXNwbGF5SW50ID0gaW50O1xuICAgIHRoaXMuZGlzcGxheURlY2ltYWwgPSBkZWNpbWFsID8gYCR7ZGVjaW1hbFNlcGFyYXRvcn0ke2RlY2ltYWx9YCA6ICcnO1xuICB9XG59XG4iXX0=