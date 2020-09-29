/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { ChangeDetectionStrategy, ChangeDetectorRef, Component, EventEmitter, Input, Output, ViewEncapsulation } from '@angular/core';
var NzTransferSearchComponent = /** @class */ (function () {
    // endregion
    function NzTransferSearchComponent(cdr) {
        this.cdr = cdr;
        this.valueChanged = new EventEmitter();
        this.valueClear = new EventEmitter();
    }
    /**
     * @return {?}
     */
    NzTransferSearchComponent.prototype._handle = /**
     * @return {?}
     */
    function () {
        this.valueChanged.emit(this.value);
    };
    /**
     * @return {?}
     */
    NzTransferSearchComponent.prototype._clear = /**
     * @return {?}
     */
    function () {
        if (this.disabled) {
            return;
        }
        this.value = '';
        this.valueClear.emit();
    };
    /**
     * @return {?}
     */
    NzTransferSearchComponent.prototype.ngOnChanges = /**
     * @return {?}
     */
    function () {
        this.cdr.detectChanges();
    };
    NzTransferSearchComponent.decorators = [
        { type: Component, args: [{
                    selector: '[nz-transfer-search]',
                    preserveWhitespaces: false,
                    template: "<input [(ngModel)]=\"value\" (ngModelChange)=\"_handle()\" [disabled]=\"disabled\" [placeholder]=\"placeholder\"\n  class=\"ant-input ant-transfer-list-search\" [ngClass]=\"{'ant-input-disabled': disabled}\">\n<a *ngIf=\"value && value.length > 0; else def\" class=\"ant-transfer-list-search-action\" (click)=\"_clear()\">\n  <i nz-icon type=\"close-circle\"></i>\n</a>\n<ng-template #def>\n  <span class=\"ant-transfer-list-search-action\"><i nz-icon type=\"search\"></i></span>\n</ng-template>",
                    encapsulation: ViewEncapsulation.None,
                    changeDetection: ChangeDetectionStrategy.OnPush
                }] }
    ];
    /** @nocollapse */
    NzTransferSearchComponent.ctorParameters = function () { return [
        { type: ChangeDetectorRef }
    ]; };
    NzTransferSearchComponent.propDecorators = {
        placeholder: [{ type: Input }],
        value: [{ type: Input }],
        disabled: [{ type: Input }],
        valueChanged: [{ type: Output }],
        valueClear: [{ type: Output }]
    };
    return NzTransferSearchComponent;
}());
export { NzTransferSearchComponent };
if (false) {
    /** @type {?} */
    NzTransferSearchComponent.prototype.placeholder;
    /** @type {?} */
    NzTransferSearchComponent.prototype.value;
    /** @type {?} */
    NzTransferSearchComponent.prototype.disabled;
    /** @type {?} */
    NzTransferSearchComponent.prototype.valueChanged;
    /** @type {?} */
    NzTransferSearchComponent.prototype.valueClear;
    /**
     * @type {?}
     * @private
     */
    NzTransferSearchComponent.prototype.cdr;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotdHJhbnNmZXItc2VhcmNoLmNvbXBvbmVudC5qcyIsInNvdXJjZVJvb3QiOiJuZzovL25nLXpvcnJvLWFudGQvIiwic291cmNlcyI6WyJ0cmFuc2Zlci9uei10cmFuc2Zlci1zZWFyY2guY29tcG9uZW50LnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7QUFBQSxPQUFPLEVBQ0wsdUJBQXVCLEVBQ3ZCLGlCQUFpQixFQUNqQixTQUFTLEVBQ1QsWUFBWSxFQUNaLEtBQUssRUFFTCxNQUFNLEVBQ04saUJBQWlCLEVBQ2xCLE1BQU0sZUFBZSxDQUFDO0FBRXZCO0lBaUJFLFlBQVk7SUFFWixtQ0FBb0IsR0FBc0I7UUFBdEIsUUFBRyxHQUFILEdBQUcsQ0FBbUI7UUFMdkIsaUJBQVksR0FBRyxJQUFJLFlBQVksRUFBVSxDQUFDO1FBQzFDLGVBQVUsR0FBRyxJQUFJLFlBQVksRUFBUSxDQUFDO0lBSVosQ0FBQzs7OztJQUU5QywyQ0FBTzs7O0lBQVA7UUFDRSxJQUFJLENBQUMsWUFBWSxDQUFDLElBQUksQ0FBQyxJQUFJLENBQUMsS0FBSyxDQUFDLENBQUM7SUFDckMsQ0FBQzs7OztJQUVELDBDQUFNOzs7SUFBTjtRQUNFLElBQUksSUFBSSxDQUFDLFFBQVEsRUFBRTtZQUNqQixPQUFPO1NBQ1I7UUFDRCxJQUFJLENBQUMsS0FBSyxHQUFHLEVBQUUsQ0FBQztRQUNoQixJQUFJLENBQUMsVUFBVSxDQUFDLElBQUksRUFBRSxDQUFDO0lBQ3pCLENBQUM7Ozs7SUFFRCwrQ0FBVzs7O0lBQVg7UUFDRSxJQUFJLENBQUMsR0FBRyxDQUFDLGFBQWEsRUFBRSxDQUFDO0lBQzNCLENBQUM7O2dCQW5DRixTQUFTLFNBQUM7b0JBQ1QsUUFBUSxFQUFFLHNCQUFzQjtvQkFDaEMsbUJBQW1CLEVBQUUsS0FBSztvQkFDMUIsMmZBQWtEO29CQUNsRCxhQUFhLEVBQUUsaUJBQWlCLENBQUMsSUFBSTtvQkFDckMsZUFBZSxFQUFFLHVCQUF1QixDQUFDLE1BQU07aUJBQ2hEOzs7O2dCQWZDLGlCQUFpQjs7OzhCQW1CaEIsS0FBSzt3QkFDTCxLQUFLOzJCQUNMLEtBQUs7K0JBRUwsTUFBTTs2QkFDTixNQUFNOztJQXFCVCxnQ0FBQztDQUFBLEFBcENELElBb0NDO1NBN0JZLHlCQUF5Qjs7O0lBR3BDLGdEQUE2Qjs7SUFDN0IsMENBQXVCOztJQUN2Qiw2Q0FBMkI7O0lBRTNCLGlEQUE2RDs7SUFDN0QsK0NBQXlEOzs7OztJQUk3Qyx3Q0FBOEIiLCJzb3VyY2VzQ29udGVudCI6WyJpbXBvcnQge1xuICBDaGFuZ2VEZXRlY3Rpb25TdHJhdGVneSxcbiAgQ2hhbmdlRGV0ZWN0b3JSZWYsXG4gIENvbXBvbmVudCxcbiAgRXZlbnRFbWl0dGVyLFxuICBJbnB1dCxcbiAgT25DaGFuZ2VzLFxuICBPdXRwdXQsXG4gIFZpZXdFbmNhcHN1bGF0aW9uXG59IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuXG5AQ29tcG9uZW50KHtcbiAgc2VsZWN0b3I6ICdbbnotdHJhbnNmZXItc2VhcmNoXScsXG4gIHByZXNlcnZlV2hpdGVzcGFjZXM6IGZhbHNlLFxuICB0ZW1wbGF0ZVVybDogJy4vbnotdHJhbnNmZXItc2VhcmNoLmNvbXBvbmVudC5odG1sJyxcbiAgZW5jYXBzdWxhdGlvbjogVmlld0VuY2Fwc3VsYXRpb24uTm9uZSxcbiAgY2hhbmdlRGV0ZWN0aW9uOiBDaGFuZ2VEZXRlY3Rpb25TdHJhdGVneS5PblB1c2hcbn0pXG5leHBvcnQgY2xhc3MgTnpUcmFuc2ZlclNlYXJjaENvbXBvbmVudCBpbXBsZW1lbnRzIE9uQ2hhbmdlcyB7XG4gIC8vIHJlZ2lvbjogZmllbGRzXG5cbiAgQElucHV0KCkgcGxhY2Vob2xkZXI6IHN0cmluZztcbiAgQElucHV0KCkgdmFsdWU6IHN0cmluZztcbiAgQElucHV0KCkgZGlzYWJsZWQ6IGJvb2xlYW47XG5cbiAgQE91dHB1dCgpIHJlYWRvbmx5IHZhbHVlQ2hhbmdlZCA9IG5ldyBFdmVudEVtaXR0ZXI8c3RyaW5nPigpO1xuICBAT3V0cHV0KCkgcmVhZG9ubHkgdmFsdWVDbGVhciA9IG5ldyBFdmVudEVtaXR0ZXI8dm9pZD4oKTtcblxuICAvLyBlbmRyZWdpb25cblxuICBjb25zdHJ1Y3Rvcihwcml2YXRlIGNkcjogQ2hhbmdlRGV0ZWN0b3JSZWYpIHt9XG5cbiAgX2hhbmRsZSgpOiB2b2lkIHtcbiAgICB0aGlzLnZhbHVlQ2hhbmdlZC5lbWl0KHRoaXMudmFsdWUpO1xuICB9XG5cbiAgX2NsZWFyKCk6IHZvaWQge1xuICAgIGlmICh0aGlzLmRpc2FibGVkKSB7XG4gICAgICByZXR1cm47XG4gICAgfVxuICAgIHRoaXMudmFsdWUgPSAnJztcbiAgICB0aGlzLnZhbHVlQ2xlYXIuZW1pdCgpO1xuICB9XG5cbiAgbmdPbkNoYW5nZXMoKTogdm9pZCB7XG4gICAgdGhpcy5jZHIuZGV0ZWN0Q2hhbmdlcygpO1xuICB9XG59XG4iXX0=