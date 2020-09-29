/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import * as tslib_1 from "tslib";
import { EventEmitter, Input, Output, ViewChild } from '@angular/core';
import { Subject } from 'rxjs';
import { takeUntil } from 'rxjs/operators';
import { InputBoolean } from '../core/util/convert';
import { CandyDate } from './lib/candy-date';
import { NzPickerComponent } from './picker.component';
/** @type {?} */
var POPUP_STYLE_PATCH = { position: 'relative' };
// Aim to override antd's style to support overlay's position strategy (position:absolute will cause it not working beacuse the overlay can't get the height/width of it's content)
/**
 * The base picker for all common APIs
 * @abstract
 */
var AbstractPickerComponent = /** @class */ (function () {
    function AbstractPickerComponent(i18n, cdr, dateHelper, noAnimation) {
        this.i18n = i18n;
        this.cdr = cdr;
        this.dateHelper = dateHelper;
        this.noAnimation = noAnimation;
        // --- Common API
        this.nzAllowClear = true;
        this.nzAutoFocus = false;
        this.nzDisabled = false;
        this.nzPopupStyle = POPUP_STYLE_PATCH;
        this.nzOnOpenChange = new EventEmitter();
        this.isRange = false; // Indicate whether the value is a range value
        this.destroyed$ = new Subject();
        this.isCustomPlaceHolder = false;
        // ------------------------------------------------------------------------
        // | Control value accessor implements
        // ------------------------------------------------------------------------
        // NOTE: onChangeFn/onTouchedFn will not be assigned if user not use as ngModel
        this.onChangeFn = (/**
         * @return {?}
         */
        function () { return void 0; });
        this.onTouchedFn = (/**
         * @return {?}
         */
        function () { return void 0; });
    }
    Object.defineProperty(AbstractPickerComponent.prototype, "realOpenState", {
        get: 
        // Indicate whether the value is a range value
        /**
         * @return {?}
         */
        function () {
            return this.picker.animationOpenState;
        } // Use picker's real open state to let re-render the picker's content when shown up
        ,
        enumerable: true,
        configurable: true
    });
    // Use picker's real open state to let re-render the picker's content when shown up
    /**
     * @return {?}
     */
    AbstractPickerComponent.prototype.initValue = 
    // Use picker's real open state to let re-render the picker's content when shown up
    /**
     * @return {?}
     */
    function () {
        this.nzValue = this.isRange ? [] : null;
    };
    /**
     * @return {?}
     */
    AbstractPickerComponent.prototype.ngOnInit = /**
     * @return {?}
     */
    function () {
        var _this = this;
        // Subscribe the every locale change if the nzLocale is not handled by user
        if (!this.nzLocale) {
            this.i18n.localeChange.pipe(takeUntil(this.destroyed$)).subscribe((/**
             * @return {?}
             */
            function () { return _this.setLocale(); }));
        }
        // Default value
        this.initValue();
    };
    /**
     * @param {?} changes
     * @return {?}
     */
    AbstractPickerComponent.prototype.ngOnChanges = /**
     * @param {?} changes
     * @return {?}
     */
    function (changes) {
        if (changes.nzPopupStyle) {
            // Always assign the popup style patch
            this.nzPopupStyle = this.nzPopupStyle ? tslib_1.__assign({}, this.nzPopupStyle, POPUP_STYLE_PATCH) : POPUP_STYLE_PATCH;
        }
        // Mark as customized placeholder by user once nzPlaceHolder assigned at the first time
        if (changes.nzPlaceHolder && changes.nzPlaceHolder.firstChange && typeof this.nzPlaceHolder !== 'undefined') {
            this.isCustomPlaceHolder = true;
        }
        if (changes.nzLocale) {
            // The nzLocale is currently handled by user
            this.setDefaultPlaceHolder();
        }
    };
    /**
     * @return {?}
     */
    AbstractPickerComponent.prototype.ngOnDestroy = /**
     * @return {?}
     */
    function () {
        this.destroyed$.next();
        this.destroyed$.complete();
    };
    /**
     * @return {?}
     */
    AbstractPickerComponent.prototype.closeOverlay = /**
     * @return {?}
     */
    function () {
        this.picker.hideOverlay();
    };
    /**
     * Common handle for value changes
     * @param value changed value
     */
    /**
     * Common handle for value changes
     * @param {?} value changed value
     * @return {?}
     */
    AbstractPickerComponent.prototype.onValueChange = /**
     * Common handle for value changes
     * @param {?} value changed value
     * @return {?}
     */
    function (value) {
        this.nzValue = value;
        if (this.isRange) {
            /** @type {?} */
            var vAsRange = (/** @type {?} */ (this.nzValue));
            if (vAsRange.length) {
                this.onChangeFn([vAsRange[0].nativeDate, vAsRange[1].nativeDate]);
            }
            else {
                this.onChangeFn([]);
            }
        }
        else {
            if (this.nzValue) {
                this.onChangeFn(((/** @type {?} */ (this.nzValue))).nativeDate);
            }
            else {
                this.onChangeFn(null);
            }
        }
        this.onTouchedFn();
    };
    /**
     * Triggered when overlayOpen changes (different with realOpenState)
     * @param open The overlayOpen in picker component
     */
    /**
     * Triggered when overlayOpen changes (different with realOpenState)
     * @param {?} open The overlayOpen in picker component
     * @return {?}
     */
    AbstractPickerComponent.prototype.onOpenChange = /**
     * Triggered when overlayOpen changes (different with realOpenState)
     * @param {?} open The overlayOpen in picker component
     * @return {?}
     */
    function (open) {
        this.nzOnOpenChange.emit(open);
    };
    /**
     * @param {?} value
     * @return {?}
     */
    AbstractPickerComponent.prototype.writeValue = /**
     * @param {?} value
     * @return {?}
     */
    function (value) {
        this.setValue(value);
        this.cdr.markForCheck();
    };
    // tslint:disable-next-line:no-any
    // tslint:disable-next-line:no-any
    /**
     * @param {?} fn
     * @return {?}
     */
    AbstractPickerComponent.prototype.registerOnChange = 
    // tslint:disable-next-line:no-any
    /**
     * @param {?} fn
     * @return {?}
     */
    function (fn) {
        this.onChangeFn = fn;
    };
    // tslint:disable-next-line:no-any
    // tslint:disable-next-line:no-any
    /**
     * @param {?} fn
     * @return {?}
     */
    AbstractPickerComponent.prototype.registerOnTouched = 
    // tslint:disable-next-line:no-any
    /**
     * @param {?} fn
     * @return {?}
     */
    function (fn) {
        this.onTouchedFn = fn;
    };
    /**
     * @param {?} disabled
     * @return {?}
     */
    AbstractPickerComponent.prototype.setDisabledState = /**
     * @param {?} disabled
     * @return {?}
     */
    function (disabled) {
        this.nzDisabled = disabled;
        this.cdr.markForCheck();
    };
    // ------------------------------------------------------------------------
    // | Internal methods
    // ------------------------------------------------------------------------
    // Reload locale from i18n with side effects
    // ------------------------------------------------------------------------
    // | Internal methods
    // ------------------------------------------------------------------------
    // Reload locale from i18n with side effects
    /**
     * @private
     * @return {?}
     */
    AbstractPickerComponent.prototype.setLocale = 
    // ------------------------------------------------------------------------
    // | Internal methods
    // ------------------------------------------------------------------------
    // Reload locale from i18n with side effects
    /**
     * @private
     * @return {?}
     */
    function () {
        this.nzLocale = this.i18n.getLocaleData('DatePicker', {});
        this.setDefaultPlaceHolder();
        this.cdr.markForCheck();
    };
    /**
     * @private
     * @return {?}
     */
    AbstractPickerComponent.prototype.setDefaultPlaceHolder = /**
     * @private
     * @return {?}
     */
    function () {
        if (!this.isCustomPlaceHolder && this.nzLocale) {
            this.nzPlaceHolder = this.isRange ? this.nzLocale.lang.rangePlaceholder : this.nzLocale.lang.placeholder;
        }
    };
    // Safe way of setting value with default
    // Safe way of setting value with default
    /**
     * @private
     * @param {?} value
     * @return {?}
     */
    AbstractPickerComponent.prototype.setValue = 
    // Safe way of setting value with default
    /**
     * @private
     * @param {?} value
     * @return {?}
     */
    function (value) {
        if (this.isRange) {
            this.nzValue = value ? ((/** @type {?} */ (value))).map((/**
             * @param {?} val
             * @return {?}
             */
            function (val) { return new CandyDate(val); })) : [];
        }
        else {
            this.nzValue = value ? new CandyDate((/** @type {?} */ (value))) : null;
        }
    };
    AbstractPickerComponent.propDecorators = {
        nzAllowClear: [{ type: Input }],
        nzAutoFocus: [{ type: Input }],
        nzDisabled: [{ type: Input }],
        nzOpen: [{ type: Input }],
        nzClassName: [{ type: Input }],
        nzDisabledDate: [{ type: Input }],
        nzLocale: [{ type: Input }],
        nzPlaceHolder: [{ type: Input }],
        nzPopupStyle: [{ type: Input }],
        nzDropdownClassName: [{ type: Input }],
        nzSize: [{ type: Input }],
        nzStyle: [{ type: Input }],
        nzFormat: [{ type: Input }],
        nzValue: [{ type: Input }],
        nzOnOpenChange: [{ type: Output }],
        picker: [{ type: ViewChild, args: [NzPickerComponent,] }]
    };
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Boolean)
    ], AbstractPickerComponent.prototype, "nzAllowClear", void 0);
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Boolean)
    ], AbstractPickerComponent.prototype, "nzAutoFocus", void 0);
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Boolean)
    ], AbstractPickerComponent.prototype, "nzDisabled", void 0);
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Boolean)
    ], AbstractPickerComponent.prototype, "nzOpen", void 0);
    return AbstractPickerComponent;
}());
export { AbstractPickerComponent };
if (false) {
    /** @type {?} */
    AbstractPickerComponent.prototype.nzAllowClear;
    /** @type {?} */
    AbstractPickerComponent.prototype.nzAutoFocus;
    /** @type {?} */
    AbstractPickerComponent.prototype.nzDisabled;
    /** @type {?} */
    AbstractPickerComponent.prototype.nzOpen;
    /** @type {?} */
    AbstractPickerComponent.prototype.nzClassName;
    /** @type {?} */
    AbstractPickerComponent.prototype.nzDisabledDate;
    /** @type {?} */
    AbstractPickerComponent.prototype.nzLocale;
    /** @type {?} */
    AbstractPickerComponent.prototype.nzPlaceHolder;
    /** @type {?} */
    AbstractPickerComponent.prototype.nzPopupStyle;
    /** @type {?} */
    AbstractPickerComponent.prototype.nzDropdownClassName;
    /** @type {?} */
    AbstractPickerComponent.prototype.nzSize;
    /** @type {?} */
    AbstractPickerComponent.prototype.nzStyle;
    /** @type {?} */
    AbstractPickerComponent.prototype.nzFormat;
    /** @type {?} */
    AbstractPickerComponent.prototype.nzValue;
    /** @type {?} */
    AbstractPickerComponent.prototype.nzOnOpenChange;
    /**
     * @type {?}
     * @protected
     */
    AbstractPickerComponent.prototype.picker;
    /** @type {?} */
    AbstractPickerComponent.prototype.isRange;
    /**
     * @type {?}
     * @protected
     */
    AbstractPickerComponent.prototype.destroyed$;
    /**
     * @type {?}
     * @protected
     */
    AbstractPickerComponent.prototype.isCustomPlaceHolder;
    /** @type {?} */
    AbstractPickerComponent.prototype.onChangeFn;
    /** @type {?} */
    AbstractPickerComponent.prototype.onTouchedFn;
    /**
     * @type {?}
     * @protected
     */
    AbstractPickerComponent.prototype.i18n;
    /**
     * @type {?}
     * @protected
     */
    AbstractPickerComponent.prototype.cdr;
    /**
     * @type {?}
     * @protected
     */
    AbstractPickerComponent.prototype.dateHelper;
    /** @type {?} */
    AbstractPickerComponent.prototype.noAnimation;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiYWJzdHJhY3QtcGlja2VyLmNvbXBvbmVudC5qcyIsInNvdXJjZVJvb3QiOiJuZzovL25nLXpvcnJvLWFudGQvIiwic291cmNlcyI6WyJkYXRlLXBpY2tlci9hYnN0cmFjdC1waWNrZXIuY29tcG9uZW50LnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7O0FBQUEsT0FBTyxFQUVMLFlBQVksRUFDWixLQUFLLEVBSUwsTUFBTSxFQUVOLFNBQVMsRUFDVixNQUFNLGVBQWUsQ0FBQztBQUd2QixPQUFPLEVBQUUsT0FBTyxFQUFFLE1BQU0sTUFBTSxDQUFDO0FBQy9CLE9BQU8sRUFBRSxTQUFTLEVBQUUsTUFBTSxnQkFBZ0IsQ0FBQztBQUUzQyxPQUFPLEVBQUUsWUFBWSxFQUFFLE1BQU0sc0JBQXNCLENBQUM7QUFJcEQsT0FBTyxFQUFFLFNBQVMsRUFBRSxNQUFNLGtCQUFrQixDQUFDO0FBQzdDLE9BQU8sRUFBRSxpQkFBaUIsRUFBRSxNQUFNLG9CQUFvQixDQUFDOztJQUVqRCxpQkFBaUIsR0FBRyxFQUFFLFFBQVEsRUFBRSxVQUFVLEVBQUU7Ozs7OztBQUtsRDtJQWtDRSxpQ0FDWSxJQUFtQixFQUNuQixHQUFzQixFQUN0QixVQUE2QixFQUNoQyxXQUFvQztRQUhqQyxTQUFJLEdBQUosSUFBSSxDQUFlO1FBQ25CLFFBQUcsR0FBSCxHQUFHLENBQW1CO1FBQ3RCLGVBQVUsR0FBVixVQUFVLENBQW1CO1FBQ2hDLGdCQUFXLEdBQVgsV0FBVyxDQUF5Qjs7UUFwQ3BCLGlCQUFZLEdBQVksSUFBSSxDQUFDO1FBQzdCLGdCQUFXLEdBQVksS0FBSyxDQUFDO1FBQzdCLGVBQVUsR0FBWSxLQUFLLENBQUM7UUFNNUMsaUJBQVksR0FBVyxpQkFBaUIsQ0FBQztRQU8vQixtQkFBYyxHQUFHLElBQUksWUFBWSxFQUFXLENBQUM7UUFJaEUsWUFBTyxHQUFZLEtBQUssQ0FBQyxDQUFDLDhDQUE4QztRQVU5RCxlQUFVLEdBQWtCLElBQUksT0FBTyxFQUFFLENBQUM7UUFDMUMs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SUFDNUI7UUFBZixZQUFZLEVBQUU7OzJEQUFpQjtJQTRKM0MsOEJBQUM7Q0FBQSxBQWpLRCxJQWlLQztTQWpLcUIsdUJBQXVCOzs7SUFFM0MsK0NBQXNEOztJQUN0RCw4Q0FBc0Q7O0lBQ3RELDZDQUFxRDs7SUFDckQseUNBQXlDOztJQUN6Qyw4Q0FBNkI7O0lBQzdCLGlEQUE4Qzs7SUFDOUMsMkNBQTZDOztJQUM3QyxnREFBMEM7O0lBQzFDLCtDQUFrRDs7SUFDbEQsc0RBQXFDOztJQUNyQyx5Q0FBbUM7O0lBQ25DLDBDQUF5Qjs7SUFDekIsMkNBQTBCOztJQUMxQiwwQ0FBeUM7O0lBRXpDLGlEQUFnRTs7Ozs7SUFFaEUseUNBQWtFOztJQUVsRSwwQ0FBeUI7Ozs7O0lBVXpCLDZDQUFvRDs7Ozs7SUFDcEQsc0RBQStDOztJQWlGL0MsNkNBQWdFOztJQUNoRSw4Q0FBdUM7Ozs7O0lBL0VyQyx1Q0FBNkI7Ozs7O0lBQzdCLHNDQUFnQzs7Ozs7SUFDaEMsNkNBQXVDOztJQUN2Qyw4Q0FBMkMiLCJzb3VyY2VzQ29udGVudCI6WyJpbXBvcnQge1xuICBDaGFuZ2VEZXRlY3RvclJlZixcbiAgRXZlbnRFbWl0dGVyLFxuICBJbnB1dCxcbiAgT25DaGFuZ2VzLFxuICBPbkRlc3Ryb3ksXG4gIE9uSW5pdCxcbiAgT3V0cHV0LFxuICBTaW1wbGVDaGFuZ2VzLFxuICBWaWV3Q2hpbGRcbn0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5pbXBvcnQgeyBDb250cm9sVmFsdWVBY2Nlc3NvciB9IGZyb20gJ0Bhbmd1bGFyL2Zvcm1zJztcblxuaW1wb3J0IHsgU3ViamVjdCB9IGZyb20gJ3J4anMnO1xuaW1wb3J0IHsgdGFrZVVudGlsIH0gZnJvbSAncnhqcy9vcGVyYXRvcnMnO1xuaW1wb3J0IHsgTnpOb0FuaW1hdGlvbkRpcmVjdGl2ZSB9IGZyb20gJy4uL2NvcmUvbm8tYW5pbWF0aW9uL256LW5vLWFuaW1hdGlvbi5kaXJlY3RpdmUnO1xuaW1wb3J0IHsgSW5wdXRCb29sZWFuIH0gZnJvbSAnLi4vY29yZS91dGlsL2NvbnZlcnQnO1xuaW1wb3J0IHsgRGF0ZUhlbHBlclNlcnZpY2UgfSBmcm9tICcuLi9pMThuL2RhdGUtaGVscGVyLnNlcnZpY2UnO1xuaW1wb3J0IHsgTnpEYXRlUGlja2VySTE4bkludGVyZmFjZSB9IGZyb20gJy4uL2kxOG4vbnotaTE4bi5pbnRlcmZhY2UnO1xuaW1wb3J0IHsgTnpJMThuU2VydmljZSB9IGZyb20gJy4uL2kxOG4vbnotaTE4bi5zZXJ2aWNlJztcbmltcG9ydCB7IENhbmR5RGF0ZSB9IGZyb20gJy4vbGliL2NhbmR5LWRhdGUnO1xuaW1wb3J0IHsgTnpQaWNrZXJDb21wb25lbnQgfSBmcm9tICcuL3BpY2tlci5jb21wb25lbnQnO1xuXG5jb25zdCBQT1BVUF9TVFlMRV9QQVRDSCA9IHsgcG9zaXRpb246ICdyZWxhdGl2ZScgfTsgLy8gQWltIHRvIG92ZXJyaWRlIGFudGQncyBzdHlsZSB0byBzdXBwb3J0IG92ZXJsYXkncyBwb3NpdGlvbiBzdHJhdGVneSAocG9zaXRpb246YWJzb2x1dGUgd2lsbCBjYXVzZSBpdCBub3Qgd29ya2luZyBiZWFjdXNlIHRoZSBvdmVybGF5IGNhbid0IGdldCB0aGUgaGVpZ2h0L3dpZHRoIG9mIGl0J3MgY29udGVudClcblxuLyoqXG4gKiBUaGUgYmFzZSBwaWNrZXIgZm9yIGFsbCBjb21tb24gQVBJc1xuICovXG5leHBvcnQgYWJzdHJhY3QgY2xhc3MgQWJzdHJhY3RQaWNrZXJDb21wb25lbnQgaW1wbGVtZW50cyBPbkluaXQsIE9uQ2hhbmdlcywgT25EZXN0cm95LCBDb250cm9sVmFsdWVBY2Nlc3NvciB7XG4gIC8vIC0tLSBDb21tb24gQVBJXG4gIEBJbnB1dCgpIEBJbnB1dEJvb2xlYW4oKSBuekFsbG93Q2xlYXI6IGJvb2xlYW4gPSB0cnVlO1xuICBASW5wdXQoKSBASW5wdXRCb29sZWFuKCkgbnpBdXRvRm9jdXM6IGJvb2xlYW4gPSBmYWxzZTtcbiAgQElucHV0KCkgQElucHV0Qm9vbGVhbigpIG56RGlzYWJsZWQ6IGJvb2xlYW4gPSBmYWxzZTtcbiAgQElucHV0KCkgQElucHV0Qm9vbGVhbigpIG56T3BlbjogYm9vbGVhbjtcbiAgQElucHV0KCkgbnpDbGFzc05hbWU6IHN0cmluZztcbiAgQElucHV0KCkgbnpEaXNhYmxlZERhdGU6IChkOiBEYXRlKSA9PiBib29sZWFuO1xuICBASW5wdXQoKSBuekxvY2FsZTogTnpEYXRlUGlja2VySTE4bkludGVyZmFjZTtcbiAgQElucHV0KCkgbnpQbGFjZUhvbGRlcjogc3RyaW5nIHwgc3RyaW5nW107XG4gIEBJbnB1dCgpIG56UG9wdXBTdHlsZTogb2JqZWN0ID0gUE9QVVBfU1RZTEVfUEFUQ0g7XG4gIEBJbnB1dCgpIG56RHJvcGRvd25DbGFzc05hbWU6IHN0cmluZztcbiAgQElucHV0KCkgbnpTaXplOiAnbGFyZ2UnIHwgJ3NtYWxsJztcbiAgQElucHV0KCkgbnpTdHlsZTogb2JqZWN0O1xuICBASW5wdXQoKSBuekZvcm1hdDogc3RyaW5nO1xuICBASW5wdXQoKSBuelZhbHVlOiBDb21wYXRpYmxlVmFsdWUgfCBudWxsO1xuXG4gIEBPdXRwdXQoKSByZWFkb25seSBuek9uT3BlbkNoYW5nZSA9IG5ldyBFdmVudEVtaXR0ZXI8Ym9vbGVhbj4oKTtcblxuICBAVmlld0NoaWxkKE56UGlja2VyQ29tcG9uZW50KSBwcm90ZWN0ZWQgcGlja2VyOiBOelBpY2tlckNvbXBvbmVudDtcblxuICBpc1JhbmdlOiBib29sZWFuID0gZmFsc2U7IC8vIEluZGljYXRlIHdoZXRoZXIgdGhlIHZhbHVlIGlzIGEgcmFuZ2UgdmFsdWVcblxuICBnZXQgcmVhbE9wZW5TdGF0ZSgpOiBib29sZWFuIHtcbiAgICByZXR1cm4gdGhpcy5waWNrZXIuYW5pbWF0aW9uT3BlblN0YXRlO1xuICB9IC8vIFVzZSBwaWNrZXIncyByZWFsIG9wZW4gc3RhdGUgdG8gbGV0IHJlLXJlbmRlciB0aGUgcGlja2VyJ3MgY29udGVudCB3aGVuIHNob3duIHVwXG5cbiAgaW5pdFZhbHVlKCk6IHZvaWQge1xuICAgIHRoaXMubnpWYWx1ZSA9IHRoaXMuaXNSYW5nZSA/IFtdIDogbnVsbDtcbiAgfVxuXG4gIHByb3RlY3RlZCBkZXN0cm95ZWQkOiBTdWJqZWN0PHZvaWQ+ID0gbmV3IFN1YmplY3QoKTtcbiAgcHJvdGVjdGVkIGlzQ3VzdG9tUGxhY2VIb2xkZXI6IGJvb2xlYW4gPSBmYWxzZTtcblxuICBjb25zdHJ1Y3RvcihcbiAgICBwcm90ZWN0ZWQgaTE4bjogTnpJMThuU2VydmljZSxcbiAgICBwcm90ZWN0ZWQgY2RyOiBDaGFuZ2VEZXRlY3RvclJlZixcbiAgICBwcm90ZWN0ZWQgZGF0ZUhlbHBlcjogRGF0ZUhlbHBlclNlcnZpY2UsXG4gICAgcHVibGljIG5vQW5pbWF0aW9uPzogTnpOb0FuaW1hdGlvbkRpcmVjdGl2ZVxuICApIHt9XG5cbiAgbmdPbkluaXQoKTogdm9pZCB7XG4gICAgLy8gU3Vic2NyaWJlIHRoZSBldmVyeSBsb2NhbGUgY2hhbmdlIGlmIHRoZSBuekxvY2FsZSBpcyBub3QgaGFuZGxlZCBieSB1c2VyXG4gICAgaWYgKCF0aGlzLm56TG9jYWxlKSB7XG4gICAgICB0aGlzLmkxOG4ubG9jYWxlQ2hhbmdlLnBpcGUodGFrZVVudGlsKHRoaXMuZGVzdHJveWVkJCkpLnN1YnNjcmliZSgoKSA9PiB0aGlzLnNldExvY2FsZSgpKTtcbiAgICB9XG5cbiAgICAvLyBEZWZhdWx0IHZhbHVlXG4gICAgdGhpcy5pbml0VmFsdWUoKTtcbiAgfVxuXG4gIG5nT25DaGFuZ2VzKGNoYW5nZXM6IFNpbXBsZUNoYW5nZXMpOiB2b2lkIHtcbiAgICBpZiAoY2hhbmdlcy5uelBvcHVwU3R5bGUpIHtcbiAgICAgIC8vIEFsd2F5cyBhc3NpZ24gdGhlIHBvcHVwIHN0eWxlIHBhdGNoXG4gICAgICB0aGlzLm56UG9wdXBTdHlsZSA9IHRoaXMubnpQb3B1cFN0eWxlID8geyAuLi50aGlzLm56UG9wdXBTdHlsZSwgLi4uUE9QVVBfU1RZTEVfUEFUQ0ggfSA6IFBPUFVQX1NUWUxFX1BBVENIO1xuICAgIH1cblxuICAgIC8vIE1hcmsgYXMgY3VzdG9taXplZCBwbGFjZWhvbGRlciBieSB1c2VyIG9uY2UgbnpQbGFjZUhvbGRlciBhc3NpZ25lZCBhdCB0aGUgZmlyc3QgdGltZVxuICAgIGlmIChjaGFuZ2VzLm56UGxhY2VIb2xkZXIgJiYgY2hhbmdlcy5uelBsYWNlSG9sZGVyLmZpcnN0Q2hhbmdlICYmIHR5cGVvZiB0aGlzLm56UGxhY2VIb2xkZXIgIT09ICd1bmRlZmluZWQnKSB7XG4gICAgICB0aGlzLmlzQ3VzdG9tUGxhY2VIb2xkZXIgPSB0cnVlO1xuICAgIH1cblxuICAgIGlmIChjaGFuZ2VzLm56TG9jYWxlKSB7XG4gICAgICAvLyBUaGUgbnpMb2NhbGUgaXMgY3VycmVudGx5IGhhbmRsZWQgYnkgdXNlclxuICAgICAgdGhpcy5zZXREZWZhdWx0UGxhY2VIb2xkZXIoKTtcbiAgICB9XG4gIH1cblxuICBuZ09uRGVzdHJveSgpOiB2b2lkIHtcbiAgICB0aGlzLmRlc3Ryb3llZCQubmV4dCgpO1xuICAgIHRoaXMuZGVzdHJveWVkJC5jb21wbGV0ZSgpO1xuICB9XG5cbiAgY2xvc2VPdmVybGF5KCk6IHZvaWQge1xuICAgIHRoaXMucGlja2VyLmhpZGVPdmVybGF5KCk7XG4gIH1cblxuICAvKipcbiAgICogQ29tbW9uIGhhbmRsZSBmb3IgdmFsdWUgY2hhbmdlc1xuICAgKiBAcGFyYW0gdmFsdWUgY2hhbmdlZCB2YWx1ZVxuICAgKi9cbiAgb25WYWx1ZUNoYW5nZSh2YWx1ZTogQ29tcGF0aWJsZVZhbHVlKTogdm9pZCB7XG4gICAgdGhpcy5uelZhbHVlID0gdmFsdWU7XG4gICAgaWYgKHRoaXMuaXNSYW5nZSkge1xuICAgICAgY29uc3QgdkFzUmFuZ2UgPSB0aGlzLm56VmFsdWUgYXMgQ2FuZHlEYXRlW107XG4gICAgICBpZiAodkFzUmFuZ2UubGVuZ3RoKSB7XG4gICAgICAgIHRoaXMub25DaGFuZ2VGbihbdkFzUmFuZ2VbMF0ubmF0aXZlRGF0ZSwgdkFzUmFuZ2VbMV0ubmF0aXZlRGF0ZV0pO1xuICAgICAgfSBlbHNlIHtcbiAgICAgICAgdGhpcy5vbkNoYW5nZUZuKFtdKTtcbiAgICAgIH1cbiAgICB9IGVsc2Uge1xuICAgICAgaWYgKHRoaXMubnpWYWx1ZSkge1xuICAgICAgICB0aGlzLm9uQ2hhbmdlRm4oKHRoaXMubnpWYWx1ZSBhcyBDYW5keURhdGUpLm5hdGl2ZURhdGUpO1xuICAgICAgfSBlbHNlIHtcbiAgICAgICAgdGhpcy5vbkNoYW5nZUZuKG51bGwpO1xuICAgICAgfVxuICAgIH1cbiAgICB0aGlzLm9uVG91Y2hlZEZuKCk7XG4gIH1cblxuICAvKipcbiAgICogVHJpZ2dlcmVkIHdoZW4gb3ZlcmxheU9wZW4gY2hhbmdlcyAoZGlmZmVyZW50IHdpdGggcmVhbE9wZW5TdGF0ZSlcbiAgICogQHBhcmFtIG9wZW4gVGhlIG92ZXJsYXlPcGVuIGluIHBpY2tlciBjb21wb25lbnRcbiAgICovXG4gIG9uT3BlbkNoYW5nZShvcGVuOiBib29sZWFuKTogdm9pZCB7XG4gICAgdGhpcy5uek9uT3BlbkNoYW5nZS5lbWl0KG9wZW4pO1xuICB9XG5cbiAgLy8gLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG4gIC8vIHwgQ29udHJvbCB2YWx1ZSBhY2Nlc3NvciBpbXBsZW1lbnRzXG4gIC8vIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxuXG4gIC8vIE5PVEU6IG9uQ2hhbmdlRm4vb25Ub3VjaGVkRm4gd2lsbCBub3QgYmUgYXNzaWduZWQgaWYgdXNlciBub3QgdXNlIGFzIG5nTW9kZWxcbiAgb25DaGFuZ2VGbjogKHZhbDogQ29tcGF0aWJsZURhdGUgfCBudWxsKSA9PiB2b2lkID0gKCkgPT4gdm9pZCAwO1xuICBvblRvdWNoZWRGbjogKCkgPT4gdm9pZCA9ICgpID0+IHZvaWQgMDtcblxuICB3cml0ZVZhbHVlKHZhbHVlOiBDb21wYXRpYmxlRGF0ZSk6IHZvaWQge1xuICAgIHRoaXMuc2V0VmFsdWUodmFsdWUpO1xuICAgIHRoaXMuY2RyLm1hcmtGb3JDaGVjaygpO1xuICB9XG5cbiAgLy8gdHNsaW50OmRpc2FibGUtbmV4dC1saW5lOm5vLWFueVxuICByZWdpc3Rlck9uQ2hhbmdlKGZuOiBhbnkpOiB2b2lkIHtcbiAgICB0aGlzLm9uQ2hhbmdlRm4gPSBmbjtcbiAgfVxuXG4gIC8vIHRzbGludDpkaXNhYmxlLW5leHQtbGluZTpuby1hbnlcbiAgcmVnaXN0ZXJPblRvdWNoZWQoZm46IGFueSk6IHZvaWQge1xuICAgIHRoaXMub25Ub3VjaGVkRm4gPSBmbjtcbiAgfVxuXG4gIHNldERpc2FibGVkU3RhdGUoZGlzYWJsZWQ6IGJvb2xlYW4pOiB2b2lkIHtcbiAgICB0aGlzLm56RGlzYWJsZWQgPSBkaXNhYmxlZDtcbiAgICB0aGlzLmNkci5tYXJrRm9yQ2hlY2soKTtcbiAgfVxuXG4gIC8vIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxuICAvLyB8IEludGVybmFsIG1ldGhvZHNcbiAgLy8gLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG5cbiAgLy8gUmVsb2FkIGxvY2FsZSBmcm9tIGkxOG4gd2l0aCBzaWRlIGVmZmVjdHNcbiAgcHJpdmF0ZSBzZXRMb2NhbGUoKTogdm9pZCB7XG4gICAgdGhpcy5uekxvY2FsZSA9IHRoaXMuaTE4bi5nZXRMb2NhbGVEYXRhKCdEYXRlUGlja2VyJywge30pO1xuICAgIHRoaXMuc2V0RGVmYXVsdFBsYWNlSG9sZGVyKCk7XG4gICAgdGhpcy5jZHIubWFya0ZvckNoZWNrKCk7XG4gIH1cblxuICBwcml2YXRlIHNldERlZmF1bHRQbGFjZUhvbGRlcigpOiB2b2lkIHtcbiAgICBpZiAoIXRoaXMuaXNDdXN0b21QbGFjZUhvbGRlciAmJiB0aGlzLm56TG9jYWxlKSB7XG4gICAgICB0aGlzLm56UGxhY2VIb2xkZXIgPSB0aGlzLmlzUmFuZ2UgPyB0aGlzLm56TG9jYWxlLmxhbmcucmFuZ2VQbGFjZWhvbGRlciA6IHRoaXMubnpMb2NhbGUubGFuZy5wbGFjZWhvbGRlcjtcbiAgICB9XG4gIH1cblxuICAvLyBTYWZlIHdheSBvZiBzZXR0aW5nIHZhbHVlIHdpdGggZGVmYXVsdFxuICBwcml2YXRlIHNldFZhbHVlKHZhbHVlOiBDb21wYXRpYmxlRGF0ZSk6IHZvaWQge1xuICAgIGlmICh0aGlzLmlzUmFuZ2UpIHtcbiAgICAgIHRoaXMubnpWYWx1ZSA9IHZhbHVlID8gKHZhbHVlIGFzIERhdGVbXSkubWFwKHZhbCA9PiBuZXcgQ2FuZHlEYXRlKHZhbCkpIDogW107XG4gICAgfSBlbHNlIHtcbiAgICAgIHRoaXMubnpWYWx1ZSA9IHZhbHVlID8gbmV3IENhbmR5RGF0ZSh2YWx1ZSBhcyBEYXRlKSA6IG51bGw7XG4gICAgfVxuICB9XG59XG5cbmV4cG9ydCB0eXBlIENvbXBhdGlibGVWYWx1ZSA9IENhbmR5RGF0ZSB8IENhbmR5RGF0ZVtdO1xuXG5leHBvcnQgdHlwZSBDb21wYXRpYmxlRGF0ZSA9IERhdGUgfCBEYXRlW107XG4iXX0=