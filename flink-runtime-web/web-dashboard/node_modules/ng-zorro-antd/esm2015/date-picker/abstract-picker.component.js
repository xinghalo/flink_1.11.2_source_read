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
const POPUP_STYLE_PATCH = { position: 'relative' };
// Aim to override antd's style to support overlay's position strategy (position:absolute will cause it not working beacuse the overlay can't get the height/width of it's content)
/**
 * The base picker for all common APIs
 * @abstract
 */
export class AbstractPickerComponent {
    /**
     * @param {?} i18n
     * @param {?} cdr
     * @param {?} dateHelper
     * @param {?=} noAnimation
     */
    constructor(i18n, cdr, dateHelper, noAnimation) {
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
        () => void 0);
        this.onTouchedFn = (/**
         * @return {?}
         */
        () => void 0);
    }
    // Indicate whether the value is a range value
    /**
     * @return {?}
     */
    get realOpenState() {
        return this.picker.animationOpenState;
    } // Use picker's real open state to let re-render the picker's content when shown up
    // Use picker's real open state to let re-render the picker's content when shown up
    /**
     * @return {?}
     */
    initValue() {
        this.nzValue = this.isRange ? [] : null;
    }
    /**
     * @return {?}
     */
    ngOnInit() {
        // Subscribe the every locale change if the nzLocale is not handled by user
        if (!this.nzLocale) {
            this.i18n.localeChange.pipe(takeUntil(this.destroyed$)).subscribe((/**
             * @return {?}
             */
            () => this.setLocale()));
        }
        // Default value
        this.initValue();
    }
    /**
     * @param {?} changes
     * @return {?}
     */
    ngOnChanges(changes) {
        if (changes.nzPopupStyle) {
            // Always assign the popup style patch
            this.nzPopupStyle = this.nzPopupStyle ? Object.assign({}, this.nzPopupStyle, POPUP_STYLE_PATCH) : POPUP_STYLE_PATCH;
        }
        // Mark as customized placeholder by user once nzPlaceHolder assigned at the first time
        if (changes.nzPlaceHolder && changes.nzPlaceHolder.firstChange && typeof this.nzPlaceHolder !== 'undefined') {
            this.isCustomPlaceHolder = true;
        }
        if (changes.nzLocale) {
            // The nzLocale is currently handled by user
            this.setDefaultPlaceHolder();
        }
    }
    /**
     * @return {?}
     */
    ngOnDestroy() {
        this.destroyed$.next();
        this.destroyed$.complete();
    }
    /**
     * @return {?}
     */
    closeOverlay() {
        this.picker.hideOverlay();
    }
    /**
     * Common handle for value changes
     * @param {?} value changed value
     * @return {?}
     */
    onValueChange(value) {
        this.nzValue = value;
        if (this.isRange) {
            /** @type {?} */
            const vAsRange = (/** @type {?} */ (this.nzValue));
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
    }
    /**
     * Triggered when overlayOpen changes (different with realOpenState)
     * @param {?} open The overlayOpen in picker component
     * @return {?}
     */
    onOpenChange(open) {
        this.nzOnOpenChange.emit(open);
    }
    /**
     * @param {?} value
     * @return {?}
     */
    writeValue(value) {
        this.setValue(value);
        this.cdr.markForCheck();
    }
    // tslint:disable-next-line:no-any
    /**
     * @param {?} fn
     * @return {?}
     */
    registerOnChange(fn) {
        this.onChangeFn = fn;
    }
    // tslint:disable-next-line:no-any
    /**
     * @param {?} fn
     * @return {?}
     */
    registerOnTouched(fn) {
        this.onTouchedFn = fn;
    }
    /**
     * @param {?} disabled
     * @return {?}
     */
    setDisabledState(disabled) {
        this.nzDisabled = disabled;
        this.cdr.markForCheck();
    }
    // ------------------------------------------------------------------------
    // | Internal methods
    // ------------------------------------------------------------------------
    // Reload locale from i18n with side effects
    /**
     * @private
     * @return {?}
     */
    setLocale() {
        this.nzLocale = this.i18n.getLocaleData('DatePicker', {});
        this.setDefaultPlaceHolder();
        this.cdr.markForCheck();
    }
    /**
     * @private
     * @return {?}
     */
    setDefaultPlaceHolder() {
        if (!this.isCustomPlaceHolder && this.nzLocale) {
            this.nzPlaceHolder = this.isRange ? this.nzLocale.lang.rangePlaceholder : this.nzLocale.lang.placeholder;
        }
    }
    // Safe way of setting value with default
    /**
     * @private
     * @param {?} value
     * @return {?}
     */
    setValue(value) {
        if (this.isRange) {
            this.nzValue = value ? ((/** @type {?} */ (value))).map((/**
             * @param {?} val
             * @return {?}
             */
            val => new CandyDate(val))) : [];
        }
        else {
            this.nzValue = value ? new CandyDate((/** @type {?} */ (value))) : null;
        }
    }
}
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
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiYWJzdHJhY3QtcGlja2VyLmNvbXBvbmVudC5qcyIsInNvdXJjZVJvb3QiOiJuZzovL25nLXpvcnJvLWFudGQvIiwic291cmNlcyI6WyJkYXRlLXBpY2tlci9hYnN0cmFjdC1waWNrZXIuY29tcG9uZW50LnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7O0FBQUEsT0FBTyxFQUVMLFlBQVksRUFDWixLQUFLLEVBSUwsTUFBTSxFQUVOLFNBQVMsRUFDVixNQUFNLGVBQWUsQ0FBQztBQUd2QixPQUFPLEVBQUUsT0FBTyxFQUFFLE1BQU0sTUFBTSxDQUFDO0FBQy9CLE9BQU8sRUFBRSxTQUFTLEVBQUUsTUFBTSxnQkFBZ0IsQ0FBQztBQUUzQyxPQUFPLEVBQUUsWUFBWSxFQUFFLE1BQU0sc0JBQXNCLENBQUM7QUFJcEQsT0FBTyxFQUFFLFNBQVMsRUFBRSxNQUFNLGtCQUFrQixDQUFDO0FBQzdDLE9BQU8sRUFBRSxpQkFBaUIsRUFBRSxNQUFNLG9CQUFvQixDQUFDOztNQUVqRCxpQkFBaUIsR0FBRyxFQUFFLFFBQVEsRUFBRSxVQUFVLEVBQUU7Ozs7OztBQUtsRCxNQUFNLE9BQWdCLHVCQUF1Qjs7Ozs7OztJQWtDM0MsWUFDWSxJQUFtQixFQUNuQixHQUFzQixFQUN0QixVQUE2QixFQUNoQyxXQUFvQztRQUhqQyxTQUFJLEdBQUosSUFBSSxDQUFlO1FBQ25CLFFBQUcsR0FBSCxHQUFHLENBQW1CO1FBQ3RCLGVBQVUsR0FBVixVQUFVLENBQW1CO1FBQ2hDLGdCQUFXLEdBQVgsV0FBVyxDQUF5Qjs7UUFwQ3BCLGlCQUFZLEdBQVksSUFBSSxDQUFDO1FBQzdCLGdCQUFXLEdBQVksS0FBSyxDQUFDO1FBQzdCLGVBQVUsR0FBWSxLQUFLLENBQUM7UUFNNUMsaUJBQVksR0FBVyxpQkFBaUIsQ0FBQztRQU8vQixtQkFBYyxHQUFHLElBQUksWUFBWSxFQUFXLENBQUM7UUFJaEUsWUFBTyxHQUFZLEtBQUssQ0FBQyxDQUFDLDhDQUE4QztRQVU5RCxlQUFVLEdBQWtCLElBQUksT0FBTyxFQUFFLENBQUM7UUFDMUMsd0JBQW1CLEdBQVksS0FBSyxDQUFDOzs7OztRQWlGL0MsZUFBVTs7O1FBQXlDLEdBQUcsRUFBRSxDQUFDLEtBQUssQ0FBQyxFQUFDO1FBQ2hFLGdCQUFXOzs7UUFBZSxHQUFHLEVBQUUsQ0FBQyxLQUFLLENBQUMsRUFBQztJQTNFcEMsQ0FBQzs7Ozs7SUFoQkosSUFBSSxhQUFhO1FBQ2YsT0FBTyxJQUFJLENBQUMsTUFBTSxDQUFDLGtCQUFrQixDQUFDO0lBQ3hDLENBQUMsQ0FBQyxtRkFBbUY7Ozs7O0lBRXJGLFNBQVM7UUFDUCxJQUFJLENBQUMsT0FBTyxHQUFHLElBQUksQ0FBQyxPQUFPLENBQUMsQ0FBQyxDQUFDLEVBQUUsQ0FBQyxDQUFDLENBQUMsSUFBSSxDQUFDO0lBQzFDLENBQUM7Ozs7SUFZRCxRQUFRO1FBQ04sMkVBQTJFO1FBQzNFLElBQUksQ0FBQyxJQUFJLENBQUMsUUFBUSxFQUFFO1lBQ2xCLElBQUksQ0FBQyxJQUFJLENBQUMsWUFBWSxDQUFDLElBQUksQ0FBQyxTQUFTLENBQUMsSUFBSSxDQUFDLFVBQVUsQ0FBQyxDQUFDLENBQUMsU0FBUzs7O1lBQUMsR0FBRyxFQUFFLENBQUMsSUFBSSxDQUFDLFNBQVMsRUFBRSxFQUFDLENBQUM7U0FDM0Y7UUFFRCxnQkFBZ0I7UUFDaEIsSUFBSSxDQUFDLFNBQVMsRUFBRSxDQUFDO0lBQ25CLENBQUM7Ozs7O0lBRUQsV0FBVyxDQUFDLE9BQXNCO1FBQ2hDLElBQUksT0FBTyxDQUFDLFlBQVksRUFBRTtZQUN4QixzQ0FBc0M7WUFDdEMsSUFBSSxDQUFDLFlBQVksR0FBRyxJQUFJLENBQUMsWUFBWSxDQUFDLENBQUMsbUJBQU0sSUFBSSxDQUFDLFlBQVksRUFBSyxpQkFBaUIsRUFBRyxDQUFDLENBQUMsaUJBQWlCLENBQUM7U0FDNUc7UUFFRCx1RkFBdUY7UUFDdkYsSUFBSSxPQUFPLENBQUMsYUFBYSxJQUFJLE9BQU8sQ0FBQyxhQUFhLENBQUMsV0FBVyxJQUFJLE9BQU8sSUFBSSxDQUFDLGFBQWEsS0FBSyxXQUFXLEVBQUU7WUFDM0csSUFBSSxDQUFDLG1CQUFtQixHQUFHLElBQUksQ0FBQztTQUNqQztRQUVELElBQUksT0FBTyxDQUFDLFFBQVEsRUFBRTtZQUNwQiw0Q0FBNEM7WUFDNUMsSUFBSSxDQUFDLHFCQUFxQixFQUFFLENBQUM7U0FDOUI7SUFDSCxDQUFDOzs7O0lBRUQsV0FBVztRQUNULElBQUksQ0FBQyxVQUFVLENBQUMsSUFBSSxFQUFFLENBQUM7UUFDdkIsSUFBSSxDQUFDLFVBQVUsQ0FBQyxRQUFRLEVBQUUsQ0FBQztJQUM3QixDQUFDOzs7O0lBRUQsWUFBWTtRQUNWLElBQUksQ0FBQyxNQUFNLENBQUMsV0FBVyxFQUFFLENBQUM7SUFDNUIsQ0FBQzs7Ozs7O0lBTUQsYUFBYSxDQUFDLEtBQXNCO1FBQ2xDLElBQUksQ0FBQyxPQUFPLEdBQUcsS0FBSyxDQUFDO1FBQ3JCLElBQUksSUFBSSxDQUFDLE9BQU8sRUFBRTs7a0JBQ1YsUUFBUSxHQUFHLG1CQUFBLElBQUksQ0FBQyxPQUFPLEVBQWU7WUFDNUMsSUFBSSxRQUFRLENBQUMsTUFBTSxFQUFFO2dCQUNuQixJQUFJLENBQUMsVUFBVSxDQUFDLENBQUMsUUFBUSxDQUFDLENBQUMsQ0FBQyxDQUFDLFVBQVUsRUFBRSxRQUFRLENBQUMsQ0FBQyxDQUFDLENBQUMsVUFBVSxDQUFDLENBQUMsQ0FBQzthQUNuRTtpQkFBTTtnQkFDTCxJQUFJLENBQUMsVUFBVSxDQUFDLEVBQUUsQ0FBQyxDQUFDO2FBQ3JCO1NBQ0Y7YUFBTTtZQUNMLElBQUksSUFBSSxDQUFDLE9BQU8sRUFBRTtnQkFDaEIsSUFBSSxDQUFDLFVBQVUsQ0FBQyxDQUFDLG1CQUFBLElBQUksQ0FBQyxPQUFPLEVBQWEsQ0FBQyxDQUFDLFVBQVUsQ0FBQyxDQUFDO2FBQ3pEO2lCQUFNO2dCQUNMLElBQUksQ0FBQyxVQUFVLENBQUMsSUFBSSxDQUFDLENBQUM7YUFDdkI7U0FDRjtRQUNELElBQUksQ0FBQyxXQUFXLEVBQUUsQ0FBQztJQUNyQixDQUFDOzs7Ozs7SUFNRCxZQUFZLENBQUMsSUFBYTtRQUN4QixJQUFJLENBQUMsY0FBYyxDQUFDLElBQUksQ0FBQyxJQUFJLENBQUMsQ0FBQztJQUNqQyxDQUFDOzs7OztJQVVELFVBQVUsQ0FBQyxLQUFxQjtRQUM5QixJQUFJLENBQUMsUUFBUSxDQUFDLEtBQUssQ0FBQyxDQUFDO1FBQ3JCLElBQUksQ0FBQyxHQUFHLENBQUMsWUFBWSxFQUFFLENBQUM7SUFDMUIsQ0FBQzs7Ozs7O0lBR0QsZ0JBQWdCLENBQUMsRUFBTztRQUN0QixJQUFJLENBQUMsVUFBVSxHQUFHLEVBQUUsQ0FBQztJQUN2QixDQUFDOzs7Ozs7SUFHRCxpQkFBaUIsQ0FBQyxFQUFPO1FBQ3ZCLElBQUksQ0FBQyxXQUFXLEdBQUcsRUFBRSxDQUFDO0lBQ3hCLENBQUM7Ozs7O0lBRUQsZ0JBQWdCLENBQUMsUUFBaUI7UUFDaEMsSUFBSSxDQUFDLFVBQVUsR0FBRyxRQUFRLENBQUM7UUFDM0IsSUFBSSxDQUFDLEdBQUcsQ0FBQyxZQUFZLEVBQUUsQ0FBQztJQUMxQixDQUFDOzs7Ozs7Ozs7SUFPTyxTQUFTO1FBQ2YsSUFBSSxDQUFDLFFBQVEsR0FBRyxJQUFJLENBQUMsSUFBSSxDQUFDLGFBQWEsQ0FBQyxZQUFZLEVBQUUsRUFBRSxDQUFDLENBQUM7UUFDMUQsSUFBSSxDQUFDLHFCQUFxQixFQUFFLENBQUM7UUFDN0IsSUFBSSxDQUFDLEdBQUcsQ0FBQyxZQUFZLEVBQUUsQ0FBQztJQUMxQixDQUFDOzs7OztJQUVPLHFCQUFxQjtRQUMzQixJQUFJLENBQUMsSUFBSSxDQUFDLG1CQUFtQixJQUFJLElBQUksQ0FBQyxRQUFRLEVBQUU7WUFDOUMsSUFBSSxDQUFDLGFBQWEsR0FBRyxJQUFJLENBQUMsT0FBTyxDQUFDLENBQUMsQ0FBQyxJQUFJLENBQUMsUUFBUSxDQUFDLElBQUksQ0FBQyxnQkFBZ0IsQ0FBQyxDQUFDLENBQUMsSUFBSSxDQUFDLFFBQVEsQ0FBQyxJQUFJLENBQUMsV0FBVyxDQUFDO1NBQzFHO0lBQ0gsQ0FBQzs7Ozs7OztJQUdPLFFBQVEsQ0FBQyxLQUFxQjtRQUNwQyxJQUFJLElBQUksQ0FBQyxPQUFPLEVBQUU7WUFDaEIsSUFBSSxDQUFDLE9BQU8sR0FBRyxLQUFLLENBQUMsQ0FBQyxDQUFDLENBQUMsbUJBQUEsS0FBSyxFQUFVLENBQUMsQ0FBQyxHQUFHOzs7O1lBQUMsR0FBRyxDQUFDLEVBQUUsQ0FBQyxJQUFJLFNBQVMsQ0FBQyxHQUFHLENBQUMsRUFBQyxDQUFDLENBQUMsQ0FBQyxFQUFFLENBQUM7U0FDOUU7YUFBTTtZQUNMLElBQUksQ0FBQyxPQUFPLEdBQUcsS0FBSyxDQUFDLENBQUMsQ0FBQyxJQUFJLFNBQVMsQ0FBQyxtQkFBQSxLQUFLLEVBQVEsQ0FBQyxDQUFDLENBQUMsQ0FBQyxJQUFJLENBQUM7U0FDNUQ7SUFDSCxDQUFDOzs7MkJBOUpBLEtBQUs7MEJBQ0wsS0FBSzt5QkFDTCxLQUFLO3FCQUNMLEtBQUs7MEJBQ0wsS0FBSzs2QkFDTCxLQUFLO3VCQUNMLEtBQUs7NEJBQ0wsS0FBSzsyQkFDTCxLQUFLO2tDQUNMLEtBQUs7cUJBQ0wsS0FBSztzQkFDTCxLQUFLO3VCQUNMLEtBQUs7c0JBQ0wsS0FBSzs2QkFFTCxNQUFNO3FCQUVOLFNBQVMsU0FBQyxpQkFBaUI7O0FBakJIO0lBQWYsWUFBWSxFQUFFOzs2REFBOEI7QUFDN0I7SUFBZixZQUFZLEVBQUU7OzREQUE4QjtBQUM3QjtJQUFmLFlBQVksRUFBRTs7MkRBQTZCO0FBQzVCO0lBQWYsWUFBWSxFQUFFOzt1REFBaUI7OztJQUh6QywrQ0FBc0Q7O0lBQ3RELDhDQUFzRDs7SUFDdEQsNkNBQXFEOztJQUNyRCx5Q0FBeUM7O0lBQ3pDLDhDQUE2Qjs7SUFDN0IsaURBQThDOztJQUM5QywyQ0FBNkM7O0lBQzdDLGdEQUEwQzs7SUFDMUMsK0NBQWtEOztJQUNsRCxzREFBcUM7O0lBQ3JDLHlDQUFtQzs7SUFDbkMsMENBQXlCOztJQUN6QiwyQ0FBMEI7O0lBQzFCLDBDQUF5Qzs7SUFFekMsaURBQWdFOzs7OztJQUVoRSx5Q0FBa0U7O0lBRWxFLDBDQUF5Qjs7Ozs7SUFVekIsNkNBQW9EOzs7OztJQUNwRCxzREFBK0M7O0lBaUYvQyw2Q0FBZ0U7O0lBQ2hFLDhDQUF1Qzs7Ozs7SUEvRXJDLHVDQUE2Qjs7Ozs7SUFDN0Isc0NBQWdDOzs7OztJQUNoQyw2Q0FBdUM7O0lBQ3ZDLDhDQUEyQyIsInNvdXJjZXNDb250ZW50IjpbImltcG9ydCB7XG4gIENoYW5nZURldGVjdG9yUmVmLFxuICBFdmVudEVtaXR0ZXIsXG4gIElucHV0LFxuICBPbkNoYW5nZXMsXG4gIE9uRGVzdHJveSxcbiAgT25Jbml0LFxuICBPdXRwdXQsXG4gIFNpbXBsZUNoYW5nZXMsXG4gIFZpZXdDaGlsZFxufSBmcm9tICdAYW5ndWxhci9jb3JlJztcbmltcG9ydCB7IENvbnRyb2xWYWx1ZUFjY2Vzc29yIH0gZnJvbSAnQGFuZ3VsYXIvZm9ybXMnO1xuXG5pbXBvcnQgeyBTdWJqZWN0IH0gZnJvbSAncnhqcyc7XG5pbXBvcnQgeyB0YWtlVW50aWwgfSBmcm9tICdyeGpzL29wZXJhdG9ycyc7XG5pbXBvcnQgeyBOek5vQW5pbWF0aW9uRGlyZWN0aXZlIH0gZnJvbSAnLi4vY29yZS9uby1hbmltYXRpb24vbnotbm8tYW5pbWF0aW9uLmRpcmVjdGl2ZSc7XG5pbXBvcnQgeyBJbnB1dEJvb2xlYW4gfSBmcm9tICcuLi9jb3JlL3V0aWwvY29udmVydCc7XG5pbXBvcnQgeyBEYXRlSGVscGVyU2VydmljZSB9IGZyb20gJy4uL2kxOG4vZGF0ZS1oZWxwZXIuc2VydmljZSc7XG5pbXBvcnQgeyBOekRhdGVQaWNrZXJJMThuSW50ZXJmYWNlIH0gZnJvbSAnLi4vaTE4bi9uei1pMThuLmludGVyZmFjZSc7XG5pbXBvcnQgeyBOekkxOG5TZXJ2aWNlIH0gZnJvbSAnLi4vaTE4bi9uei1pMThuLnNlcnZpY2UnO1xuaW1wb3J0IHsgQ2FuZHlEYXRlIH0gZnJvbSAnLi9saWIvY2FuZHktZGF0ZSc7XG5pbXBvcnQgeyBOelBpY2tlckNvbXBvbmVudCB9IGZyb20gJy4vcGlja2VyLmNvbXBvbmVudCc7XG5cbmNvbnN0IFBPUFVQX1NUWUxFX1BBVENIID0geyBwb3NpdGlvbjogJ3JlbGF0aXZlJyB9OyAvLyBBaW0gdG8gb3ZlcnJpZGUgYW50ZCdzIHN0eWxlIHRvIHN1cHBvcnQgb3ZlcmxheSdzIHBvc2l0aW9uIHN0cmF0ZWd5IChwb3NpdGlvbjphYnNvbHV0ZSB3aWxsIGNhdXNlIGl0IG5vdCB3b3JraW5nIGJlYWN1c2UgdGhlIG92ZXJsYXkgY2FuJ3QgZ2V0IHRoZSBoZWlnaHQvd2lkdGggb2YgaXQncyBjb250ZW50KVxuXG4vKipcbiAqIFRoZSBiYXNlIHBpY2tlciBmb3IgYWxsIGNvbW1vbiBBUElzXG4gKi9cbmV4cG9ydCBhYnN0cmFjdCBjbGFzcyBBYnN0cmFjdFBpY2tlckNvbXBvbmVudCBpbXBsZW1lbnRzIE9uSW5pdCwgT25DaGFuZ2VzLCBPbkRlc3Ryb3ksIENvbnRyb2xWYWx1ZUFjY2Vzc29yIHtcbiAgLy8gLS0tIENvbW1vbiBBUElcbiAgQElucHV0KCkgQElucHV0Qm9vbGVhbigpIG56QWxsb3dDbGVhcjogYm9vbGVhbiA9IHRydWU7XG4gIEBJbnB1dCgpIEBJbnB1dEJvb2xlYW4oKSBuekF1dG9Gb2N1czogYm9vbGVhbiA9IGZhbHNlO1xuICBASW5wdXQoKSBASW5wdXRCb29sZWFuKCkgbnpEaXNhYmxlZDogYm9vbGVhbiA9IGZhbHNlO1xuICBASW5wdXQoKSBASW5wdXRCb29sZWFuKCkgbnpPcGVuOiBib29sZWFuO1xuICBASW5wdXQoKSBuekNsYXNzTmFtZTogc3RyaW5nO1xuICBASW5wdXQoKSBuekRpc2FibGVkRGF0ZTogKGQ6IERhdGUpID0+IGJvb2xlYW47XG4gIEBJbnB1dCgpIG56TG9jYWxlOiBOekRhdGVQaWNrZXJJMThuSW50ZXJmYWNlO1xuICBASW5wdXQoKSBuelBsYWNlSG9sZGVyOiBzdHJpbmcgfCBzdHJpbmdbXTtcbiAgQElucHV0KCkgbnpQb3B1cFN0eWxlOiBvYmplY3QgPSBQT1BVUF9TVFlMRV9QQVRDSDtcbiAgQElucHV0KCkgbnpEcm9wZG93bkNsYXNzTmFtZTogc3RyaW5nO1xuICBASW5wdXQoKSBuelNpemU6ICdsYXJnZScgfCAnc21hbGwnO1xuICBASW5wdXQoKSBuelN0eWxlOiBvYmplY3Q7XG4gIEBJbnB1dCgpIG56Rm9ybWF0OiBzdHJpbmc7XG4gIEBJbnB1dCgpIG56VmFsdWU6IENvbXBhdGlibGVWYWx1ZSB8IG51bGw7XG5cbiAgQE91dHB1dCgpIHJlYWRvbmx5IG56T25PcGVuQ2hhbmdlID0gbmV3IEV2ZW50RW1pdHRlcjxib29sZWFuPigpO1xuXG4gIEBWaWV3Q2hpbGQoTnpQaWNrZXJDb21wb25lbnQpIHByb3RlY3RlZCBwaWNrZXI6IE56UGlja2VyQ29tcG9uZW50O1xuXG4gIGlzUmFuZ2U6IGJvb2xlYW4gPSBmYWxzZTsgLy8gSW5kaWNhdGUgd2hldGhlciB0aGUgdmFsdWUgaXMgYSByYW5nZSB2YWx1ZVxuXG4gIGdldCByZWFsT3BlblN0YXRlKCk6IGJvb2xlYW4ge1xuICAgIHJldHVybiB0aGlzLnBpY2tlci5hbmltYXRpb25PcGVuU3RhdGU7XG4gIH0gLy8gVXNlIHBpY2tlcidzIHJlYWwgb3BlbiBzdGF0ZSB0byBsZXQgcmUtcmVuZGVyIHRoZSBwaWNrZXIncyBjb250ZW50IHdoZW4gc2hvd24gdXBcblxuICBpbml0VmFsdWUoKTogdm9pZCB7XG4gICAgdGhpcy5uelZhbHVlID0gdGhpcy5pc1JhbmdlID8gW10gOiBudWxsO1xuICB9XG5cbiAgcHJvdGVjdGVkIGRlc3Ryb3llZCQ6IFN1YmplY3Q8dm9pZD4gPSBuZXcgU3ViamVjdCgpO1xuICBwcm90ZWN0ZWQgaXNDdXN0b21QbGFjZUhvbGRlcjogYm9vbGVhbiA9IGZhbHNlO1xuXG4gIGNvbnN0cnVjdG9yKFxuICAgIHByb3RlY3RlZCBpMThuOiBOekkxOG5TZXJ2aWNlLFxuICAgIHByb3RlY3RlZCBjZHI6IENoYW5nZURldGVjdG9yUmVmLFxuICAgIHByb3RlY3RlZCBkYXRlSGVscGVyOiBEYXRlSGVscGVyU2VydmljZSxcbiAgICBwdWJsaWMgbm9BbmltYXRpb24/OiBOek5vQW5pbWF0aW9uRGlyZWN0aXZlXG4gICkge31cblxuICBuZ09uSW5pdCgpOiB2b2lkIHtcbiAgICAvLyBTdWJzY3JpYmUgdGhlIGV2ZXJ5IGxvY2FsZSBjaGFuZ2UgaWYgdGhlIG56TG9jYWxlIGlzIG5vdCBoYW5kbGVkIGJ5IHVzZXJcbiAgICBpZiAoIXRoaXMubnpMb2NhbGUpIHtcbiAgICAgIHRoaXMuaTE4bi5sb2NhbGVDaGFuZ2UucGlwZSh0YWtlVW50aWwodGhpcy5kZXN0cm95ZWQkKSkuc3Vic2NyaWJlKCgpID0+IHRoaXMuc2V0TG9jYWxlKCkpO1xuICAgIH1cblxuICAgIC8vIERlZmF1bHQgdmFsdWVcbiAgICB0aGlzLmluaXRWYWx1ZSgpO1xuICB9XG5cbiAgbmdPbkNoYW5nZXMoY2hhbmdlczogU2ltcGxlQ2hhbmdlcyk6IHZvaWQge1xuICAgIGlmIChjaGFuZ2VzLm56UG9wdXBTdHlsZSkge1xuICAgICAgLy8gQWx3YXlzIGFzc2lnbiB0aGUgcG9wdXAgc3R5bGUgcGF0Y2hcbiAgICAgIHRoaXMubnpQb3B1cFN0eWxlID0gdGhpcy5uelBvcHVwU3R5bGUgPyB7IC4uLnRoaXMubnpQb3B1cFN0eWxlLCAuLi5QT1BVUF9TVFlMRV9QQVRDSCB9IDogUE9QVVBfU1RZTEVfUEFUQ0g7XG4gICAgfVxuXG4gICAgLy8gTWFyayBhcyBjdXN0b21pemVkIHBsYWNlaG9sZGVyIGJ5IHVzZXIgb25jZSBuelBsYWNlSG9sZGVyIGFzc2lnbmVkIGF0IHRoZSBmaXJzdCB0aW1lXG4gICAgaWYgKGNoYW5nZXMubnpQbGFjZUhvbGRlciAmJiBjaGFuZ2VzLm56UGxhY2VIb2xkZXIuZmlyc3RDaGFuZ2UgJiYgdHlwZW9mIHRoaXMubnpQbGFjZUhvbGRlciAhPT0gJ3VuZGVmaW5lZCcpIHtcbiAgICAgIHRoaXMuaXNDdXN0b21QbGFjZUhvbGRlciA9IHRydWU7XG4gICAgfVxuXG4gICAgaWYgKGNoYW5nZXMubnpMb2NhbGUpIHtcbiAgICAgIC8vIFRoZSBuekxvY2FsZSBpcyBjdXJyZW50bHkgaGFuZGxlZCBieSB1c2VyXG4gICAgICB0aGlzLnNldERlZmF1bHRQbGFjZUhvbGRlcigpO1xuICAgIH1cbiAgfVxuXG4gIG5nT25EZXN0cm95KCk6IHZvaWQge1xuICAgIHRoaXMuZGVzdHJveWVkJC5uZXh0KCk7XG4gICAgdGhpcy5kZXN0cm95ZWQkLmNvbXBsZXRlKCk7XG4gIH1cblxuICBjbG9zZU92ZXJsYXkoKTogdm9pZCB7XG4gICAgdGhpcy5waWNrZXIuaGlkZU92ZXJsYXkoKTtcbiAgfVxuXG4gIC8qKlxuICAgKiBDb21tb24gaGFuZGxlIGZvciB2YWx1ZSBjaGFuZ2VzXG4gICAqIEBwYXJhbSB2YWx1ZSBjaGFuZ2VkIHZhbHVlXG4gICAqL1xuICBvblZhbHVlQ2hhbmdlKHZhbHVlOiBDb21wYXRpYmxlVmFsdWUpOiB2b2lkIHtcbiAgICB0aGlzLm56VmFsdWUgPSB2YWx1ZTtcbiAgICBpZiAodGhpcy5pc1JhbmdlKSB7XG4gICAgICBjb25zdCB2QXNSYW5nZSA9IHRoaXMubnpWYWx1ZSBhcyBDYW5keURhdGVbXTtcbiAgICAgIGlmICh2QXNSYW5nZS5sZW5ndGgpIHtcbiAgICAgICAgdGhpcy5vbkNoYW5nZUZuKFt2QXNSYW5nZVswXS5uYXRpdmVEYXRlLCB2QXNSYW5nZVsxXS5uYXRpdmVEYXRlXSk7XG4gICAgICB9IGVsc2Uge1xuICAgICAgICB0aGlzLm9uQ2hhbmdlRm4oW10pO1xuICAgICAgfVxuICAgIH0gZWxzZSB7XG4gICAgICBpZiAodGhpcy5uelZhbHVlKSB7XG4gICAgICAgIHRoaXMub25DaGFuZ2VGbigodGhpcy5uelZhbHVlIGFzIENhbmR5RGF0ZSkubmF0aXZlRGF0ZSk7XG4gICAgICB9IGVsc2Uge1xuICAgICAgICB0aGlzLm9uQ2hhbmdlRm4obnVsbCk7XG4gICAgICB9XG4gICAgfVxuICAgIHRoaXMub25Ub3VjaGVkRm4oKTtcbiAgfVxuXG4gIC8qKlxuICAgKiBUcmlnZ2VyZWQgd2hlbiBvdmVybGF5T3BlbiBjaGFuZ2VzIChkaWZmZXJlbnQgd2l0aCByZWFsT3BlblN0YXRlKVxuICAgKiBAcGFyYW0gb3BlbiBUaGUgb3ZlcmxheU9wZW4gaW4gcGlja2VyIGNvbXBvbmVudFxuICAgKi9cbiAgb25PcGVuQ2hhbmdlKG9wZW46IGJvb2xlYW4pOiB2b2lkIHtcbiAgICB0aGlzLm56T25PcGVuQ2hhbmdlLmVtaXQob3Blbik7XG4gIH1cblxuICAvLyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cbiAgLy8gfCBDb250cm9sIHZhbHVlIGFjY2Vzc29yIGltcGxlbWVudHNcbiAgLy8gLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG5cbiAgLy8gTk9URTogb25DaGFuZ2VGbi9vblRvdWNoZWRGbiB3aWxsIG5vdCBiZSBhc3NpZ25lZCBpZiB1c2VyIG5vdCB1c2UgYXMgbmdNb2RlbFxuICBvbkNoYW5nZUZuOiAodmFsOiBDb21wYXRpYmxlRGF0ZSB8IG51bGwpID0+IHZvaWQgPSAoKSA9PiB2b2lkIDA7XG4gIG9uVG91Y2hlZEZuOiAoKSA9PiB2b2lkID0gKCkgPT4gdm9pZCAwO1xuXG4gIHdyaXRlVmFsdWUodmFsdWU6IENvbXBhdGlibGVEYXRlKTogdm9pZCB7XG4gICAgdGhpcy5zZXRWYWx1ZSh2YWx1ZSk7XG4gICAgdGhpcy5jZHIubWFya0ZvckNoZWNrKCk7XG4gIH1cblxuICAvLyB0c2xpbnQ6ZGlzYWJsZS1uZXh0LWxpbmU6bm8tYW55XG4gIHJlZ2lzdGVyT25DaGFuZ2UoZm46IGFueSk6IHZvaWQge1xuICAgIHRoaXMub25DaGFuZ2VGbiA9IGZuO1xuICB9XG5cbiAgLy8gdHNsaW50OmRpc2FibGUtbmV4dC1saW5lOm5vLWFueVxuICByZWdpc3Rlck9uVG91Y2hlZChmbjogYW55KTogdm9pZCB7XG4gICAgdGhpcy5vblRvdWNoZWRGbiA9IGZuO1xuICB9XG5cbiAgc2V0RGlzYWJsZWRTdGF0ZShkaXNhYmxlZDogYm9vbGVhbik6IHZvaWQge1xuICAgIHRoaXMubnpEaXNhYmxlZCA9IGRpc2FibGVkO1xuICAgIHRoaXMuY2RyLm1hcmtGb3JDaGVjaygpO1xuICB9XG5cbiAgLy8gLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG4gIC8vIHwgSW50ZXJuYWwgbWV0aG9kc1xuICAvLyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cblxuICAvLyBSZWxvYWQgbG9jYWxlIGZyb20gaTE4biB3aXRoIHNpZGUgZWZmZWN0c1xuICBwcml2YXRlIHNldExvY2FsZSgpOiB2b2lkIHtcbiAgICB0aGlzLm56TG9jYWxlID0gdGhpcy5pMThuLmdldExvY2FsZURhdGEoJ0RhdGVQaWNrZXInLCB7fSk7XG4gICAgdGhpcy5zZXREZWZhdWx0UGxhY2VIb2xkZXIoKTtcbiAgICB0aGlzLmNkci5tYXJrRm9yQ2hlY2soKTtcbiAgfVxuXG4gIHByaXZhdGUgc2V0RGVmYXVsdFBsYWNlSG9sZGVyKCk6IHZvaWQge1xuICAgIGlmICghdGhpcy5pc0N1c3RvbVBsYWNlSG9sZGVyICYmIHRoaXMubnpMb2NhbGUpIHtcbiAgICAgIHRoaXMubnpQbGFjZUhvbGRlciA9IHRoaXMuaXNSYW5nZSA/IHRoaXMubnpMb2NhbGUubGFuZy5yYW5nZVBsYWNlaG9sZGVyIDogdGhpcy5uekxvY2FsZS5sYW5nLnBsYWNlaG9sZGVyO1xuICAgIH1cbiAgfVxuXG4gIC8vIFNhZmUgd2F5IG9mIHNldHRpbmcgdmFsdWUgd2l0aCBkZWZhdWx0XG4gIHByaXZhdGUgc2V0VmFsdWUodmFsdWU6IENvbXBhdGlibGVEYXRlKTogdm9pZCB7XG4gICAgaWYgKHRoaXMuaXNSYW5nZSkge1xuICAgICAgdGhpcy5uelZhbHVlID0gdmFsdWUgPyAodmFsdWUgYXMgRGF0ZVtdKS5tYXAodmFsID0+IG5ldyBDYW5keURhdGUodmFsKSkgOiBbXTtcbiAgICB9IGVsc2Uge1xuICAgICAgdGhpcy5uelZhbHVlID0gdmFsdWUgPyBuZXcgQ2FuZHlEYXRlKHZhbHVlIGFzIERhdGUpIDogbnVsbDtcbiAgICB9XG4gIH1cbn1cblxuZXhwb3J0IHR5cGUgQ29tcGF0aWJsZVZhbHVlID0gQ2FuZHlEYXRlIHwgQ2FuZHlEYXRlW107XG5cbmV4cG9ydCB0eXBlIENvbXBhdGlibGVEYXRlID0gRGF0ZSB8IERhdGVbXTtcbiJdfQ==