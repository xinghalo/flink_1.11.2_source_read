/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import * as tslib_1 from "tslib";
import { FocusMonitor } from '@angular/cdk/a11y';
import { DOWN_ARROW, ENTER, UP_ARROW } from '@angular/cdk/keycodes';
import { forwardRef, ChangeDetectionStrategy, ChangeDetectorRef, Component, ElementRef, EventEmitter, Input, Output, Renderer2, ViewChild, ViewEncapsulation } from '@angular/core';
import { NG_VALUE_ACCESSOR } from '@angular/forms';
import { isNotNil } from '../core/util/check';
import { InputBoolean } from '../core/util/convert';
export class NzInputNumberComponent {
    /**
     * @param {?} elementRef
     * @param {?} renderer
     * @param {?} cdr
     * @param {?} focusMonitor
     */
    constructor(elementRef, renderer, cdr, focusMonitor) {
        this.elementRef = elementRef;
        this.renderer = renderer;
        this.cdr = cdr;
        this.focusMonitor = focusMonitor;
        this.isFocused = false;
        this.disabledUp = false;
        this.disabledDown = false;
        this.onChange = (/**
         * @return {?}
         */
        () => null);
        this.onTouched = (/**
         * @return {?}
         */
        () => null);
        this.nzBlur = new EventEmitter();
        this.nzFocus = new EventEmitter();
        this.nzSize = 'default';
        this.nzMin = -Infinity;
        this.nzMax = Infinity;
        this.nzParser = (/**
         * @param {?} value
         * @return {?}
         */
        (value) => value); // tslint:disable-line:no-any
        this.nzPlaceHolder = '';
        this.nzStep = 1;
        this.nzDisabled = false;
        this.nzAutoFocus = false;
        this.nzFormatter = (/**
         * @param {?} value
         * @return {?}
         */
        value => value);
        renderer.addClass(elementRef.nativeElement, 'ant-input-number');
    }
    // tslint:disable-line:no-any
    /**
     * @return {?}
     */
    updateAutoFocus() {
        if (this.nzAutoFocus) {
            this.renderer.setAttribute(this.inputElement.nativeElement, 'autofocus', 'autofocus');
        }
        else {
            this.renderer.removeAttribute(this.inputElement.nativeElement, 'autofocus');
        }
    }
    /**
     * @param {?} value
     * @return {?}
     */
    onModelChange(value) {
        this.actualValue = this.nzParser(value
            .trim()
            .replace(/。/g, '.')
            .replace(/[^\w\.-]+/g, ''));
        this.inputElement.nativeElement.value = this.actualValue;
    }
    /**
     * @param {?} value
     * @return {?}
     */
    getCurrentValidValue(value) {
        /** @type {?} */
        let val = value;
        if (val === '') {
            val = '';
        }
        else if (!this.isNotCompleteNumber(val)) {
            val = (/** @type {?} */ (this.getValidValue(val)));
        }
        else {
            val = this.value;
        }
        return this.toNumber(val);
    }
    // '1.' '1x' 'xx' '' => are not complete numbers
    /**
     * @param {?} num
     * @return {?}
     */
    isNotCompleteNumber(num) {
        return (isNaN((/** @type {?} */ (num))) ||
            num === '' ||
            num === null ||
            !!(num && num.toString().indexOf('.') === num.toString().length - 1));
    }
    /**
     * @param {?=} value
     * @return {?}
     */
    getValidValue(value) {
        /** @type {?} */
        let val = parseFloat((/** @type {?} */ (value)));
        // https://github.com/ant-design/ant-design/issues/7358
        if (isNaN(val)) {
            return value;
        }
        if (val < this.nzMin) {
            val = this.nzMin;
        }
        if (val > this.nzMax) {
            val = this.nzMax;
        }
        return val;
    }
    /**
     * @param {?} num
     * @return {?}
     */
    toNumber(num) {
        if (this.isNotCompleteNumber(num)) {
            return (/** @type {?} */ (num));
        }
        if (isNotNil(this.nzPrecision)) {
            return Number(Number(num).toFixed(this.nzPrecision));
        }
        return Number(num);
    }
    /**
     * @return {?}
     */
    setValidateValue() {
        /** @type {?} */
        const value = this.getCurrentValidValue(this.actualValue);
        this.setValue(value, `${this.value}` !== `${value}`);
    }
    /**
     * @return {?}
     */
    onBlur() {
        this.isFocused = false;
        this.setValidateValue();
    }
    /**
     * @return {?}
     */
    onFocus() {
        this.isFocused = true;
    }
    /**
     * @param {?} e
     * @return {?}
     */
    getRatio(e) {
        /** @type {?} */
        let ratio = 1;
        if (e.metaKey || e.ctrlKey) {
            ratio = 0.1;
        }
        else if (e.shiftKey) {
            ratio = 10;
        }
        return ratio;
    }
    /**
     * @param {?} e
     * @param {?=} ratio
     * @return {?}
     */
    down(e, ratio) {
        if (!this.isFocused) {
            this.focus();
        }
        this.step('down', e, ratio);
    }
    /**
     * @param {?} e
     * @param {?=} ratio
     * @return {?}
     */
    up(e, ratio) {
        if (!this.isFocused) {
            this.focus();
        }
        this.step('up', e, ratio);
    }
    /**
     * @param {?} value
     * @return {?}
     */
    getPrecision(value) {
        /** @type {?} */
        const valueString = value.toString();
        if (valueString.indexOf('e-') >= 0) {
            return parseInt(valueString.slice(valueString.indexOf('e-') + 2), 10);
        }
        /** @type {?} */
        let precision = 0;
        if (valueString.indexOf('.') >= 0) {
            precision = valueString.length - valueString.indexOf('.') - 1;
        }
        return precision;
    }
    // step={1.0} value={1.51}
    // press +
    // then value should be 2.51, rather than 2.5
    // if this.props.precision is undefined
    // https://github.com/react-component/input-number/issues/39
    /**
     * @param {?} currentValue
     * @param {?} ratio
     * @return {?}
     */
    getMaxPrecision(currentValue, ratio) {
        if (isNotNil(this.nzPrecision)) {
            return this.nzPrecision;
        }
        /** @type {?} */
        const ratioPrecision = this.getPrecision(ratio);
        /** @type {?} */
        const stepPrecision = this.getPrecision(this.nzStep);
        /** @type {?} */
        const currentValuePrecision = this.getPrecision((/** @type {?} */ (currentValue)));
        if (!currentValue) {
            return ratioPrecision + stepPrecision;
        }
        return Math.max(currentValuePrecision, ratioPrecision + stepPrecision);
    }
    /**
     * @param {?} currentValue
     * @param {?} ratio
     * @return {?}
     */
    getPrecisionFactor(currentValue, ratio) {
        /** @type {?} */
        const precision = this.getMaxPrecision(currentValue, ratio);
        return Math.pow(10, precision);
    }
    /**
     * @param {?} val
     * @param {?} rat
     * @return {?}
     */
    upStep(val, rat) {
        /** @type {?} */
        const precisionFactor = this.getPrecisionFactor(val, rat);
        /** @type {?} */
        const precision = Math.abs(this.getMaxPrecision(val, rat));
        /** @type {?} */
        let result;
        if (typeof val === 'number') {
            result = ((precisionFactor * val + precisionFactor * this.nzStep * rat) / precisionFactor).toFixed(precision);
        }
        else {
            result = this.nzMin === -Infinity ? this.nzStep : this.nzMin;
        }
        return this.toNumber(result);
    }
    /**
     * @param {?} val
     * @param {?} rat
     * @return {?}
     */
    downStep(val, rat) {
        /** @type {?} */
        const precisionFactor = this.getPrecisionFactor(val, rat);
        /** @type {?} */
        const precision = Math.abs(this.getMaxPrecision(val, rat));
        /** @type {?} */
        let result;
        if (typeof val === 'number') {
            result = ((precisionFactor * val - precisionFactor * this.nzStep * rat) / precisionFactor).toFixed(precision);
        }
        else {
            result = this.nzMin === -Infinity ? -this.nzStep : this.nzMin;
        }
        return this.toNumber(result);
    }
    /**
     * @param {?} type
     * @param {?} e
     * @param {?=} ratio
     * @return {?}
     */
    step(type, e, ratio = 1) {
        this.stop();
        e.preventDefault();
        if (this.nzDisabled) {
            return;
        }
        /** @type {?} */
        const value = this.getCurrentValidValue(this.actualValue) || 0;
        /** @type {?} */
        let val = 0;
        if (type === 'up') {
            val = this.upStep(value, ratio);
        }
        else if (type === 'down') {
            val = this.downStep(value, ratio);
        }
        /** @type {?} */
        const outOfRange = val > this.nzMax || val < this.nzMin;
        if (val > this.nzMax) {
            val = this.nzMax;
        }
        else if (val < this.nzMin) {
            val = this.nzMin;
        }
        this.setValue(val, true);
        this.isFocused = true;
        if (outOfRange) {
            return;
        }
        this.autoStepTimer = setTimeout((/**
         * @return {?}
         */
        () => {
            this[type](e, ratio, true);
        }), 600);
    }
    /**
     * @return {?}
     */
    stop() {
        if (this.autoStepTimer) {
            clearTimeout(this.autoStepTimer);
        }
    }
    /**
     * @param {?} value
     * @param {?} emit
     * @return {?}
     */
    setValue(value, emit) {
        if (emit && `${this.value}` !== `${value}`) {
            this.onChange(value);
        }
        this.value = value;
        this.actualValue = value;
        /** @type {?} */
        const displayValue = isNotNil(this.nzFormatter(this.value)) ? this.nzFormatter(this.value) : '';
        this.displayValue = displayValue;
        this.inputElement.nativeElement.value = displayValue;
        this.disabledUp = this.disabledDown = false;
        if (value || value === 0) {
            /** @type {?} */
            const val = Number(value);
            if (val >= this.nzMax) {
                this.disabledUp = true;
            }
            if (val <= this.nzMin) {
                this.disabledDown = true;
            }
        }
    }
    /**
     * @param {?} e
     * @return {?}
     */
    onKeyDown(e) {
        if (e.code === 'ArrowUp' || e.keyCode === UP_ARROW) {
            /** @type {?} */
            const ratio = this.getRatio(e);
            this.up(e, ratio);
            this.stop();
        }
        else if (e.code === 'ArrowDown' || e.keyCode === DOWN_ARROW) {
            /** @type {?} */
            const ratio = this.getRatio(e);
            this.down(e, ratio);
            this.stop();
        }
        else if (e.keyCode === ENTER) {
            this.setValidateValue();
        }
    }
    /**
     * @return {?}
     */
    onKeyUp() {
        this.stop();
    }
    /**
     * @param {?} value
     * @return {?}
     */
    writeValue(value) {
        this.setValue(value, false);
        this.cdr.markForCheck();
    }
    /**
     * @param {?} fn
     * @return {?}
     */
    registerOnChange(fn) {
        this.onChange = fn;
    }
    /**
     * @param {?} fn
     * @return {?}
     */
    registerOnTouched(fn) {
        this.onTouched = fn;
    }
    /**
     * @param {?} isDisabled
     * @return {?}
     */
    setDisabledState(isDisabled) {
        this.nzDisabled = isDisabled;
        this.cdr.markForCheck();
    }
    /**
     * @return {?}
     */
    focus() {
        this.focusMonitor.focusVia(this.inputElement, 'keyboard');
    }
    /**
     * @return {?}
     */
    blur() {
        this.inputElement.nativeElement.blur();
    }
    /**
     * @return {?}
     */
    ngOnInit() {
        this.focusMonitor.monitor(this.elementRef, true).subscribe((/**
         * @param {?} focusOrigin
         * @return {?}
         */
        focusOrigin => {
            if (!focusOrigin) {
                this.nzBlur.emit();
                Promise.resolve().then((/**
                 * @return {?}
                 */
                () => this.onTouched()));
            }
            else {
                this.nzFocus.emit();
            }
        }));
    }
    /**
     * @param {?} changes
     * @return {?}
     */
    ngOnChanges(changes) {
        if (changes.nzAutoFocus) {
            this.updateAutoFocus();
        }
        if (changes.nzFormatter) {
            /** @type {?} */
            const value = this.getCurrentValidValue(this.actualValue);
            this.setValue(value, true);
        }
    }
    /**
     * @return {?}
     */
    ngAfterViewInit() {
        if (this.nzAutoFocus) {
            this.focus();
        }
    }
    /**
     * @return {?}
     */
    ngOnDestroy() {
        this.focusMonitor.stopMonitoring(this.elementRef);
    }
}
NzInputNumberComponent.decorators = [
    { type: Component, args: [{
                selector: 'nz-input-number',
                template: "<div class=\"ant-input-number-handler-wrap\">\n  <span unselectable=\"unselectable\"\n    class=\"ant-input-number-handler ant-input-number-handler-up\"\n    (mousedown)=\"up($event)\"\n    (mouseup)=\"stop()\"\n    (mouseleave)=\"stop()\"\n    [class.ant-input-number-handler-up-disabled]=\"disabledUp\">\n    <i nz-icon type=\"up\" class=\"ant-input-number-handler-up-inner\"></i>\n  </span>\n  <span unselectable=\"unselectable\"\n    class=\"ant-input-number-handler ant-input-number-handler-down\"\n    (mousedown)=\"down($event)\"\n    (mouseup)=\"stop()\"\n    (mouseleave)=\"stop()\"\n    [class.ant-input-number-handler-down-disabled]=\"disabledDown\">\n    <i nz-icon type=\"down\" class=\"ant-input-number-handler-down-inner\"></i>\n  </span>\n</div>\n<div class=\"ant-input-number-input-wrap\">\n  <input #inputElement\n    autocomplete=\"off\"\n    class=\"ant-input-number-input\"\n    [disabled]=\"nzDisabled\"\n    [attr.min]=\"nzMin\"\n    [attr.max]=\"nzMax\"\n    [placeholder]=\"nzPlaceHolder\"\n    [attr.step]=\"nzStep\"\n    (keydown)=\"onKeyDown($event)\"\n    (keyup)=\"onKeyUp()\"\n    (blur)=\"onBlur()\"\n    (focus)=\"onFocus()\"\n    [ngModel]=\"displayValue\"\n    (ngModelChange)=\"onModelChange($event)\">\n</div>",
                providers: [
                    {
                        provide: NG_VALUE_ACCESSOR,
                        useExisting: forwardRef((/**
                         * @return {?}
                         */
                        () => NzInputNumberComponent)),
                        multi: true
                    }
                ],
                changeDetection: ChangeDetectionStrategy.OnPush,
                encapsulation: ViewEncapsulation.None,
                host: {
                    '[class.ant-input-number-focused]': 'isFocused',
                    '[class.ant-input-number-lg]': `nzSize === 'large'`,
                    '[class.ant-input-number-sm]': `nzSize === 'small'`,
                    '[class.ant-input-number-disabled]': 'nzDisabled'
                }
            }] }
];
/** @nocollapse */
NzInputNumberComponent.ctorParameters = () => [
    { type: ElementRef },
    { type: Renderer2 },
    { type: ChangeDetectorRef },
    { type: FocusMonitor }
];
NzInputNumberComponent.propDecorators = {
    nzBlur: [{ type: Output }],
    nzFocus: [{ type: Output }],
    inputElement: [{ type: ViewChild, args: ['inputElement',] }],
    nzSize: [{ type: Input }],
    nzMin: [{ type: Input }],
    nzMax: [{ type: Input }],
    nzParser: [{ type: Input }],
    nzPrecision: [{ type: Input }],
    nzPlaceHolder: [{ type: Input }],
    nzStep: [{ type: Input }],
    nzDisabled: [{ type: Input }],
    nzAutoFocus: [{ type: Input }],
    nzFormatter: [{ type: Input }]
};
tslib_1.__decorate([
    InputBoolean(),
    tslib_1.__metadata("design:type", Object)
], NzInputNumberComponent.prototype, "nzDisabled", void 0);
tslib_1.__decorate([
    InputBoolean(),
    tslib_1.__metadata("design:type", Object)
], NzInputNumberComponent.prototype, "nzAutoFocus", void 0);
if (false) {
    /**
     * @type {?}
     * @private
     */
    NzInputNumberComponent.prototype.autoStepTimer;
    /**
     * @type {?}
     * @private
     */
    NzInputNumberComponent.prototype.actualValue;
    /**
     * @type {?}
     * @private
     */
    NzInputNumberComponent.prototype.value;
    /** @type {?} */
    NzInputNumberComponent.prototype.displayValue;
    /** @type {?} */
    NzInputNumberComponent.prototype.isFocused;
    /** @type {?} */
    NzInputNumberComponent.prototype.disabledUp;
    /** @type {?} */
    NzInputNumberComponent.prototype.disabledDown;
    /** @type {?} */
    NzInputNumberComponent.prototype.onChange;
    /** @type {?} */
    NzInputNumberComponent.prototype.onTouched;
    /** @type {?} */
    NzInputNumberComponent.prototype.nzBlur;
    /** @type {?} */
    NzInputNumberComponent.prototype.nzFocus;
    /** @type {?} */
    NzInputNumberComponent.prototype.inputElement;
    /** @type {?} */
    NzInputNumberComponent.prototype.nzSize;
    /** @type {?} */
    NzInputNumberComponent.prototype.nzMin;
    /** @type {?} */
    NzInputNumberComponent.prototype.nzMax;
    /** @type {?} */
    NzInputNumberComponent.prototype.nzParser;
    /** @type {?} */
    NzInputNumberComponent.prototype.nzPrecision;
    /** @type {?} */
    NzInputNumberComponent.prototype.nzPlaceHolder;
    /** @type {?} */
    NzInputNumberComponent.prototype.nzStep;
    /** @type {?} */
    NzInputNumberComponent.prototype.nzDisabled;
    /** @type {?} */
    NzInputNumberComponent.prototype.nzAutoFocus;
    /** @type {?} */
    NzInputNumberComponent.prototype.nzFormatter;
    /**
     * @type {?}
     * @private
     */
    NzInputNumberComponent.prototype.elementRef;
    /**
     * @type {?}
     * @private
     */
    NzInputNumberComponent.prototype.renderer;
    /**
     * @type {?}
     * @private
     */
    NzInputNumberComponent.prototype.cdr;
    /**
     * @type {?}
     * @private
     */
    NzInputNumberComponent.prototype.focusMonitor;
    /* Skipping unhandled member: [property: string]: any;*/
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotaW5wdXQtbnVtYmVyLmNvbXBvbmVudC5qcyIsInNvdXJjZVJvb3QiOiJuZzovL25nLXpvcnJvLWFudGQvIiwic291cmNlcyI6WyJpbnB1dC1udW1iZXIvbnotaW5wdXQtbnVtYmVyLmNvbXBvbmVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7OztBQUFBLE9BQU8sRUFBRSxZQUFZLEVBQUUsTUFBTSxtQkFBbUIsQ0FBQztBQUNqRCxPQUFPLEVBQUUsVUFBVSxFQUFFLEtBQUssRUFBRSxRQUFRLEVBQUUsTUFBTSx1QkFBdUIsQ0FBQztBQUNwRSxPQUFPLEVBQ0wsVUFBVSxFQUVWLHVCQUF1QixFQUN2QixpQkFBaUIsRUFDakIsU0FBUyxFQUNULFVBQVUsRUFDVixZQUFZLEVBQ1osS0FBSyxFQUlMLE1BQU0sRUFDTixTQUFTLEVBRVQsU0FBUyxFQUNULGlCQUFpQixFQUNsQixNQUFNLGVBQWUsQ0FBQztBQUN2QixPQUFPLEVBQXdCLGlCQUFpQixFQUFFLE1BQU0sZ0JBQWdCLENBQUM7QUFHekUsT0FBTyxFQUFFLFFBQVEsRUFBRSxNQUFNLG9CQUFvQixDQUFDO0FBQzlDLE9BQU8sRUFBRSxZQUFZLEVBQUUsTUFBTSxzQkFBc0IsQ0FBQztBQXFCcEQsTUFBTSxPQUFPLHNCQUFzQjs7Ozs7OztJQWdTakMsWUFDVSxVQUFzQixFQUN0QixRQUFtQixFQUNuQixHQUFzQixFQUN0QixZQUEwQjtRQUgxQixlQUFVLEdBQVYsVUFBVSxDQUFZO1FBQ3RCLGFBQVEsR0FBUixRQUFRLENBQVc7UUFDbkIsUUFBRyxHQUFILEdBQUcsQ0FBbUI7UUFDdEIsaUJBQVksR0FBWixZQUFZLENBQWM7UUEvUnBDLGNBQVMsR0FBRyxLQUFLLENBQUM7UUFDbEIsZUFBVSxHQUFHLEtBQUssQ0FBQztRQUNuQixpQkFBWSxHQUFHLEtBQUssQ0FBQztRQUNyQixhQUFROzs7UUFBNEIsR0FBRyxFQUFFLENBQUMsSUFBSSxFQUFDO1FBQy9DLGNBQVM7OztRQUFlLEdBQUcsRUFBRSxDQUFDLElBQUksRUFBQztRQUNoQixXQUFNLEdBQUcsSUFBSSxZQUFZLEVBQUUsQ0FBQztRQUM1QixZQUFPLEdBQUcsSUFBSSxZQUFZLEVBQUUsQ0FBQztRQUV2QyxXQUFNLEdBQWtCLFNBQVMsQ0FBQztRQUNsQyxVQUFLLEdBQVcsQ0FBQyxRQUFRLENBQUM7UUFDMUIsVUFBSyxHQUFXLFFBQVEsQ0FBQztRQUN6QixhQUFROzs7O1FBQUcsQ0FBQyxLQUFVLEVBQUUsRUFBRSxDQUFDLEtBQUssRUFBQyxDQUFDLDZCQUE2QjtRQUUvRCxrQkFBYSxHQUFHLEVBQUUsQ0FBQztRQUNuQixXQUFNLEdBQUcsQ0FBQyxDQUFDO1FBQ0ssZUFBVSxHQUFHLEtBQUssQ0FBQztRQUNuQixnQkFBVyxHQUFHLEtBQUssQ0FBQztRQUNwQyxnQkFBVzs7OztRQUF1QyxLQUFLLENBQUMsRUFBRSxDQUFDLEtBQUssRUFBQztRQWdSeEUsUUFBUSxDQUFDLFFBQVEsQ0FBQyxVQUFVLENBQUMsYUFBYSxFQUFFLGtCQUFrQixDQUFDLENBQUM7SUFDbEUsQ0FBQzs7Ozs7SUE3UUQsZUFBZTtRQUNiLElBQUksSUFBSSxDQUFDLFdBQVcsRUFBRTtZQUNwQixJQUFJLENBQUMsUUFBUSxDQUFDLFlBQVksQ0FBQyxJQUFJLENBQUMsWUFBWSxDQUFDLGFBQWEsRUFBRSxXQUFXLEVBQUUsV0FBVyxDQUFDLENBQUM7U0FDdkY7YUFBTTtZQUNMLElBQUksQ0FBQyxRQUFRLENBQUMsZUFBZSxDQUFDLElBQUksQ0FBQyxZQUFZLENBQUMsYUFBYSxFQUFFLFdBQVcsQ0FBQyxDQUFDO1NBQzdFO0lBQ0gsQ0FBQzs7Ozs7SUFFRCxhQUFhLENBQUMsS0FBYTtRQUN6QixJQUFJLENBQUMsV0FBVyxHQUFHLElBQUksQ0FBQyxRQUFRLENBQzlCLEtBQUs7YUFDRixJQUFJLEVBQUU7YUFDTixPQUFPLENBQUMsSUFBSSxFQUFFLEdBQUcsQ0FBQzthQUNsQixPQUFPLENBQUMsWUFBWSxFQUFFLEVBQUUsQ0FBQyxDQUM3QixDQUFDO1FBQ0YsSUFBSSxDQUFDLFlBQVksQ0FBQyxhQUFhLENBQUMsS0FBSyxHQUFHLElBQUksQ0FBQyxXQUFXLENBQUM7SUFDM0QsQ0FBQzs7Ozs7SUFFRCxvQkFBb0IsQ0FBQyxLQUFzQjs7WUFDckMsR0FBRyxHQUFHLEtBQUs7UUFDZixJQUFJLEdBQUcsS0FBSyxFQUFFLEVBQUU7WUFDZCxHQUFHLEdBQUcsRUFBRSxDQUFDO1NBQ1Y7YUFBTSxJQUFJLENBQUMsSUFBSSxDQUFDLG1CQUFtQixDQUFDLEdBQUcsQ0FBQyxFQUFFO1lBQ3pDLEdBQUcsR0FBRyxtQkFBQSxJQUFJLENBQUMsYUFBYSxDQUFDLEdBQUcsQ0FBQyxFQUFVLENBQUM7U0FDekM7YUFBTTtZQUNMLEdBQUcsR0FBRyxJQUFJLENBQUMsS0FBSyxDQUFDO1NBQ2xCO1FBQ0QsT0FBTyxJQUFJLENBQUMsUUFBUSxDQUFDLEdBQUcsQ0FBQyxDQUFDO0lBQzVCLENBQUM7Ozs7OztJQUdELG1CQUFtQixDQUFDLEdBQW9CO1FBQ3RDLE9BQU8sQ0FDTCxLQUFLLENBQUMsbUJBQUEsR0FBRyxFQUFVLENBQUM7WUFDcEIsR0FBRyxLQUFLLEVBQUU7WUFDVixHQUFHLEtBQUssSUFBSTtZQUNaLENBQUMsQ0FBQyxDQUFDLEdBQUcsSUFBSSxHQUFHLENBQUMsUUFBUSxFQUFFLENBQUMsT0FBTyxDQUFDLEdBQUcsQ0FBQyxLQUFLLEdBQUcsQ0FBQyxRQUFRLEVBQUUsQ0FBQyxNQUFNLEdBQUcsQ0FBQyxDQUFDLENBQ3JFLENBQUM7SUFDSixDQUFDOzs7OztJQUVELGFBQWEsQ0FBQyxLQUF1Qjs7WUFDL0IsR0FBRyxHQUFHLFVBQVUsQ0FBQyxtQkFBQSxLQUFLLEVBQVUsQ0FBQztRQUNyQyx1REFBdUQ7UUFDdkQsSUFBSSxLQUFLLENBQUMsR0FBRyxDQUFDLEVBQUU7WUFDZCxPQUFPLEtBQUssQ0FBQztTQUNkO1FBQ0QsSUFBSSxHQUFHLEdBQUcsSUFBSSxDQUFDLEtBQUssRUFBRTtZQUNwQixHQUFHLEdBQUcsSUFBSSxDQUFDLEtBQUssQ0FBQztTQUNsQjtRQUNELElBQUksR0FBRyxHQUFHLElBQUksQ0FBQyxLQUFLLEVBQUU7WUFDcEIsR0FBRyxHQUFHLElBQUksQ0FBQyxLQUFLLENBQUM7U0FDbEI7UUFDRCxPQUFPLEdBQUcsQ0FBQztJQUNiLENBQUM7Ozs7O0lBRUQsUUFBUSxDQUFDLEdBQW9CO1FBQzNCLElBQUksSUFBSSxDQUFDLG1CQUFtQixDQUFDLEdBQUcsQ0FBQyxFQUFFO1lBQ2pDLE9BQU8sbUJBQUEsR0FBRyxFQUFVLENBQUM7U0FDdEI7UUFDRCxJQUFJLFFBQVEsQ0FBQyxJQUFJLENBQUMsV0FBVyxDQUFDLEVBQUU7WUFDOUIsT0FBTyxNQUFNLENBQUMsTUFBTSxDQUFDLEdBQUcsQ0FBQyxDQUFDLE9BQU8sQ0FBQyxJQUFJLENBQUMsV0FBVyxDQUFDLENBQUMsQ0FBQztTQUN0RDtRQUNELE9BQU8sTUFBTSxDQUFDLEdBQUcsQ0FBQyxDQUFDO0lBQ3JCLENBQUM7Ozs7SUFFRCxnQkFBZ0I7O2NBQ1IsS0FBSyxHQUFHLElBQUksQ0FBQyxvQkFBb0IsQ0FBQyxJQUFJLENBQUMsV0FBVyxDQUFDO1FBQ3pELElBQUksQ0FBQyxRQUFRLENBQUMsS0FBSyxFQUFFLEdBQUcsSUFBSSxDQUFDLEtBQUssRUFBRSxLQUFLLEdBQUcsS0FBSyxFQUFFLENBQUMsQ0FBQztJQUN2RCxDQUFDOzs7O0lBRUQsTUFBTTtRQUNKLElBQUksQ0FBQyxTQUFTLEdBQUcsS0FBSyxDQUFDO1FBQ3ZCLElBQUksQ0FBQyxnQkFBZ0IsRUFBRSxDQUFDO0lBQzFCLENBQUM7Ozs7SUFFRCxPQUFPO1FBQ0wsSUFBSSxDQUFDLFNBQVMsR0FBRyxJQUFJLENBQUM7SUFDeEIsQ0FBQzs7Ozs7SUFFRCxRQUFRLENBQUMsQ0FBZ0I7O1lBQ25CLEtBQUssR0FBRyxDQUFDO1FBQ2IsSUFBSSxDQUFDLENBQUMsT0FBTyxJQUFJLENBQUMsQ0FBQyxPQUFPLEVBQUU7WUFDMUIsS0FBSyxHQUFHLEdBQUcsQ0FBQztTQUNiO2FBQU0sSUFBSSxDQUFDLENBQUMsUUFBUSxFQUFFO1lBQ3JCLEtBQUssR0FBRyxFQUFFLENBQUM7U0FDWjtRQUNELE9BQU8sS0FBSyxDQUFDO0lBQ2YsQ0FBQzs7Ozs7O0lBRUQsSUFBSSxDQUFDLENBQTZCLEVBQUUsS0FBYztRQUNoRCxJQUFJLENBQUMsSUFBSSxDQUFDLFNBQVMsRUFBRTtZQUNuQixJQUFJLENBQUMsS0FBSyxFQUFFLENBQUM7U0FDZDtRQUNELElBQUksQ0FBQyxJQUFJLENBQUMsTUFBTSxFQUFFLENBQUMsRUFBRSxLQUFLLENBQUMsQ0FBQztJQUM5QixDQUFDOzs7Ozs7SUFFRCxFQUFFLENBQUMsQ0FBNkIsRUFBRSxLQUFjO1FBQzlDLElBQUksQ0FBQyxJQUFJLENBQUMsU0FBUyxFQUFFO1lBQ25CLElBQUksQ0FBQyxLQUFLLEVBQUUsQ0FBQztTQUNkO1FBQ0QsSUFBSSxDQUFDLElBQUksQ0FBQyxJQUFJLEVBQUUsQ0FBQyxFQUFFLEtBQUssQ0FBQyxDQUFDO0lBQzVCLENBQUM7Ozs7O0lBRUQsWUFBWSxDQUFDLEtBQWE7O2NBQ2xCLFdBQVcsR0FBRyxLQUFLLENBQUMsUUFBUSxFQUFFO1FBQ3BDLElBQUksV0FBVyxDQUFDLE9BQU8sQ0FBQyxJQUFJLENBQUMsSUFBSSxDQUFDLEVBQUU7WUFDbEMsT0FBTyxRQUFRLENBQUMsV0FBVyxDQUFDLEtBQUssQ0FBQyxXQUFXLENBQUMsT0FBTyxDQUFDLElBQUksQ0FBQyxHQUFHLENBQUMsQ0FBQyxFQUFFLEVBQUUsQ0FBQyxDQUFDO1NBQ3ZFOztZQUNHLFNBQVMsR0FBRyxDQUFDO1FBQ2pCLElBQUksV0FBVyxDQUFDLE9BQU8sQ0FBQyxHQUFHLENBQUMsSUFBSSxDQUFDLEVBQUU7WUFDakMsU0FBUyxHQUFHLFdBQVcsQ0FBQyxNQUFNLEdBQUcsV0FBVyxDQUFDLE9BQU8sQ0FBQyxHQUFHLENBQUMsR0FBRyxDQUFDLENBQUM7U0FDL0Q7UUFDRCxPQUFPLFNBQVMsQ0FBQztJQUNuQixDQUFDOzs7Ozs7Ozs7OztJQU9ELGVBQWUsQ0FBQyxZQUE2QixFQUFFLEtBQWE7UUFDMUQsSUFBSSxRQUFRLENBQUMsSUFBSSxDQUFDLFdBQVcsQ0FBQyxFQUFFO1lBQzlCLE9BQU8sSUFBSSxDQUFDLFdBQVcsQ0FBQztTQUN6Qjs7Y0FDSyxjQUFjLEdBQUcsSUFBSSxDQUFDLFlBQVksQ0FBQyxLQUFLLENBQUM7O2NBQ3pDLGFBQWEsR0FBRyxJQUFJLENBQUMsWUFBWSxDQUFDLElBQUksQ0FBQyxNQUFNLENBQUM7O2NBQzlDLHFCQUFxQixHQUFHLElBQUksQ0FBQyxZQUFZLENBQUMsbUJBQUEsWUFBWSxFQUFVLENBQUM7UUFDdkUsSUFBSSxDQUFDLFlBQVksRUFBRTtZQUNqQixPQUFPLGNBQWMsR0FBRyxhQUFhLENBQUM7U0FDdkM7UUFDRCxPQUFPLElBQUksQ0FBQyxHQUFHLENBQUMscUJBQXFCLEVBQUUsY0FBYyxHQUFHLGFBQWEsQ0FBQyxDQUFDO0lBQ3pFLENBQUM7Ozs7OztJQUVELGtCQUFrQixDQUFDLFlBQTZCLEVBQUUsS0FBYTs7Y0FDdkQsU0FBUyxHQUFHLElBQUksQ0FBQyxlQUFlLENBQUMsWUFBWSxFQUFFLEtBQUssQ0FBQztRQUMzRCxPQUFPLElBQUksQ0FBQyxHQUFHLENBQUMsRUFBRSxFQUFFLFNBQVMsQ0FBQyxDQUFDO0lBQ2pDLENBQUM7Ozs7OztJQUVELE1BQU0sQ0FBQyxHQUFvQixFQUFFLEdBQVc7O2NBQ2hDLGVBQWUsR0FBRyxJQUFJLENBQUMsa0JBQWtCLENBQUMsR0FBRyxFQUFFLEdBQUcsQ0FBQzs7Y0FDbkQsU0FBUyxHQUFHLElBQUksQ0FBQyxHQUFHLENBQUMsSUFBSSxDQUFDLGVBQWUsQ0FBQyxHQUFHLEVBQUUsR0FBRyxDQUFDLENBQUM7O1lBQ3RELE1BQU07UUFDVixJQUFJLE9BQU8sR0FBRyxLQUFLLFFBQVEsRUFBRTtZQUMzQixNQUFNLEdBQUcsQ0FBQyxDQUFDLGVBQWUsR0FBRyxHQUFHLEdBQUcsZUFBZSxHQUFHLElBQUksQ0FBQyxNQUFNLEdBQUcsR0FBRyxDQUFDLEdBQUcsZUFBZSxDQUFDLENBQUMsT0FBTyxDQUFDLFNBQVMsQ0FBQyxDQUFDO1NBQy9HO2FBQU07WUFDTCxNQUFNLEdBQUcsSUFBSSxDQUFDLEtBQUssS0FBSyxDQUFDLFFBQVEsQ0FBQyxDQUFDLENBQUMsSUFBSSxDQUFDLE1BQU0sQ0FBQyxDQUFDLENBQUMsSUFBSSxDQUFDLEtBQUssQ0FBQztTQUM5RDtRQUNELE9BQU8sSUFBSSxDQUFDLFFBQVEsQ0FBQyxNQUFNLENBQUMsQ0FBQztJQUMvQixDQUFDOzs7Ozs7SUFFRCxRQUFRLENBQUMsR0FBb0IsRUFBRSxHQUFXOztjQUNsQyxlQUFlLEdBQUcsSUFBSSxDQUFDLGtCQUFrQixDQUFDLEdBQUcsRUFBRSxHQUFHLENBQUM7O2NBQ25ELFNBQVMsR0FBRyxJQUFJLENBQUMsR0FBRyxDQUFDLElBQUksQ0FBQyxlQUFlLENBQUMsR0FBRyxFQUFFLEdBQUcsQ0FBQyxDQUFDOztZQUN0RCxNQUFNO1FBQ1YsSUFBSSxPQUFPLEdBQUcsS0FBSyxRQUFRLEVBQUU7WUFDM0IsTUFBTSxHQUFHLENBQUMsQ0FBQyxlQUFlLEdBQUcsR0FBRyxHQUFHLGVBQWUsR0FBRyxJQUFJLENBQUMsTUFBTSxHQUFHLEdBQUcsQ0FBQyxHQUFHLGVBQWUsQ0FBQyxDQUFDLE9BQU8sQ0FBQyxTQUFTLENBQUMsQ0FBQztTQUMvRzthQUFNO1lBQ0wsTUFBTSxHQUFHLElBQUksQ0FBQyxLQUFLLEtBQUssQ0FBQyxRQUFRLENBQUMsQ0FBQyxDQUFDLENBQUMsSUFBSSxDQUFDLE1BQU0sQ0FBQyxDQUFDLENBQUMsSUFBSSxDQUFDLEtBQUssQ0FBQztTQUMvRDtRQUNELE9BQU8sSUFBSSxDQUFDLFFBQVEsQ0FBQyxNQUFNLENBQUMsQ0FBQztJQUMvQixDQUFDOzs7Ozs7O0lBRUQsSUFBSSxDQUFDLElBQVksRUFBRSxDQUE2QixFQUFFLFFBQWdCLENBQUM7UUFDakUsSUFBSSxDQUFDLElBQUksRUFBRSxDQUFDO1FBQ1osQ0FBQyxDQUFDLGNBQWMsRUFBRSxDQUFDO1FBQ25CLElBQUksSUFBSSxDQUFDLFVBQVUsRUFBRTtZQUNuQixPQUFPO1NBQ1I7O2NBQ0ssS0FBSyxHQUFHLElBQUksQ0FBQyxvQkFBb0IsQ0FBQyxJQUFJLENBQUMsV0FBVyxDQUFDLElBQUksQ0FBQzs7WUFDMUQsR0FBRyxHQUFHLENBQUM7UUFDWCxJQUFJLElBQUksS0FBSyxJQUFJLEVBQUU7WUFDakIsR0FBRyxHQUFHLElBQUksQ0FBQyxNQUFNLENBQUMsS0FBSyxFQUFFLEtBQUssQ0FBQyxDQUFDO1NBQ2pDO2FBQU0sSUFBSSxJQUFJLEtBQUssTUFBTSxFQUFFO1lBQzFCLEdBQUcsR0FBRyxJQUFJLENBQUMsUUFBUSxDQUFDLEtBQUssRUFBRSxLQUFLLENBQUMsQ0FBQztTQUNuQzs7Y0FDSyxVQUFVLEdBQUcsR0FBRyxHQUFHLElBQUksQ0FBQyxLQUFLLElBQUksR0FBRyxHQUFHLElBQUksQ0FBQyxLQUFLO1FBQ3ZELElBQUksR0FBRyxHQUFHLElBQUksQ0FBQyxLQUFLLEVBQUU7WUFDcEIsR0FBRyxHQUFHLElBQUksQ0FBQyxLQUFLLENBQUM7U0FDbEI7YUFBTSxJQUFJLEdBQUcsR0FBRyxJQUFJLENBQUMsS0FBSyxFQUFFO1lBQzNCLEdBQUcsR0FBRyxJQUFJLENBQUMsS0FBSyxDQUFDO1NBQ2xCO1FBQ0QsSUFBSSxDQUFDLFFBQVEsQ0FBQyxHQUFHLEVBQUUsSUFBSSxDQUFDLENBQUM7UUFDekIsSUFBSSxDQUFDLFNBQVMsR0FBRyxJQUFJLENBQUM7UUFDdEIsSUFBSSxVQUFVLEVBQUU7WUFDZCxPQUFPO1NBQ1I7UUFDRCxJQUFJLENBQUMsYUFBYSxHQUFHLFVBQVU7OztRQUFDLEdBQUcsRUFBRTtZQUNuQyxJQUFJLENBQUMsSUFBSSxDQUFDLENBQUMsQ0FBQyxFQUFFLEtBQUssRUFBRSxJQUFJLENBQUMsQ0FBQztRQUM3QixDQUFDLEdBQUUsR0FBRyxDQUFDLENBQUM7SUFDVixDQUFDOzs7O0lBRUQsSUFBSTtRQUNGLElBQUksSUFBSSxDQUFDLGFBQWEsRUFBRTtZQUN0QixZQUFZLENBQUMsSUFBSSxDQUFDLGFBQWEsQ0FBQyxDQUFDO1NBQ2xDO0lBQ0gsQ0FBQzs7Ozs7O0lBRUQsUUFBUSxDQUFDLEtBQWEsRUFBRSxJQUFhO1FBQ25DLElBQUksSUFBSSxJQUFJLEdBQUcsSUFBSSxDQUFDLEtBQUssRUFBRSxLQUFLLEdBQUcsS0FBSyxFQUFFLEVBQUU7WUFDMUMsSUFBSSxDQUFDLFFBQVEsQ0FBQyxLQUFLLENBQUMsQ0FBQztTQUN0QjtRQUNELElBQUksQ0FBQyxLQUFLLEdBQUcsS0FBSyxDQUFDO1FBQ25CLElBQUksQ0FBQyxXQUFXLEdBQUcsS0FBSyxDQUFDOztjQUNuQixZQUFZLEdBQUcsUUFBUSxDQUFDLElBQUksQ0FBQyxXQUFXLENBQUMsSUFBSSxDQUFDLEtBQUssQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDLElBQUksQ0FBQyxXQUFXLENBQUMsSUFBSSxDQUFDLEtBQUssQ0FBQyxDQUFDLENBQUMsQ0FBQyxFQUFFO1FBQy9GLElBQUksQ0FBQyxZQUFZLEdBQUcsWUFBWSxDQUFDO1FBQ2pDLElBQUksQ0FBQyxZQUFZLENBQUMsYUFBYSxDQUFDLEtBQUssR0FBRyxZQUFZLENBQUM7UUFDckQsSUFBSSxDQUFDLFVBQVUsR0FBRyxJQUFJLENBQUMsWUFBWSxHQUFHLEtBQUssQ0FBQztRQUM1QyxJQUFJLEtBQUssSUFBSSxLQUFLLEtBQUssQ0FBQyxFQUFFOztrQkFDbEIsR0FBRyxHQUFHLE1BQU0sQ0FBQyxLQUFLLENBQUM7WUFDekIsSUFBSSxHQUFHLElBQUksSUFBSSxDQUFDLEtBQUssRUFBRTtnQkFDckIsSUFBSSxDQUFDLFVBQVUsR0FBRyxJQUFJLENBQUM7YUFDeEI7WUFDRCxJQUFJLEdBQUcsSUFBSSxJQUFJLENBQUMsS0FBSyxFQUFFO2dCQUNyQixJQUFJLENBQUMsWUFBWSxHQUFHLElBQUksQ0FBQzthQUMxQjtTQUNGO0lBQ0gsQ0FBQzs7Ozs7SUFFRCxTQUFTLENBQUMsQ0FBZ0I7UUFDeEIsSUFBSSxDQUFDLENBQUMsSUFBSSxLQUFLLFNBQVMsSUFBSSxDQUFDLENBQUMsT0FBTyxLQUFLLFFBQVEsRUFBRTs7a0JBQzVDLEtBQUssR0FBRyxJQUFJLENBQUMsUUFBUSxDQUFDLENBQUMsQ0FBQztZQUM5QixJQUFJLENBQUMsRUFBRSxDQUFDLENBQUMsRUFBRSxLQUFLLENBQUMsQ0FBQztZQUNsQixJQUFJLENBQUMsSUFBSSxFQUFFLENBQUM7U0FDYjthQUFNLElBQUksQ0FBQyxDQUFDLElBQUksS0FBSyxXQUFXLElBQUksQ0FBQyxDQUFDLE9BQU8sS0FBSyxVQUFVLEVBQUU7O2tCQUN2RCxLQUFLLEdBQUcsSUFBSSxDQUFDLFFBQVEsQ0FBQyxDQUFDLENBQUM7WUFDOUIsSUFBSSxDQUFDLElBQUksQ0FBQyxDQUFDLEVBQUUsS0FBSyxDQUFDLENBQUM7WUFDcEIsSUFBSSxDQUFDLElBQUksRUFBRSxDQUFDO1NBQ2I7YUFBTSxJQUFJLENBQUMsQ0FBQyxPQUFPLEtBQUssS0FBSyxFQUFFO1lBQzlCLElBQUksQ0FBQyxnQkFBZ0IsRUFBRSxDQUFDO1NBQ3pCO0lBQ0gsQ0FBQzs7OztJQUVELE9BQU87UUFDTCxJQUFJLENBQUMsSUFBSSxFQUFFLENBQUM7SUFDZCxDQUFDOzs7OztJQUVELFVBQVUsQ0FBQyxLQUFhO1FBQ3RCLElBQUksQ0FBQyxRQUFRLENBQUMsS0FBSyxFQUFFLEtBQUssQ0FBQyxDQUFDO1FBQzVCLElBQUksQ0FBQyxHQUFHLENBQUMsWUFBWSxFQUFFLENBQUM7SUFDMUIsQ0FBQzs7Ozs7SUFFRCxnQkFBZ0IsQ0FBQyxFQUF1QjtRQUN0QyxJQUFJLENBQUMsUUFBUSxHQUFHLEVBQUUsQ0FBQztJQUNyQixDQUFDOzs7OztJQUVELGlCQUFpQixDQUFDLEVBQWM7UUFDOUIsSUFBSSxDQUFDLFNBQVMsR0FBRyxFQUFFLENBQUM7SUFDdEIsQ0FBQzs7Ozs7SUFFRCxnQkFBZ0IsQ0FBQyxVQUFtQjtRQUNsQyxJQUFJLENBQUMsVUFBVSxHQUFHLFVBQVUsQ0FBQztRQUM3QixJQUFJLENBQUMsR0FBRyxDQUFDLFlBQVksRUFBRSxDQUFDO0lBQzFCLENBQUM7Ozs7SUFFRCxLQUFLO1FBQ0gsSUFBSSxDQUFDLFlBQVksQ0FBQyxRQUFRLENBQUMsSUFBSSxDQUFDLFlBQVksRUFBRSxVQUFVLENBQUMsQ0FBQztJQUM1RCxDQUFDOzs7O0lBRUQsSUFBSTtRQUNGLElBQUksQ0FBQyxZQUFZLENBQUMsYUFBYSxDQUFDLElBQUksRUFBRSxDQUFDO0lBQ3pDLENBQUM7Ozs7SUFXRCxRQUFRO1FBQ04sSUFBSSxDQUFDLFlBQVksQ0FBQyxPQUFPLENBQUMsSUFBSSxDQUFDLFVBQVUsRUFBRSxJQUFJLENBQUMsQ0FBQyxTQUFTOzs7O1FBQUMsV0FBVyxDQUFDLEVBQUU7WUFDdkUsSUFBSSxDQUFDLFdBQVcsRUFBRTtnQkFDaEIsSUFBSSxDQUFDLE1BQU0sQ0FBQyxJQUFJLEVBQUUsQ0FBQztnQkFDbkIsT0FBTyxDQUFDLE9BQU8sRUFBRSxDQUFDLElBQUk7OztnQkFBQyxHQUFHLEVBQUUsQ0FBQyxJQUFJLENBQUMsU0FBUyxFQUFFLEVBQUMsQ0FBQzthQUNoRDtpQkFBTTtnQkFDTCxJQUFJLENBQUMsT0FBTyxDQUFDLElBQUksRUFBRSxDQUFDO2FBQ3JCO1FBQ0gsQ0FBQyxFQUFDLENBQUM7SUFDTCxDQUFDOzs7OztJQUVELFdBQVcsQ0FBQyxPQUFzQjtRQUNoQyxJQUFJLE9BQU8sQ0FBQyxXQUFXLEVBQUU7WUFDdkIsSUFBSSxDQUFDLGVBQWUsRUFBRSxDQUFDO1NBQ3hCO1FBQ0QsSUFBSSxPQUFPLENBQUMsV0FBVyxFQUFFOztrQkFDakIsS0FBSyxHQUFHLElBQUksQ0FBQyxvQkFBb0IsQ0FBQyxJQUFJLENBQUMsV0FBVyxDQUFDO1lBQ3pELElBQUksQ0FBQyxRQUFRLENBQUMsS0FBSyxFQUFFLElBQUksQ0FBQyxDQUFDO1NBQzVCO0lBQ0gsQ0FBQzs7OztJQUVELGVBQWU7UUFDYixJQUFJLElBQUksQ0FBQyxXQUFXLEVBQUU7WUFDcEIsSUFBSSxDQUFDLEtBQUssRUFBRSxDQUFDO1NBQ2Q7SUFDSCxDQUFDOzs7O0lBRUQsV0FBVztRQUNULElBQUksQ0FBQyxZQUFZLENBQUMsY0FBYyxDQUFDLElBQUksQ0FBQyxVQUFVLENBQUMsQ0FBQztJQUNwRCxDQUFDOzs7WUF6VkYsU0FBUyxTQUFDO2dCQUNULFFBQVEsRUFBRSxpQkFBaUI7Z0JBQzNCLDB1Q0FBK0M7Z0JBQy9DLFNBQVMsRUFBRTtvQkFDVDt3QkFDRSxPQUFPLEVBQUUsaUJBQWlCO3dCQUMxQixXQUFXLEVBQUUsVUFBVTs7O3dCQUFDLEdBQUcsRUFBRSxDQUFDLHNCQUFzQixFQUFDO3dCQUNyRCxLQUFLLEVBQUUsSUFBSTtxQkFDWjtpQkFDRjtnQkFDRCxlQUFlLEVBQUUsdUJBQXVCLENBQUMsTUFBTTtnQkFDL0MsYUFBYSxFQUFFLGlCQUFpQixDQUFDLElBQUk7Z0JBQ3JDLElBQUksRUFBRTtvQkFDSixrQ0FBa0MsRUFBRSxXQUFXO29CQUMvQyw2QkFBNkIsRUFBRSxvQkFBb0I7b0JBQ25ELDZCQUE2QixFQUFFLG9CQUFvQjtvQkFDbkQsbUNBQW1DLEVBQUUsWUFBWTtpQkFDbEQ7YUFDRjs7OztZQXBDQyxVQUFVO1lBT1YsU0FBUztZQVRULGlCQUFpQjtZQU5WLFlBQVk7OztxQkF1RGxCLE1BQU07c0JBQ04sTUFBTTsyQkFDTixTQUFTLFNBQUMsY0FBYztxQkFDeEIsS0FBSztvQkFDTCxLQUFLO29CQUNMLEtBQUs7dUJBQ0wsS0FBSzswQkFDTCxLQUFLOzRCQUNMLEtBQUs7cUJBQ0wsS0FBSzt5QkFDTCxLQUFLOzBCQUNMLEtBQUs7MEJBQ0wsS0FBSzs7QUFGbUI7SUFBZixZQUFZLEVBQUU7OzBEQUFvQjtBQUNuQjtJQUFmLFlBQVksRUFBRTs7MkRBQXFCOzs7Ozs7SUFwQjdDLCtDQUE4Qjs7Ozs7SUFDOUIsNkNBQXFDOzs7OztJQUNyQyx1Q0FBK0I7O0lBQy9CLDhDQUE4Qjs7SUFDOUIsMkNBQWtCOztJQUNsQiw0Q0FBbUI7O0lBQ25CLDhDQUFxQjs7SUFDckIsMENBQStDOztJQUMvQywyQ0FBbUM7O0lBQ25DLHdDQUErQzs7SUFDL0MseUNBQWdEOztJQUNoRCw4Q0FBb0Q7O0lBQ3BELHdDQUEyQzs7SUFDM0MsdUNBQW1DOztJQUNuQyx1Q0FBa0M7O0lBQ2xDLDBDQUEwQzs7SUFDMUMsNkNBQTZCOztJQUM3QiwrQ0FBNEI7O0lBQzVCLHdDQUFvQjs7SUFDcEIsNENBQTRDOztJQUM1Qyw2Q0FBNkM7O0lBQzdDLDZDQUEwRTs7Ozs7SUEyUXhFLDRDQUE4Qjs7Ozs7SUFDOUIsMENBQTJCOzs7OztJQUMzQixxQ0FBOEI7Ozs7O0lBQzlCLDhDQUFrQyIsInNvdXJjZXNDb250ZW50IjpbImltcG9ydCB7IEZvY3VzTW9uaXRvciB9IGZyb20gJ0Bhbmd1bGFyL2Nkay9hMTF5JztcbmltcG9ydCB7IERPV05fQVJST1csIEVOVEVSLCBVUF9BUlJPVyB9IGZyb20gJ0Bhbmd1bGFyL2Nkay9rZXljb2Rlcyc7XG5pbXBvcnQge1xuICBmb3J3YXJkUmVmLFxuICBBZnRlclZpZXdJbml0LFxuICBDaGFuZ2VEZXRlY3Rpb25TdHJhdGVneSxcbiAgQ2hhbmdlRGV0ZWN0b3JSZWYsXG4gIENvbXBvbmVudCxcbiAgRWxlbWVudFJlZixcbiAgRXZlbnRFbWl0dGVyLFxuICBJbnB1dCxcbiAgT25DaGFuZ2VzLFxuICBPbkRlc3Ryb3ksXG4gIE9uSW5pdCxcbiAgT3V0cHV0LFxuICBSZW5kZXJlcjIsXG4gIFNpbXBsZUNoYW5nZXMsXG4gIFZpZXdDaGlsZCxcbiAgVmlld0VuY2Fwc3VsYXRpb25cbn0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5pbXBvcnQgeyBDb250cm9sVmFsdWVBY2Nlc3NvciwgTkdfVkFMVUVfQUNDRVNTT1IgfSBmcm9tICdAYW5ndWxhci9mb3Jtcyc7XG5pbXBvcnQgeyBOelNpemVMRFNUeXBlIH0gZnJvbSAnLi4vY29yZS90eXBlcy9zaXplJztcblxuaW1wb3J0IHsgaXNOb3ROaWwgfSBmcm9tICcuLi9jb3JlL3V0aWwvY2hlY2snO1xuaW1wb3J0IHsgSW5wdXRCb29sZWFuIH0gZnJvbSAnLi4vY29yZS91dGlsL2NvbnZlcnQnO1xuXG5AQ29tcG9uZW50KHtcbiAgc2VsZWN0b3I6ICduei1pbnB1dC1udW1iZXInLFxuICB0ZW1wbGF0ZVVybDogJy4vbnotaW5wdXQtbnVtYmVyLmNvbXBvbmVudC5odG1sJyxcbiAgcHJvdmlkZXJzOiBbXG4gICAge1xuICAgICAgcHJvdmlkZTogTkdfVkFMVUVfQUNDRVNTT1IsXG4gICAgICB1c2VFeGlzdGluZzogZm9yd2FyZFJlZigoKSA9PiBOeklucHV0TnVtYmVyQ29tcG9uZW50KSxcbiAgICAgIG11bHRpOiB0cnVlXG4gICAgfVxuICBdLFxuICBjaGFuZ2VEZXRlY3Rpb246IENoYW5nZURldGVjdGlvblN0cmF0ZWd5Lk9uUHVzaCxcbiAgZW5jYXBzdWxhdGlvbjogVmlld0VuY2Fwc3VsYXRpb24uTm9uZSxcbiAgaG9zdDoge1xuICAgICdbY2xhc3MuYW50LWlucHV0LW51bWJlci1mb2N1c2VkXSc6ICdpc0ZvY3VzZWQnLFxuICAgICdbY2xhc3MuYW50LWlucHV0LW51bWJlci1sZ10nOiBgbnpTaXplID09PSAnbGFyZ2UnYCxcbiAgICAnW2NsYXNzLmFudC1pbnB1dC1udW1iZXItc21dJzogYG56U2l6ZSA9PT0gJ3NtYWxsJ2AsXG4gICAgJ1tjbGFzcy5hbnQtaW5wdXQtbnVtYmVyLWRpc2FibGVkXSc6ICduekRpc2FibGVkJ1xuICB9XG59KVxuZXhwb3J0IGNsYXNzIE56SW5wdXROdW1iZXJDb21wb25lbnQgaW1wbGVtZW50cyBDb250cm9sVmFsdWVBY2Nlc3NvciwgQWZ0ZXJWaWV3SW5pdCwgT25DaGFuZ2VzLCBPbkluaXQsIE9uRGVzdHJveSB7XG4gIHByaXZhdGUgYXV0b1N0ZXBUaW1lcjogbnVtYmVyO1xuICBwcml2YXRlIGFjdHVhbFZhbHVlOiBzdHJpbmcgfCBudW1iZXI7XG4gIHByaXZhdGUgdmFsdWU6IHN0cmluZyB8IG51bWJlcjtcbiAgZGlzcGxheVZhbHVlOiBzdHJpbmcgfCBudW1iZXI7XG4gIGlzRm9jdXNlZCA9IGZhbHNlO1xuICBkaXNhYmxlZFVwID0gZmFsc2U7XG4gIGRpc2FibGVkRG93biA9IGZhbHNlO1xuICBvbkNoYW5nZTogKHZhbHVlOiBudW1iZXIpID0+IHZvaWQgPSAoKSA9PiBudWxsO1xuICBvblRvdWNoZWQ6ICgpID0+IHZvaWQgPSAoKSA9PiBudWxsO1xuICBAT3V0cHV0KCkgcmVhZG9ubHkgbnpCbHVyID0gbmV3IEV2ZW50RW1pdHRlcigpO1xuICBAT3V0cHV0KCkgcmVhZG9ubHkgbnpGb2N1cyA9IG5ldyBFdmVudEVtaXR0ZXIoKTtcbiAgQFZpZXdDaGlsZCgnaW5wdXRFbGVtZW50JykgaW5wdXRFbGVtZW50OiBFbGVtZW50UmVmO1xuICBASW5wdXQoKSBuelNpemU6IE56U2l6ZUxEU1R5cGUgPSAnZGVmYXVsdCc7XG4gIEBJbnB1dCgpIG56TWluOiBudW1iZXIgPSAtSW5maW5pdHk7XG4gIEBJbnB1dCgpIG56TWF4OiBudW1iZXIgPSBJbmZpbml0eTtcbiAgQElucHV0KCkgbnpQYXJzZXIgPSAodmFsdWU6IGFueSkgPT4gdmFsdWU7IC8vIHRzbGludDpkaXNhYmxlLWxpbmU6bm8tYW55XG4gIEBJbnB1dCgpIG56UHJlY2lzaW9uOiBudW1iZXI7XG4gIEBJbnB1dCgpIG56UGxhY2VIb2xkZXIgPSAnJztcbiAgQElucHV0KCkgbnpTdGVwID0gMTtcbiAgQElucHV0KCkgQElucHV0Qm9vbGVhbigpIG56RGlzYWJsZWQgPSBmYWxzZTtcbiAgQElucHV0KCkgQElucHV0Qm9vbGVhbigpIG56QXV0b0ZvY3VzID0gZmFsc2U7XG4gIEBJbnB1dCgpIG56Rm9ybWF0dGVyOiAodmFsdWU6IG51bWJlcikgPT4gc3RyaW5nIHwgbnVtYmVyID0gdmFsdWUgPT4gdmFsdWU7XG5cbiAgW3Byb3BlcnR5OiBzdHJpbmddOiBhbnk7IC8vIHRzbGludDpkaXNhYmxlLWxpbmU6bm8tYW55XG5cbiAgdXBkYXRlQXV0b0ZvY3VzKCk6IHZvaWQge1xuICAgIGlmICh0aGlzLm56QXV0b0ZvY3VzKSB7XG4gICAgICB0aGlzLnJlbmRlcmVyLnNldEF0dHJpYnV0ZSh0aGlzLmlucHV0RWxlbWVudC5uYXRpdmVFbGVtZW50LCAnYXV0b2ZvY3VzJywgJ2F1dG9mb2N1cycpO1xuICAgIH0gZWxzZSB7XG4gICAgICB0aGlzLnJlbmRlcmVyLnJlbW92ZUF0dHJpYnV0ZSh0aGlzLmlucHV0RWxlbWVudC5uYXRpdmVFbGVtZW50LCAnYXV0b2ZvY3VzJyk7XG4gICAgfVxuICB9XG5cbiAgb25Nb2RlbENoYW5nZSh2YWx1ZTogc3RyaW5nKTogdm9pZCB7XG4gICAgdGhpcy5hY3R1YWxWYWx1ZSA9IHRoaXMubnpQYXJzZXIoXG4gICAgICB2YWx1ZVxuICAgICAgICAudHJpbSgpXG4gICAgICAgIC5yZXBsYWNlKC/jgIIvZywgJy4nKVxuICAgICAgICAucmVwbGFjZSgvW15cXHdcXC4tXSsvZywgJycpXG4gICAgKTtcbiAgICB0aGlzLmlucHV0RWxlbWVudC5uYXRpdmVFbGVtZW50LnZhbHVlID0gdGhpcy5hY3R1YWxWYWx1ZTtcbiAgfVxuXG4gIGdldEN1cnJlbnRWYWxpZFZhbHVlKHZhbHVlOiBzdHJpbmcgfCBudW1iZXIpOiBudW1iZXIge1xuICAgIGxldCB2YWwgPSB2YWx1ZTtcbiAgICBpZiAodmFsID09PSAnJykge1xuICAgICAgdmFsID0gJyc7XG4gICAgfSBlbHNlIGlmICghdGhpcy5pc05vdENvbXBsZXRlTnVtYmVyKHZhbCkpIHtcbiAgICAgIHZhbCA9IHRoaXMuZ2V0VmFsaWRWYWx1ZSh2YWwpIGFzIHN0cmluZztcbiAgICB9IGVsc2Uge1xuICAgICAgdmFsID0gdGhpcy52YWx1ZTtcbiAgICB9XG4gICAgcmV0dXJuIHRoaXMudG9OdW1iZXIodmFsKTtcbiAgfVxuXG4gIC8vICcxLicgJzF4JyAneHgnICcnID0+IGFyZSBub3QgY29tcGxldGUgbnVtYmVyc1xuICBpc05vdENvbXBsZXRlTnVtYmVyKG51bTogc3RyaW5nIHwgbnVtYmVyKTogYm9vbGVhbiB7XG4gICAgcmV0dXJuIChcbiAgICAgIGlzTmFOKG51bSBhcyBudW1iZXIpIHx8XG4gICAgICBudW0gPT09ICcnIHx8XG4gICAgICBudW0gPT09IG51bGwgfHxcbiAgICAgICEhKG51bSAmJiBudW0udG9TdHJpbmcoKS5pbmRleE9mKCcuJykgPT09IG51bS50b1N0cmluZygpLmxlbmd0aCAtIDEpXG4gICAgKTtcbiAgfVxuXG4gIGdldFZhbGlkVmFsdWUodmFsdWU/OiBzdHJpbmcgfCBudW1iZXIpOiBzdHJpbmcgfCBudW1iZXIgfCB1bmRlZmluZWQge1xuICAgIGxldCB2YWwgPSBwYXJzZUZsb2F0KHZhbHVlIGFzIHN0cmluZyk7XG4gICAgLy8gaHR0cHM6Ly9naXRodWIuY29tL2FudC1kZXNpZ24vYW50LWRlc2lnbi9pc3N1ZXMvNzM1OFxuICAgIGlmIChpc05hTih2YWwpKSB7XG4gICAgICByZXR1cm4gdmFsdWU7XG4gICAgfVxuICAgIGlmICh2YWwgPCB0aGlzLm56TWluKSB7XG4gICAgICB2YWwgPSB0aGlzLm56TWluO1xuICAgIH1cbiAgICBpZiAodmFsID4gdGhpcy5uek1heCkge1xuICAgICAgdmFsID0gdGhpcy5uek1heDtcbiAgICB9XG4gICAgcmV0dXJuIHZhbDtcbiAgfVxuXG4gIHRvTnVtYmVyKG51bTogc3RyaW5nIHwgbnVtYmVyKTogbnVtYmVyIHtcbiAgICBpZiAodGhpcy5pc05vdENvbXBsZXRlTnVtYmVyKG51bSkpIHtcbiAgICAgIHJldHVybiBudW0gYXMgbnVtYmVyO1xuICAgIH1cbiAgICBpZiAoaXNOb3ROaWwodGhpcy5uelByZWNpc2lvbikpIHtcbiAgICAgIHJldHVybiBOdW1iZXIoTnVtYmVyKG51bSkudG9GaXhlZCh0aGlzLm56UHJlY2lzaW9uKSk7XG4gICAgfVxuICAgIHJldHVybiBOdW1iZXIobnVtKTtcbiAgfVxuXG4gIHNldFZhbGlkYXRlVmFsdWUoKTogdm9pZCB7XG4gICAgY29uc3QgdmFsdWUgPSB0aGlzLmdldEN1cnJlbnRWYWxpZFZhbHVlKHRoaXMuYWN0dWFsVmFsdWUpO1xuICAgIHRoaXMuc2V0VmFsdWUodmFsdWUsIGAke3RoaXMudmFsdWV9YCAhPT0gYCR7dmFsdWV9YCk7XG4gIH1cblxuICBvbkJsdXIoKTogdm9pZCB7XG4gICAgdGhpcy5pc0ZvY3VzZWQgPSBmYWxzZTtcbiAgICB0aGlzLnNldFZhbGlkYXRlVmFsdWUoKTtcbiAgfVxuXG4gIG9uRm9jdXMoKTogdm9pZCB7XG4gICAgdGhpcy5pc0ZvY3VzZWQgPSB0cnVlO1xuICB9XG5cbiAgZ2V0UmF0aW8oZTogS2V5Ym9hcmRFdmVudCk6IG51bWJlciB7XG4gICAgbGV0IHJhdGlvID0gMTtcbiAgICBpZiAoZS5tZXRhS2V5IHx8IGUuY3RybEtleSkge1xuICAgICAgcmF0aW8gPSAwLjE7XG4gICAgfSBlbHNlIGlmIChlLnNoaWZ0S2V5KSB7XG4gICAgICByYXRpbyA9IDEwO1xuICAgIH1cbiAgICByZXR1cm4gcmF0aW87XG4gIH1cblxuICBkb3duKGU6IE1vdXNlRXZlbnQgfCBLZXlib2FyZEV2ZW50LCByYXRpbz86IG51bWJlcik6IHZvaWQge1xuICAgIGlmICghdGhpcy5pc0ZvY3VzZWQpIHtcbiAgICAgIHRoaXMuZm9jdXMoKTtcbiAgICB9XG4gICAgdGhpcy5zdGVwKCdkb3duJywgZSwgcmF0aW8pO1xuICB9XG5cbiAgdXAoZTogTW91c2VFdmVudCB8IEtleWJvYXJkRXZlbnQsIHJhdGlvPzogbnVtYmVyKTogdm9pZCB7XG4gICAgaWYgKCF0aGlzLmlzRm9jdXNlZCkge1xuICAgICAgdGhpcy5mb2N1cygpO1xuICAgIH1cbiAgICB0aGlzLnN0ZXAoJ3VwJywgZSwgcmF0aW8pO1xuICB9XG5cbiAgZ2V0UHJlY2lzaW9uKHZhbHVlOiBudW1iZXIpOiBudW1iZXIge1xuICAgIGNvbnN0IHZhbHVlU3RyaW5nID0gdmFsdWUudG9TdHJpbmcoKTtcbiAgICBpZiAodmFsdWVTdHJpbmcuaW5kZXhPZignZS0nKSA+PSAwKSB7XG4gICAgICByZXR1cm4gcGFyc2VJbnQodmFsdWVTdHJpbmcuc2xpY2UodmFsdWVTdHJpbmcuaW5kZXhPZignZS0nKSArIDIpLCAxMCk7XG4gICAgfVxuICAgIGxldCBwcmVjaXNpb24gPSAwO1xuICAgIGlmICh2YWx1ZVN0cmluZy5pbmRleE9mKCcuJykgPj0gMCkge1xuICAgICAgcHJlY2lzaW9uID0gdmFsdWVTdHJpbmcubGVuZ3RoIC0gdmFsdWVTdHJpbmcuaW5kZXhPZignLicpIC0gMTtcbiAgICB9XG4gICAgcmV0dXJuIHByZWNpc2lvbjtcbiAgfVxuXG4gIC8vIHN0ZXA9ezEuMH0gdmFsdWU9ezEuNTF9XG4gIC8vIHByZXNzICtcbiAgLy8gdGhlbiB2YWx1ZSBzaG91bGQgYmUgMi41MSwgcmF0aGVyIHRoYW4gMi41XG4gIC8vIGlmIHRoaXMucHJvcHMucHJlY2lzaW9uIGlzIHVuZGVmaW5lZFxuICAvLyBodHRwczovL2dpdGh1Yi5jb20vcmVhY3QtY29tcG9uZW50L2lucHV0LW51bWJlci9pc3N1ZXMvMzlcbiAgZ2V0TWF4UHJlY2lzaW9uKGN1cnJlbnRWYWx1ZTogc3RyaW5nIHwgbnVtYmVyLCByYXRpbzogbnVtYmVyKTogbnVtYmVyIHtcbiAgICBpZiAoaXNOb3ROaWwodGhpcy5uelByZWNpc2lvbikpIHtcbiAgICAgIHJldHVybiB0aGlzLm56UHJlY2lzaW9uO1xuICAgIH1cbiAgICBjb25zdCByYXRpb1ByZWNpc2lvbiA9IHRoaXMuZ2V0UHJlY2lzaW9uKHJhdGlvKTtcbiAgICBjb25zdCBzdGVwUHJlY2lzaW9uID0gdGhpcy5nZXRQcmVjaXNpb24odGhpcy5uelN0ZXApO1xuICAgIGNvbnN0IGN1cnJlbnRWYWx1ZVByZWNpc2lvbiA9IHRoaXMuZ2V0UHJlY2lzaW9uKGN1cnJlbnRWYWx1ZSBhcyBudW1iZXIpO1xuICAgIGlmICghY3VycmVudFZhbHVlKSB7XG4gICAgICByZXR1cm4gcmF0aW9QcmVjaXNpb24gKyBzdGVwUHJlY2lzaW9uO1xuICAgIH1cbiAgICByZXR1cm4gTWF0aC5tYXgoY3VycmVudFZhbHVlUHJlY2lzaW9uLCByYXRpb1ByZWNpc2lvbiArIHN0ZXBQcmVjaXNpb24pO1xuICB9XG5cbiAgZ2V0UHJlY2lzaW9uRmFjdG9yKGN1cnJlbnRWYWx1ZTogc3RyaW5nIHwgbnVtYmVyLCByYXRpbzogbnVtYmVyKTogbnVtYmVyIHtcbiAgICBjb25zdCBwcmVjaXNpb24gPSB0aGlzLmdldE1heFByZWNpc2lvbihjdXJyZW50VmFsdWUsIHJhdGlvKTtcbiAgICByZXR1cm4gTWF0aC5wb3coMTAsIHByZWNpc2lvbik7XG4gIH1cblxuICB1cFN0ZXAodmFsOiBzdHJpbmcgfCBudW1iZXIsIHJhdDogbnVtYmVyKTogbnVtYmVyIHtcbiAgICBjb25zdCBwcmVjaXNpb25GYWN0b3IgPSB0aGlzLmdldFByZWNpc2lvbkZhY3Rvcih2YWwsIHJhdCk7XG4gICAgY29uc3QgcHJlY2lzaW9uID0gTWF0aC5hYnModGhpcy5nZXRNYXhQcmVjaXNpb24odmFsLCByYXQpKTtcbiAgICBsZXQgcmVzdWx0O1xuICAgIGlmICh0eXBlb2YgdmFsID09PSAnbnVtYmVyJykge1xuICAgICAgcmVzdWx0ID0gKChwcmVjaXNpb25GYWN0b3IgKiB2YWwgKyBwcmVjaXNpb25GYWN0b3IgKiB0aGlzLm56U3RlcCAqIHJhdCkgLyBwcmVjaXNpb25GYWN0b3IpLnRvRml4ZWQocHJlY2lzaW9uKTtcbiAgICB9IGVsc2Uge1xuICAgICAgcmVzdWx0ID0gdGhpcy5uek1pbiA9PT0gLUluZmluaXR5ID8gdGhpcy5uelN0ZXAgOiB0aGlzLm56TWluO1xuICAgIH1cbiAgICByZXR1cm4gdGhpcy50b051bWJlcihyZXN1bHQpO1xuICB9XG5cbiAgZG93blN0ZXAodmFsOiBzdHJpbmcgfCBudW1iZXIsIHJhdDogbnVtYmVyKTogbnVtYmVyIHtcbiAgICBjb25zdCBwcmVjaXNpb25GYWN0b3IgPSB0aGlzLmdldFByZWNpc2lvbkZhY3Rvcih2YWwsIHJhdCk7XG4gICAgY29uc3QgcHJlY2lzaW9uID0gTWF0aC5hYnModGhpcy5nZXRNYXhQcmVjaXNpb24odmFsLCByYXQpKTtcbiAgICBsZXQgcmVzdWx0O1xuICAgIGlmICh0eXBlb2YgdmFsID09PSAnbnVtYmVyJykge1xuICAgICAgcmVzdWx0ID0gKChwcmVjaXNpb25GYWN0b3IgKiB2YWwgLSBwcmVjaXNpb25GYWN0b3IgKiB0aGlzLm56U3RlcCAqIHJhdCkgLyBwcmVjaXNpb25GYWN0b3IpLnRvRml4ZWQocHJlY2lzaW9uKTtcbiAgICB9IGVsc2Uge1xuICAgICAgcmVzdWx0ID0gdGhpcy5uek1pbiA9PT0gLUluZmluaXR5ID8gLXRoaXMubnpTdGVwIDogdGhpcy5uek1pbjtcbiAgICB9XG4gICAgcmV0dXJuIHRoaXMudG9OdW1iZXIocmVzdWx0KTtcbiAgfVxuXG4gIHN0ZXAodHlwZTogc3RyaW5nLCBlOiBNb3VzZUV2ZW50IHwgS2V5Ym9hcmRFdmVudCwgcmF0aW86IG51bWJlciA9IDEpOiB2b2lkIHtcbiAgICB0aGlzLnN0b3AoKTtcbiAgICBlLnByZXZlbnREZWZhdWx0KCk7XG4gICAgaWYgKHRoaXMubnpEaXNhYmxlZCkge1xuICAgICAgcmV0dXJuO1xuICAgIH1cbiAgICBjb25zdCB2YWx1ZSA9IHRoaXMuZ2V0Q3VycmVudFZhbGlkVmFsdWUodGhpcy5hY3R1YWxWYWx1ZSkgfHwgMDtcbiAgICBsZXQgdmFsID0gMDtcbiAgICBpZiAodHlwZSA9PT0gJ3VwJykge1xuICAgICAgdmFsID0gdGhpcy51cFN0ZXAodmFsdWUsIHJhdGlvKTtcbiAgICB9IGVsc2UgaWYgKHR5cGUgPT09ICdkb3duJykge1xuICAgICAgdmFsID0gdGhpcy5kb3duU3RlcCh2YWx1ZSwgcmF0aW8pO1xuICAgIH1cbiAgICBjb25zdCBvdXRPZlJhbmdlID0gdmFsID4gdGhpcy5uek1heCB8fCB2YWwgPCB0aGlzLm56TWluO1xuICAgIGlmICh2YWwgPiB0aGlzLm56TWF4KSB7XG4gICAgICB2YWwgPSB0aGlzLm56TWF4O1xuICAgIH0gZWxzZSBpZiAodmFsIDwgdGhpcy5uek1pbikge1xuICAgICAgdmFsID0gdGhpcy5uek1pbjtcbiAgICB9XG4gICAgdGhpcy5zZXRWYWx1ZSh2YWwsIHRydWUpO1xuICAgIHRoaXMuaXNGb2N1c2VkID0gdHJ1ZTtcbiAgICBpZiAob3V0T2ZSYW5nZSkge1xuICAgICAgcmV0dXJuO1xuICAgIH1cbiAgICB0aGlzLmF1dG9TdGVwVGltZXIgPSBzZXRUaW1lb3V0KCgpID0+IHtcbiAgICAgIHRoaXNbdHlwZV0oZSwgcmF0aW8sIHRydWUpO1xuICAgIH0sIDYwMCk7XG4gIH1cblxuICBzdG9wKCk6IHZvaWQge1xuICAgIGlmICh0aGlzLmF1dG9TdGVwVGltZXIpIHtcbiAgICAgIGNsZWFyVGltZW91dCh0aGlzLmF1dG9TdGVwVGltZXIpO1xuICAgIH1cbiAgfVxuXG4gIHNldFZhbHVlKHZhbHVlOiBudW1iZXIsIGVtaXQ6IGJvb2xlYW4pOiB2b2lkIHtcbiAgICBpZiAoZW1pdCAmJiBgJHt0aGlzLnZhbHVlfWAgIT09IGAke3ZhbHVlfWApIHtcbiAgICAgIHRoaXMub25DaGFuZ2UodmFsdWUpO1xuICAgIH1cbiAgICB0aGlzLnZhbHVlID0gdmFsdWU7XG4gICAgdGhpcy5hY3R1YWxWYWx1ZSA9IHZhbHVlO1xuICAgIGNvbnN0IGRpc3BsYXlWYWx1ZSA9IGlzTm90TmlsKHRoaXMubnpGb3JtYXR0ZXIodGhpcy52YWx1ZSkpID8gdGhpcy5uekZvcm1hdHRlcih0aGlzLnZhbHVlKSA6ICcnO1xuICAgIHRoaXMuZGlzcGxheVZhbHVlID0gZGlzcGxheVZhbHVlO1xuICAgIHRoaXMuaW5wdXRFbGVtZW50Lm5hdGl2ZUVsZW1lbnQudmFsdWUgPSBkaXNwbGF5VmFsdWU7XG4gICAgdGhpcy5kaXNhYmxlZFVwID0gdGhpcy5kaXNhYmxlZERvd24gPSBmYWxzZTtcbiAgICBpZiAodmFsdWUgfHwgdmFsdWUgPT09IDApIHtcbiAgICAgIGNvbnN0IHZhbCA9IE51bWJlcih2YWx1ZSk7XG4gICAgICBpZiAodmFsID49IHRoaXMubnpNYXgpIHtcbiAgICAgICAgdGhpcy5kaXNhYmxlZFVwID0gdHJ1ZTtcbiAgICAgIH1cbiAgICAgIGlmICh2YWwgPD0gdGhpcy5uek1pbikge1xuICAgICAgICB0aGlzLmRpc2FibGVkRG93biA9IHRydWU7XG4gICAgICB9XG4gICAgfVxuICB9XG5cbiAgb25LZXlEb3duKGU6IEtleWJvYXJkRXZlbnQpOiB2b2lkIHtcbiAgICBpZiAoZS5jb2RlID09PSAnQXJyb3dVcCcgfHwgZS5rZXlDb2RlID09PSBVUF9BUlJPVykge1xuICAgICAgY29uc3QgcmF0aW8gPSB0aGlzLmdldFJhdGlvKGUpO1xuICAgICAgdGhpcy51cChlLCByYXRpbyk7XG4gICAgICB0aGlzLnN0b3AoKTtcbiAgICB9IGVsc2UgaWYgKGUuY29kZSA9PT0gJ0Fycm93RG93bicgfHwgZS5rZXlDb2RlID09PSBET1dOX0FSUk9XKSB7XG4gICAgICBjb25zdCByYXRpbyA9IHRoaXMuZ2V0UmF0aW8oZSk7XG4gICAgICB0aGlzLmRvd24oZSwgcmF0aW8pO1xuICAgICAgdGhpcy5zdG9wKCk7XG4gICAgfSBlbHNlIGlmIChlLmtleUNvZGUgPT09IEVOVEVSKSB7XG4gICAgICB0aGlzLnNldFZhbGlkYXRlVmFsdWUoKTtcbiAgICB9XG4gIH1cblxuICBvbktleVVwKCk6IHZvaWQge1xuICAgIHRoaXMuc3RvcCgpO1xuICB9XG5cbiAgd3JpdGVWYWx1ZSh2YWx1ZTogbnVtYmVyKTogdm9pZCB7XG4gICAgdGhpcy5zZXRWYWx1ZSh2YWx1ZSwgZmFsc2UpO1xuICAgIHRoaXMuY2RyLm1hcmtGb3JDaGVjaygpO1xuICB9XG5cbiAgcmVnaXN0ZXJPbkNoYW5nZShmbjogKF86IG51bWJlcikgPT4gdm9pZCk6IHZvaWQge1xuICAgIHRoaXMub25DaGFuZ2UgPSBmbjtcbiAgfVxuXG4gIHJlZ2lzdGVyT25Ub3VjaGVkKGZuOiAoKSA9PiB2b2lkKTogdm9pZCB7XG4gICAgdGhpcy5vblRvdWNoZWQgPSBmbjtcbiAgfVxuXG4gIHNldERpc2FibGVkU3RhdGUoaXNEaXNhYmxlZDogYm9vbGVhbik6IHZvaWQge1xuICAgIHRoaXMubnpEaXNhYmxlZCA9IGlzRGlzYWJsZWQ7XG4gICAgdGhpcy5jZHIubWFya0ZvckNoZWNrKCk7XG4gIH1cblxuICBmb2N1cygpOiB2b2lkIHtcbiAgICB0aGlzLmZvY3VzTW9uaXRvci5mb2N1c1ZpYSh0aGlzLmlucHV0RWxlbWVudCwgJ2tleWJvYXJkJyk7XG4gIH1cblxuICBibHVyKCk6IHZvaWQge1xuICAgIHRoaXMuaW5wdXRFbGVtZW50Lm5hdGl2ZUVsZW1lbnQuYmx1cigpO1xuICB9XG5cbiAgY29uc3RydWN0b3IoXG4gICAgcHJpdmF0ZSBlbGVtZW50UmVmOiBFbGVtZW50UmVmLFxuICAgIHByaXZhdGUgcmVuZGVyZXI6IFJlbmRlcmVyMixcbiAgICBwcml2YXRlIGNkcjogQ2hhbmdlRGV0ZWN0b3JSZWYsXG4gICAgcHJpdmF0ZSBmb2N1c01vbml0b3I6IEZvY3VzTW9uaXRvclxuICApIHtcbiAgICByZW5kZXJlci5hZGRDbGFzcyhlbGVtZW50UmVmLm5hdGl2ZUVsZW1lbnQsICdhbnQtaW5wdXQtbnVtYmVyJyk7XG4gIH1cblxuICBuZ09uSW5pdCgpOiB2b2lkIHtcbiAgICB0aGlzLmZvY3VzTW9uaXRvci5tb25pdG9yKHRoaXMuZWxlbWVudFJlZiwgdHJ1ZSkuc3Vic2NyaWJlKGZvY3VzT3JpZ2luID0+IHtcbiAgICAgIGlmICghZm9jdXNPcmlnaW4pIHtcbiAgICAgICAgdGhpcy5uekJsdXIuZW1pdCgpO1xuICAgICAgICBQcm9taXNlLnJlc29sdmUoKS50aGVuKCgpID0+IHRoaXMub25Ub3VjaGVkKCkpO1xuICAgICAgfSBlbHNlIHtcbiAgICAgICAgdGhpcy5uekZvY3VzLmVtaXQoKTtcbiAgICAgIH1cbiAgICB9KTtcbiAgfVxuXG4gIG5nT25DaGFuZ2VzKGNoYW5nZXM6IFNpbXBsZUNoYW5nZXMpOiB2b2lkIHtcbiAgICBpZiAoY2hhbmdlcy5uekF1dG9Gb2N1cykge1xuICAgICAgdGhpcy51cGRhdGVBdXRvRm9jdXMoKTtcbiAgICB9XG4gICAgaWYgKGNoYW5nZXMubnpGb3JtYXR0ZXIpIHtcbiAgICAgIGNvbnN0IHZhbHVlID0gdGhpcy5nZXRDdXJyZW50VmFsaWRWYWx1ZSh0aGlzLmFjdHVhbFZhbHVlKTtcbiAgICAgIHRoaXMuc2V0VmFsdWUodmFsdWUsIHRydWUpO1xuICAgIH1cbiAgfVxuXG4gIG5nQWZ0ZXJWaWV3SW5pdCgpOiB2b2lkIHtcbiAgICBpZiAodGhpcy5uekF1dG9Gb2N1cykge1xuICAgICAgdGhpcy5mb2N1cygpO1xuICAgIH1cbiAgfVxuXG4gIG5nT25EZXN0cm95KCk6IHZvaWQge1xuICAgIHRoaXMuZm9jdXNNb25pdG9yLnN0b3BNb25pdG9yaW5nKHRoaXMuZWxlbWVudFJlZik7XG4gIH1cbn1cbiJdfQ==