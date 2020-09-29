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
var NzInputNumberComponent = /** @class */ (function () {
    function NzInputNumberComponent(elementRef, renderer, cdr, focusMonitor) {
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
        function () { return null; });
        this.onTouched = (/**
         * @return {?}
         */
        function () { return null; });
        this.nzBlur = new EventEmitter();
        this.nzFocus = new EventEmitter();
        this.nzSize = 'default';
        this.nzMin = -Infinity;
        this.nzMax = Infinity;
        this.nzParser = (/**
         * @param {?} value
         * @return {?}
         */
        function (value) { return value; }); // tslint:disable-line:no-any
        this.nzPlaceHolder = '';
        this.nzStep = 1;
        this.nzDisabled = false;
        this.nzAutoFocus = false;
        this.nzFormatter = (/**
         * @param {?} value
         * @return {?}
         */
        function (value) { return value; });
        renderer.addClass(elementRef.nativeElement, 'ant-input-number');
    }
    // tslint:disable-line:no-any
    /**
     * @return {?}
     */
    NzInputNumberComponent.prototype.updateAutoFocus = 
    // tslint:disable-line:no-any
    /**
     * @return {?}
     */
    function () {
        if (this.nzAutoFocus) {
            this.renderer.setAttribute(this.inputElement.nativeElement, 'autofocus', 'autofocus');
        }
        else {
            this.renderer.removeAttribute(this.inputElement.nativeElement, 'autofocus');
        }
    };
    /**
     * @param {?} value
     * @return {?}
     */
    NzInputNumberComponent.prototype.onModelChange = /**
     * @param {?} value
     * @return {?}
     */
    function (value) {
        this.actualValue = this.nzParser(value
            .trim()
            .replace(/。/g, '.')
            .replace(/[^\w\.-]+/g, ''));
        this.inputElement.nativeElement.value = this.actualValue;
    };
    /**
     * @param {?} value
     * @return {?}
     */
    NzInputNumberComponent.prototype.getCurrentValidValue = /**
     * @param {?} value
     * @return {?}
     */
    function (value) {
        /** @type {?} */
        var val = value;
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
    };
    // '1.' '1x' 'xx' '' => are not complete numbers
    // '1.' '1x' 'xx' '' => are not complete numbers
    /**
     * @param {?} num
     * @return {?}
     */
    NzInputNumberComponent.prototype.isNotCompleteNumber = 
    // '1.' '1x' 'xx' '' => are not complete numbers
    /**
     * @param {?} num
     * @return {?}
     */
    function (num) {
        return (isNaN((/** @type {?} */ (num))) ||
            num === '' ||
            num === null ||
            !!(num && num.toString().indexOf('.') === num.toString().length - 1));
    };
    /**
     * @param {?=} value
     * @return {?}
     */
    NzInputNumberComponent.prototype.getValidValue = /**
     * @param {?=} value
     * @return {?}
     */
    function (value) {
        /** @type {?} */
        var val = parseFloat((/** @type {?} */ (value)));
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
    };
    /**
     * @param {?} num
     * @return {?}
     */
    NzInputNumberComponent.prototype.toNumber = /**
     * @param {?} num
     * @return {?}
     */
    function (num) {
        if (this.isNotCompleteNumber(num)) {
            return (/** @type {?} */ (num));
        }
        if (isNotNil(this.nzPrecision)) {
            return Number(Number(num).toFixed(this.nzPrecision));
        }
        return Number(num);
    };
    /**
     * @return {?}
     */
    NzInputNumberComponent.prototype.setValidateValue = /**
     * @return {?}
     */
    function () {
        /** @type {?} */
        var value = this.getCurrentValidValue(this.actualValue);
        this.setValue(value, "" + this.value !== "" + value);
    };
    /**
     * @return {?}
     */
    NzInputNumberComponent.prototype.onBlur = /**
     * @return {?}
     */
    function () {
        this.isFocused = false;
        this.setValidateValue();
    };
    /**
     * @return {?}
     */
    NzInputNumberComponent.prototype.onFocus = /**
     * @return {?}
     */
    function () {
        this.isFocused = true;
    };
    /**
     * @param {?} e
     * @return {?}
     */
    NzInputNumberComponent.prototype.getRatio = /**
     * @param {?} e
     * @return {?}
     */
    function (e) {
        /** @type {?} */
        var ratio = 1;
        if (e.metaKey || e.ctrlKey) {
            ratio = 0.1;
        }
        else if (e.shiftKey) {
            ratio = 10;
        }
        return ratio;
    };
    /**
     * @param {?} e
     * @param {?=} ratio
     * @return {?}
     */
    NzInputNumberComponent.prototype.down = /**
     * @param {?} e
     * @param {?=} ratio
     * @return {?}
     */
    function (e, ratio) {
        if (!this.isFocused) {
            this.focus();
        }
        this.step('down', e, ratio);
    };
    /**
     * @param {?} e
     * @param {?=} ratio
     * @return {?}
     */
    NzInputNumberComponent.prototype.up = /**
     * @param {?} e
     * @param {?=} ratio
     * @return {?}
     */
    function (e, ratio) {
        if (!this.isFocused) {
            this.focus();
        }
        this.step('up', e, ratio);
    };
    /**
     * @param {?} value
     * @return {?}
     */
    NzInputNumberComponent.prototype.getPrecision = /**
     * @param {?} value
     * @return {?}
     */
    function (value) {
        /** @type {?} */
        var valueString = value.toString();
        if (valueString.indexOf('e-') >= 0) {
            return parseInt(valueString.slice(valueString.indexOf('e-') + 2), 10);
        }
        /** @type {?} */
        var precision = 0;
        if (valueString.indexOf('.') >= 0) {
            precision = valueString.length - valueString.indexOf('.') - 1;
        }
        return precision;
    };
    // step={1.0} value={1.51}
    // press +
    // then value should be 2.51, rather than 2.5
    // if this.props.precision is undefined
    // https://github.com/react-component/input-number/issues/39
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
    NzInputNumberComponent.prototype.getMaxPrecision = 
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
    function (currentValue, ratio) {
        if (isNotNil(this.nzPrecision)) {
            return this.nzPrecision;
        }
        /** @type {?} */
        var ratioPrecision = this.getPrecision(ratio);
        /** @type {?} */
        var stepPrecision = this.getPrecision(this.nzStep);
        /** @type {?} */
        var currentValuePrecision = this.getPrecision((/** @type {?} */ (currentValue)));
        if (!currentValue) {
            return ratioPrecision + stepPrecision;
        }
        return Math.max(currentValuePrecision, ratioPrecision + stepPrecision);
    };
    /**
     * @param {?} currentValue
     * @param {?} ratio
     * @return {?}
     */
    NzInputNumberComponent.prototype.getPrecisionFactor = /**
     * @param {?} currentValue
     * @param {?} ratio
     * @return {?}
     */
    function (currentValue, ratio) {
        /** @type {?} */
        var precision = this.getMaxPrecision(currentValue, ratio);
        return Math.pow(10, precision);
    };
    /**
     * @param {?} val
     * @param {?} rat
     * @return {?}
     */
    NzInputNumberComponent.prototype.upStep = /**
     * @param {?} val
     * @param {?} rat
     * @return {?}
     */
    function (val, rat) {
        /** @type {?} */
        var precisionFactor = this.getPrecisionFactor(val, rat);
        /** @type {?} */
        var precision = Math.abs(this.getMaxPrecision(val, rat));
        /** @type {?} */
        var result;
        if (typeof val === 'number') {
            result = ((precisionFactor * val + precisionFactor * this.nzStep * rat) / precisionFactor).toFixed(precision);
        }
        else {
            result = this.nzMin === -Infinity ? this.nzStep : this.nzMin;
        }
        return this.toNumber(result);
    };
    /**
     * @param {?} val
     * @param {?} rat
     * @return {?}
     */
    NzInputNumberComponent.prototype.downStep = /**
     * @param {?} val
     * @param {?} rat
     * @return {?}
     */
    function (val, rat) {
        /** @type {?} */
        var precisionFactor = this.getPrecisionFactor(val, rat);
        /** @type {?} */
        var precision = Math.abs(this.getMaxPrecision(val, rat));
        /** @type {?} */
        var result;
        if (typeof val === 'number') {
            result = ((precisionFactor * val - precisionFactor * this.nzStep * rat) / precisionFactor).toFixed(precision);
        }
        else {
            result = this.nzMin === -Infinity ? -this.nzStep : this.nzMin;
        }
        return this.toNumber(result);
    };
    /**
     * @param {?} type
     * @param {?} e
     * @param {?=} ratio
     * @return {?}
     */
    NzInputNumberComponent.prototype.step = /**
     * @param {?} type
     * @param {?} e
     * @param {?=} ratio
     * @return {?}
     */
    function (type, e, ratio) {
        var _this = this;
        if (ratio === void 0) { ratio = 1; }
        this.stop();
        e.preventDefault();
        if (this.nzDisabled) {
            return;
        }
        /** @type {?} */
        var value = this.getCurrentValidValue(this.actualValue) || 0;
        /** @type {?} */
        var val = 0;
        if (type === 'up') {
            val = this.upStep(value, ratio);
        }
        else if (type === 'down') {
            val = this.downStep(value, ratio);
        }
        /** @type {?} */
        var outOfRange = val > this.nzMax || val < this.nzMin;
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
        function () {
            _this[type](e, ratio, true);
        }), 600);
    };
    /**
     * @return {?}
     */
    NzInputNumberComponent.prototype.stop = /**
     * @return {?}
     */
    function () {
        if (this.autoStepTimer) {
            clearTimeout(this.autoStepTimer);
        }
    };
    /**
     * @param {?} value
     * @param {?} emit
     * @return {?}
     */
    NzInputNumberComponent.prototype.setValue = /**
     * @param {?} value
     * @param {?} emit
     * @return {?}
     */
    function (value, emit) {
        if (emit && "" + this.value !== "" + value) {
            this.onChange(value);
        }
        this.value = value;
        this.actualValue = value;
        /** @type {?} */
        var displayValue = isNotNil(this.nzFormatter(this.value)) ? this.nzFormatter(this.value) : '';
        this.displayValue = displayValue;
        this.inputElement.nativeElement.value = displayValue;
        this.disabledUp = this.disabledDown = false;
        if (value || value === 0) {
            /** @type {?} */
            var val = Number(value);
            if (val >= this.nzMax) {
                this.disabledUp = true;
            }
            if (val <= this.nzMin) {
                this.disabledDown = true;
            }
        }
    };
    /**
     * @param {?} e
     * @return {?}
     */
    NzInputNumberComponent.prototype.onKeyDown = /**
     * @param {?} e
     * @return {?}
     */
    function (e) {
        if (e.code === 'ArrowUp' || e.keyCode === UP_ARROW) {
            /** @type {?} */
            var ratio = this.getRatio(e);
            this.up(e, ratio);
            this.stop();
        }
        else if (e.code === 'ArrowDown' || e.keyCode === DOWN_ARROW) {
            /** @type {?} */
            var ratio = this.getRatio(e);
            this.down(e, ratio);
            this.stop();
        }
        else if (e.keyCode === ENTER) {
            this.setValidateValue();
        }
    };
    /**
     * @return {?}
     */
    NzInputNumberComponent.prototype.onKeyUp = /**
     * @return {?}
     */
    function () {
        this.stop();
    };
    /**
     * @param {?} value
     * @return {?}
     */
    NzInputNumberComponent.prototype.writeValue = /**
     * @param {?} value
     * @return {?}
     */
    function (value) {
        this.setValue(value, false);
        this.cdr.markForCheck();
    };
    /**
     * @param {?} fn
     * @return {?}
     */
    NzInputNumberComponent.prototype.registerOnChange = /**
     * @param {?} fn
     * @return {?}
     */
    function (fn) {
        this.onChange = fn;
    };
    /**
     * @param {?} fn
     * @return {?}
     */
    NzInputNumberComponent.prototype.registerOnTouched = /**
     * @param {?} fn
     * @return {?}
     */
    function (fn) {
        this.onTouched = fn;
    };
    /**
     * @param {?} isDisabled
     * @return {?}
     */
    NzInputNumberComponent.prototype.setDisabledState = /**
     * @param {?} isDisabled
     * @return {?}
     */
    function (isDisabled) {
        this.nzDisabled = isDisabled;
        this.cdr.markForCheck();
    };
    /**
     * @return {?}
     */
    NzInputNumberComponent.prototype.focus = /**
     * @return {?}
     */
    function () {
        this.focusMonitor.focusVia(this.inputElement, 'keyboard');
    };
    /**
     * @return {?}
     */
    NzInputNumberComponent.prototype.blur = /**
     * @return {?}
     */
    function () {
        this.inputElement.nativeElement.blur();
    };
    /**
     * @return {?}
     */
    NzInputNumberComponent.prototype.ngOnInit = /**
     * @return {?}
     */
    function () {
        var _this = this;
        this.focusMonitor.monitor(this.elementRef, true).subscribe((/**
         * @param {?} focusOrigin
         * @return {?}
         */
        function (focusOrigin) {
            if (!focusOrigin) {
                _this.nzBlur.emit();
                Promise.resolve().then((/**
                 * @return {?}
                 */
                function () { return _this.onTouched(); }));
            }
            else {
                _this.nzFocus.emit();
            }
        }));
    };
    /**
     * @param {?} changes
     * @return {?}
     */
    NzInputNumberComponent.prototype.ngOnChanges = /**
     * @param {?} changes
     * @return {?}
     */
    function (changes) {
        if (changes.nzAutoFocus) {
            this.updateAutoFocus();
        }
        if (changes.nzFormatter) {
            /** @type {?} */
            var value = this.getCurrentValidValue(this.actualValue);
            this.setValue(value, true);
        }
    };
    /**
     * @return {?}
     */
    NzInputNumberComponent.prototype.ngAfterViewInit = /**
     * @return {?}
     */
    function () {
        if (this.nzAutoFocus) {
            this.focus();
        }
    };
    /**
     * @return {?}
     */
    NzInputNumberComponent.prototype.ngOnDestroy = /**
     * @return {?}
     */
    function () {
        this.focusMonitor.stopMonitoring(this.elementRef);
    };
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
                            function () { return NzInputNumberComponent; })),
                            multi: true
                        }
                    ],
                    changeDetection: ChangeDetectionStrategy.OnPush,
                    encapsulation: ViewEncapsulation.None,
                    host: {
                        '[class.ant-input-number-focused]': 'isFocused',
                        '[class.ant-input-number-lg]': "nzSize === 'large'",
                        '[class.ant-input-number-sm]': "nzSize === 'small'",
                        '[class.ant-input-number-disabled]': 'nzDisabled'
                    }
                }] }
    ];
    /** @nocollapse */
    NzInputNumberComponent.ctorParameters = function () { return [
        { type: ElementRef },
        { type: Renderer2 },
        { type: ChangeDetectorRef },
        { type: FocusMonitor }
    ]; };
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
    return NzInputNumberComponent;
}());
export { NzInputNumberComponent };
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
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotaW5wdXQtbnVtYmVyLmNvbXBvbmVudC5qcyIsInNvdXJjZVJvb3QiOiJuZzovL25nLXpvcnJvLWFudGQvIiwic291cmNlcyI6WyJpbnB1dC1udW1iZXIvbnotaW5wdXQtbnVtYmVyLmNvbXBvbmVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7OztBQUFBLE9BQU8sRUFBRSxZQUFZLEVBQUUsTUFBTSxtQkFBbUIsQ0FBQztBQUNqRCxPQUFPLEVBQUUsVUFBVSxFQUFFLEtBQUssRUFBRSxRQUFRLEVBQUUsTUFBTSx1QkFBdUIsQ0FBQztBQUNwRSxPQUFPLEVBQ0wsVUFBVSxFQUVWLHVCQUF1QixFQUN2QixpQkFBaUIsRUFDakIsU0FBUyxFQUNULFVBQVUsRUFDVixZQUFZLEVBQ1osS0FBSyxFQUlMLE1BQU0sRUFDTixTQUFTLEVBRVQsU0FBUyxFQUNULGlCQUFpQixFQUNsQixNQUFNLGVBQWUsQ0FBQztBQUN2QixPQUFPLEVBQXdCLGlCQUFpQixFQUFFLE1BQU0sZ0JBQWdCLENBQUM7QUFHekUsT0FBTyxFQUFFLFFBQVEsRUFBRSxNQUFNLG9CQUFvQixDQUFDO0FBQzlDLE9BQU8sRUFBRSxZQUFZLEVBQUUsTUFBTSxzQkFBc0IsQ0FBQztBQUVwRDtJQW1URSxnQ0FDVSxVQUFzQixFQUN0QixRQUFtQixFQUNuQixHQUFzQixFQUN0QixZQUEwQjtRQUgxQixlQUFVLEdBQVYsVUFBVSxDQUFZO1FBQ3RCLGFBQVEsR0FBUixRQUFRLENBQVc7UUFDbkIsUUFBRyxHQUFILEdBQUcsQ0FBbUI7UUFDdEIsaUJBQVksR0FBWixZQUFZLENBQWM7UUEvUnBDLGNBQVMsR0FBRyxLQUFLLENBQUM7UUFDbEIsZUFBVSxHQUFHLEtBQUssQ0FBQztRQUNuQixpQkFBWSxHQUFHLEtBQUssQ0FBQztRQUNyQixhQUFROzs7UUFBNEIsY0FBTSxPQUFBLElBQUksRUFBSixDQUFJLEVBQUM7UUFDL0MsY0FBUzs7O1FBQWUsY0FBTSxPQUFBLElBQUksRUFBSixDQUFJLEVBQUM7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QUs7NkJBQ0wsS0FBSzs4QkFDTCxLQUFLOzhCQUNMLEtBQUs7O0lBRm1CO1FBQWYsWUFBWSxFQUFFOzs4REFBb0I7SUFDbkI7UUFBZixZQUFZLEVBQUU7OytEQUFxQjtJQWtUL0MsNkJBQUM7Q0FBQSxBQTFWRCxJQTBWQztTQXZVWSxzQkFBc0I7Ozs7OztJQUNqQywrQ0FBOEI7Ozs7O0lBQzlCLDZDQUFxQzs7Ozs7SUFDckMsdUNBQStCOztJQUMvQiw4Q0FBOEI7O0lBQzlCLDJDQUFrQjs7SUFDbEIsNENBQW1COztJQUNuQiw4Q0FBcUI7O0lBQ3JCLDBDQUErQzs7SUFDL0MsMkNBQW1DOztJQUNuQyx3Q0FBK0M7O0lBQy9DLHlDQUFnRDs7SUFDaEQsOENBQW9EOztJQUNwRCx3Q0FBMkM7O0lBQzNDLHVDQUFtQzs7SUFDbkMsdUNBQWtDOztJQUNsQywwQ0FBMEM7O0lBQzFDLDZDQUE2Qjs7SUFDN0IsK0NBQTRCOztJQUM1Qix3Q0FBb0I7O0lBQ3BCLDRDQUE0Qzs7SUFDNUMsNkNBQTZDOztJQUM3Qyw2Q0FBMEU7Ozs7O0lBMlF4RSw0Q0FBOEI7Ozs7O0lBQzlCLDBDQUEyQjs7Ozs7SUFDM0IscUNBQThCOzs7OztJQUM5Qiw4Q0FBa0MiLCJzb3VyY2VzQ29udGVudCI6WyJpbXBvcnQgeyBGb2N1c01vbml0b3IgfSBmcm9tICdAYW5ndWxhci9jZGsvYTExeSc7XG5pbXBvcnQgeyBET1dOX0FSUk9XLCBFTlRFUiwgVVBfQVJST1cgfSBmcm9tICdAYW5ndWxhci9jZGsva2V5Y29kZXMnO1xuaW1wb3J0IHtcbiAgZm9yd2FyZFJlZixcbiAgQWZ0ZXJWaWV3SW5pdCxcbiAgQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3ksXG4gIENoYW5nZURldGVjdG9yUmVmLFxuICBDb21wb25lbnQsXG4gIEVsZW1lbnRSZWYsXG4gIEV2ZW50RW1pdHRlcixcbiAgSW5wdXQsXG4gIE9uQ2hhbmdlcyxcbiAgT25EZXN0cm95LFxuICBPbkluaXQsXG4gIE91dHB1dCxcbiAgUmVuZGVyZXIyLFxuICBTaW1wbGVDaGFuZ2VzLFxuICBWaWV3Q2hpbGQsXG4gIFZpZXdFbmNhcHN1bGF0aW9uXG59IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuaW1wb3J0IHsgQ29udHJvbFZhbHVlQWNjZXNzb3IsIE5HX1ZBTFVFX0FDQ0VTU09SIH0gZnJvbSAnQGFuZ3VsYXIvZm9ybXMnO1xuaW1wb3J0IHsgTnpTaXplTERTVHlwZSB9IGZyb20gJy4uL2NvcmUvdHlwZXMvc2l6ZSc7XG5cbmltcG9ydCB7IGlzTm90TmlsIH0gZnJvbSAnLi4vY29yZS91dGlsL2NoZWNrJztcbmltcG9ydCB7IElucHV0Qm9vbGVhbiB9IGZyb20gJy4uL2NvcmUvdXRpbC9jb252ZXJ0JztcblxuQENvbXBvbmVudCh7XG4gIHNlbGVjdG9yOiAnbnotaW5wdXQtbnVtYmVyJyxcbiAgdGVtcGxhdGVVcmw6ICcuL256LWlucHV0LW51bWJlci5jb21wb25lbnQuaHRtbCcsXG4gIHByb3ZpZGVyczogW1xuICAgIHtcbiAgICAgIHByb3ZpZGU6IE5HX1ZBTFVFX0FDQ0VTU09SLFxuICAgICAgdXNlRXhpc3Rpbmc6IGZvcndhcmRSZWYoKCkgPT4gTnpJbnB1dE51bWJlckNvbXBvbmVudCksXG4gICAgICBtdWx0aTogdHJ1ZVxuICAgIH1cbiAgXSxcbiAgY2hhbmdlRGV0ZWN0aW9uOiBDaGFuZ2VEZXRlY3Rpb25TdHJhdGVneS5PblB1c2gsXG4gIGVuY2Fwc3VsYXRpb246IFZpZXdFbmNhcHN1bGF0aW9uLk5vbmUsXG4gIGhvc3Q6IHtcbiAgICAnW2NsYXNzLmFudC1pbnB1dC1udW1iZXItZm9jdXNlZF0nOiAnaXNGb2N1c2VkJyxcbiAgICAnW2NsYXNzLmFudC1pbnB1dC1udW1iZXItbGddJzogYG56U2l6ZSA9PT0gJ2xhcmdlJ2AsXG4gICAgJ1tjbGFzcy5hbnQtaW5wdXQtbnVtYmVyLXNtXSc6IGBuelNpemUgPT09ICdzbWFsbCdgLFxuICAgICdbY2xhc3MuYW50LWlucHV0LW51bWJlci1kaXNhYmxlZF0nOiAnbnpEaXNhYmxlZCdcbiAgfVxufSlcbmV4cG9ydCBjbGFzcyBOeklucHV0TnVtYmVyQ29tcG9uZW50IGltcGxlbWVudHMgQ29udHJvbFZhbHVlQWNjZXNzb3IsIEFmdGVyVmlld0luaXQsIE9uQ2hhbmdlcywgT25Jbml0LCBPbkRlc3Ryb3kge1xuICBwcml2YXRlIGF1dG9TdGVwVGltZXI6IG51bWJlcjtcbiAgcHJpdmF0ZSBhY3R1YWxWYWx1ZTogc3RyaW5nIHwgbnVtYmVyO1xuICBwcml2YXRlIHZhbHVlOiBzdHJpbmcgfCBudW1iZXI7XG4gIGRpc3BsYXlWYWx1ZTogc3RyaW5nIHwgbnVtYmVyO1xuICBpc0ZvY3VzZWQgPSBmYWxzZTtcbiAgZGlzYWJsZWRVcCA9IGZhbHNlO1xuICBkaXNhYmxlZERvd24gPSBmYWxzZTtcbiAgb25DaGFuZ2U6ICh2YWx1ZTogbnVtYmVyKSA9PiB2b2lkID0gKCkgPT4gbnVsbDtcbiAgb25Ub3VjaGVkOiAoKSA9PiB2b2lkID0gKCkgPT4gbnVsbDtcbiAgQE91dHB1dCgpIHJlYWRvbmx5IG56Qmx1ciA9IG5ldyBFdmVudEVtaXR0ZXIoKTtcbiAgQE91dHB1dCgpIHJlYWRvbmx5IG56Rm9jdXMgPSBuZXcgRXZlbnRFbWl0dGVyKCk7XG4gIEBWaWV3Q2hpbGQoJ2lucHV0RWxlbWVudCcpIGlucHV0RWxlbWVudDogRWxlbWVudFJlZjtcbiAgQElucHV0KCkgbnpTaXplOiBOelNpemVMRFNUeXBlID0gJ2RlZmF1bHQnO1xuICBASW5wdXQoKSBuek1pbjogbnVtYmVyID0gLUluZmluaXR5O1xuICBASW5wdXQoKSBuek1heDogbnVtYmVyID0gSW5maW5pdHk7XG4gIEBJbnB1dCgpIG56UGFyc2VyID0gKHZhbHVlOiBhbnkpID0+IHZhbHVlOyAvLyB0c2xpbnQ6ZGlzYWJsZS1saW5lOm5vLWFueVxuICBASW5wdXQoKSBuelByZWNpc2lvbjogbnVtYmVyO1xuICBASW5wdXQoKSBuelBsYWNlSG9sZGVyID0gJyc7XG4gIEBJbnB1dCgpIG56U3RlcCA9IDE7XG4gIEBJbnB1dCgpIEBJbnB1dEJvb2xlYW4oKSBuekRpc2FibGVkID0gZmFsc2U7XG4gIEBJbnB1dCgpIEBJbnB1dEJvb2xlYW4oKSBuekF1dG9Gb2N1cyA9IGZhbHNlO1xuICBASW5wdXQoKSBuekZvcm1hdHRlcjogKHZhbHVlOiBudW1iZXIpID0+IHN0cmluZyB8IG51bWJlciA9IHZhbHVlID0+IHZhbHVlO1xuXG4gIFtwcm9wZXJ0eTogc3RyaW5nXTogYW55OyAvLyB0c2xpbnQ6ZGlzYWJsZS1saW5lOm5vLWFueVxuXG4gIHVwZGF0ZUF1dG9Gb2N1cygpOiB2b2lkIHtcbiAgICBpZiAodGhpcy5uekF1dG9Gb2N1cykge1xuICAgICAgdGhpcy5yZW5kZXJlci5zZXRBdHRyaWJ1dGUodGhpcy5pbnB1dEVsZW1lbnQubmF0aXZlRWxlbWVudCwgJ2F1dG9mb2N1cycsICdhdXRvZm9jdXMnKTtcbiAgICB9IGVsc2Uge1xuICAgICAgdGhpcy5yZW5kZXJlci5yZW1vdmVBdHRyaWJ1dGUodGhpcy5pbnB1dEVsZW1lbnQubmF0aXZlRWxlbWVudCwgJ2F1dG9mb2N1cycpO1xuICAgIH1cbiAgfVxuXG4gIG9uTW9kZWxDaGFuZ2UodmFsdWU6IHN0cmluZyk6IHZvaWQge1xuICAgIHRoaXMuYWN0dWFsVmFsdWUgPSB0aGlzLm56UGFyc2VyKFxuICAgICAgdmFsdWVcbiAgICAgICAgLnRyaW0oKVxuICAgICAgICAucmVwbGFjZSgv44CCL2csICcuJylcbiAgICAgICAgLnJlcGxhY2UoL1teXFx3XFwuLV0rL2csICcnKVxuICAgICk7XG4gICAgdGhpcy5pbnB1dEVsZW1lbnQubmF0aXZlRWxlbWVudC52YWx1ZSA9IHRoaXMuYWN0dWFsVmFsdWU7XG4gIH1cblxuICBnZXRDdXJyZW50VmFsaWRWYWx1ZSh2YWx1ZTogc3RyaW5nIHwgbnVtYmVyKTogbnVtYmVyIHtcbiAgICBsZXQgdmFsID0gdmFsdWU7XG4gICAgaWYgKHZhbCA9PT0gJycpIHtcbiAgICAgIHZhbCA9ICcnO1xuICAgIH0gZWxzZSBpZiAoIXRoaXMuaXNOb3RDb21wbGV0ZU51bWJlcih2YWwpKSB7XG4gICAgICB2YWwgPSB0aGlzLmdldFZhbGlkVmFsdWUodmFsKSBhcyBzdHJpbmc7XG4gICAgfSBlbHNlIHtcbiAgICAgIHZhbCA9IHRoaXMudmFsdWU7XG4gICAgfVxuICAgIHJldHVybiB0aGlzLnRvTnVtYmVyKHZhbCk7XG4gIH1cblxuICAvLyAnMS4nICcxeCcgJ3h4JyAnJyA9PiBhcmUgbm90IGNvbXBsZXRlIG51bWJlcnNcbiAgaXNOb3RDb21wbGV0ZU51bWJlcihudW06IHN0cmluZyB8IG51bWJlcik6IGJvb2xlYW4ge1xuICAgIHJldHVybiAoXG4gICAgICBpc05hTihudW0gYXMgbnVtYmVyKSB8fFxuICAgICAgbnVtID09PSAnJyB8fFxuICAgICAgbnVtID09PSBudWxsIHx8XG4gICAgICAhIShudW0gJiYgbnVtLnRvU3RyaW5nKCkuaW5kZXhPZignLicpID09PSBudW0udG9TdHJpbmcoKS5sZW5ndGggLSAxKVxuICAgICk7XG4gIH1cblxuICBnZXRWYWxpZFZhbHVlKHZhbHVlPzogc3RyaW5nIHwgbnVtYmVyKTogc3RyaW5nIHwgbnVtYmVyIHwgdW5kZWZpbmVkIHtcbiAgICBsZXQgdmFsID0gcGFyc2VGbG9hdCh2YWx1ZSBhcyBzdHJpbmcpO1xuICAgIC8vIGh0dHBzOi8vZ2l0aHViLmNvbS9hbnQtZGVzaWduL2FudC1kZXNpZ24vaXNzdWVzLzczNThcbiAgICBpZiAoaXNOYU4odmFsKSkge1xuICAgICAgcmV0dXJuIHZhbHVlO1xuICAgIH1cbiAgICBpZiAodmFsIDwgdGhpcy5uek1pbikge1xuICAgICAgdmFsID0gdGhpcy5uek1pbjtcbiAgICB9XG4gICAgaWYgKHZhbCA+IHRoaXMubnpNYXgpIHtcbiAgICAgIHZhbCA9IHRoaXMubnpNYXg7XG4gICAgfVxuICAgIHJldHVybiB2YWw7XG4gIH1cblxuICB0b051bWJlcihudW06IHN0cmluZyB8IG51bWJlcik6IG51bWJlciB7XG4gICAgaWYgKHRoaXMuaXNOb3RDb21wbGV0ZU51bWJlcihudW0pKSB7XG4gICAgICByZXR1cm4gbnVtIGFzIG51bWJlcjtcbiAgICB9XG4gICAgaWYgKGlzTm90TmlsKHRoaXMubnpQcmVjaXNpb24pKSB7XG4gICAgICByZXR1cm4gTnVtYmVyKE51bWJlcihudW0pLnRvRml4ZWQodGhpcy5uelByZWNpc2lvbikpO1xuICAgIH1cbiAgICByZXR1cm4gTnVtYmVyKG51bSk7XG4gIH1cblxuICBzZXRWYWxpZGF0ZVZhbHVlKCk6IHZvaWQge1xuICAgIGNvbnN0IHZhbHVlID0gdGhpcy5nZXRDdXJyZW50VmFsaWRWYWx1ZSh0aGlzLmFjdHVhbFZhbHVlKTtcbiAgICB0aGlzLnNldFZhbHVlKHZhbHVlLCBgJHt0aGlzLnZhbHVlfWAgIT09IGAke3ZhbHVlfWApO1xuICB9XG5cbiAgb25CbHVyKCk6IHZvaWQge1xuICAgIHRoaXMuaXNGb2N1c2VkID0gZmFsc2U7XG4gICAgdGhpcy5zZXRWYWxpZGF0ZVZhbHVlKCk7XG4gIH1cblxuICBvbkZvY3VzKCk6IHZvaWQge1xuICAgIHRoaXMuaXNGb2N1c2VkID0gdHJ1ZTtcbiAgfVxuXG4gIGdldFJhdGlvKGU6IEtleWJvYXJkRXZlbnQpOiBudW1iZXIge1xuICAgIGxldCByYXRpbyA9IDE7XG4gICAgaWYgKGUubWV0YUtleSB8fCBlLmN0cmxLZXkpIHtcbiAgICAgIHJhdGlvID0gMC4xO1xuICAgIH0gZWxzZSBpZiAoZS5zaGlmdEtleSkge1xuICAgICAgcmF0aW8gPSAxMDtcbiAgICB9XG4gICAgcmV0dXJuIHJhdGlvO1xuICB9XG5cbiAgZG93bihlOiBNb3VzZUV2ZW50IHwgS2V5Ym9hcmRFdmVudCwgcmF0aW8/OiBudW1iZXIpOiB2b2lkIHtcbiAgICBpZiAoIXRoaXMuaXNGb2N1c2VkKSB7XG4gICAgICB0aGlzLmZvY3VzKCk7XG4gICAgfVxuICAgIHRoaXMuc3RlcCgnZG93bicsIGUsIHJhdGlvKTtcbiAgfVxuXG4gIHVwKGU6IE1vdXNlRXZlbnQgfCBLZXlib2FyZEV2ZW50LCByYXRpbz86IG51bWJlcik6IHZvaWQge1xuICAgIGlmICghdGhpcy5pc0ZvY3VzZWQpIHtcbiAgICAgIHRoaXMuZm9jdXMoKTtcbiAgICB9XG4gICAgdGhpcy5zdGVwKCd1cCcsIGUsIHJhdGlvKTtcbiAgfVxuXG4gIGdldFByZWNpc2lvbih2YWx1ZTogbnVtYmVyKTogbnVtYmVyIHtcbiAgICBjb25zdCB2YWx1ZVN0cmluZyA9IHZhbHVlLnRvU3RyaW5nKCk7XG4gICAgaWYgKHZhbHVlU3RyaW5nLmluZGV4T2YoJ2UtJykgPj0gMCkge1xuICAgICAgcmV0dXJuIHBhcnNlSW50KHZhbHVlU3RyaW5nLnNsaWNlKHZhbHVlU3RyaW5nLmluZGV4T2YoJ2UtJykgKyAyKSwgMTApO1xuICAgIH1cbiAgICBsZXQgcHJlY2lzaW9uID0gMDtcbiAgICBpZiAodmFsdWVTdHJpbmcuaW5kZXhPZignLicpID49IDApIHtcbiAgICAgIHByZWNpc2lvbiA9IHZhbHVlU3RyaW5nLmxlbmd0aCAtIHZhbHVlU3RyaW5nLmluZGV4T2YoJy4nKSAtIDE7XG4gICAgfVxuICAgIHJldHVybiBwcmVjaXNpb247XG4gIH1cblxuICAvLyBzdGVwPXsxLjB9IHZhbHVlPXsxLjUxfVxuICAvLyBwcmVzcyArXG4gIC8vIHRoZW4gdmFsdWUgc2hvdWxkIGJlIDIuNTEsIHJhdGhlciB0aGFuIDIuNVxuICAvLyBpZiB0aGlzLnByb3BzLnByZWNpc2lvbiBpcyB1bmRlZmluZWRcbiAgLy8gaHR0cHM6Ly9naXRodWIuY29tL3JlYWN0LWNvbXBvbmVudC9pbnB1dC1udW1iZXIvaXNzdWVzLzM5XG4gIGdldE1heFByZWNpc2lvbihjdXJyZW50VmFsdWU6IHN0cmluZyB8IG51bWJlciwgcmF0aW86IG51bWJlcik6IG51bWJlciB7XG4gICAgaWYgKGlzTm90TmlsKHRoaXMubnpQcmVjaXNpb24pKSB7XG4gICAgICByZXR1cm4gdGhpcy5uelByZWNpc2lvbjtcbiAgICB9XG4gICAgY29uc3QgcmF0aW9QcmVjaXNpb24gPSB0aGlzLmdldFByZWNpc2lvbihyYXRpbyk7XG4gICAgY29uc3Qgc3RlcFByZWNpc2lvbiA9IHRoaXMuZ2V0UHJlY2lzaW9uKHRoaXMubnpTdGVwKTtcbiAgICBjb25zdCBjdXJyZW50VmFsdWVQcmVjaXNpb24gPSB0aGlzLmdldFByZWNpc2lvbihjdXJyZW50VmFsdWUgYXMgbnVtYmVyKTtcbiAgICBpZiAoIWN1cnJlbnRWYWx1ZSkge1xuICAgICAgcmV0dXJuIHJhdGlvUHJlY2lzaW9uICsgc3RlcFByZWNpc2lvbjtcbiAgICB9XG4gICAgcmV0dXJuIE1hdGgubWF4KGN1cnJlbnRWYWx1ZVByZWNpc2lvbiwgcmF0aW9QcmVjaXNpb24gKyBzdGVwUHJlY2lzaW9uKTtcbiAgfVxuXG4gIGdldFByZWNpc2lvbkZhY3RvcihjdXJyZW50VmFsdWU6IHN0cmluZyB8IG51bWJlciwgcmF0aW86IG51bWJlcik6IG51bWJlciB7XG4gICAgY29uc3QgcHJlY2lzaW9uID0gdGhpcy5nZXRNYXhQcmVjaXNpb24oY3VycmVudFZhbHVlLCByYXRpbyk7XG4gICAgcmV0dXJuIE1hdGgucG93KDEwLCBwcmVjaXNpb24pO1xuICB9XG5cbiAgdXBTdGVwKHZhbDogc3RyaW5nIHwgbnVtYmVyLCByYXQ6IG51bWJlcik6IG51bWJlciB7XG4gICAgY29uc3QgcHJlY2lzaW9uRmFjdG9yID0gdGhpcy5nZXRQcmVjaXNpb25GYWN0b3IodmFsLCByYXQpO1xuICAgIGNvbnN0IHByZWNpc2lvbiA9IE1hdGguYWJzKHRoaXMuZ2V0TWF4UHJlY2lzaW9uKHZhbCwgcmF0KSk7XG4gICAgbGV0IHJlc3VsdDtcbiAgICBpZiAodHlwZW9mIHZhbCA9PT0gJ251bWJlcicpIHtcbiAgICAgIHJlc3VsdCA9ICgocHJlY2lzaW9uRmFjdG9yICogdmFsICsgcHJlY2lzaW9uRmFjdG9yICogdGhpcy5uelN0ZXAgKiByYXQpIC8gcHJlY2lzaW9uRmFjdG9yKS50b0ZpeGVkKHByZWNpc2lvbik7XG4gICAgfSBlbHNlIHtcbiAgICAgIHJlc3VsdCA9IHRoaXMubnpNaW4gPT09IC1JbmZpbml0eSA/IHRoaXMubnpTdGVwIDogdGhpcy5uek1pbjtcbiAgICB9XG4gICAgcmV0dXJuIHRoaXMudG9OdW1iZXIocmVzdWx0KTtcbiAgfVxuXG4gIGRvd25TdGVwKHZhbDogc3RyaW5nIHwgbnVtYmVyLCByYXQ6IG51bWJlcik6IG51bWJlciB7XG4gICAgY29uc3QgcHJlY2lzaW9uRmFjdG9yID0gdGhpcy5nZXRQcmVjaXNpb25GYWN0b3IodmFsLCByYXQpO1xuICAgIGNvbnN0IHByZWNpc2lvbiA9IE1hdGguYWJzKHRoaXMuZ2V0TWF4UHJlY2lzaW9uKHZhbCwgcmF0KSk7XG4gICAgbGV0IHJlc3VsdDtcbiAgICBpZiAodHlwZW9mIHZhbCA9PT0gJ251bWJlcicpIHtcbiAgICAgIHJlc3VsdCA9ICgocHJlY2lzaW9uRmFjdG9yICogdmFsIC0gcHJlY2lzaW9uRmFjdG9yICogdGhpcy5uelN0ZXAgKiByYXQpIC8gcHJlY2lzaW9uRmFjdG9yKS50b0ZpeGVkKHByZWNpc2lvbik7XG4gICAgfSBlbHNlIHtcbiAgICAgIHJlc3VsdCA9IHRoaXMubnpNaW4gPT09IC1JbmZpbml0eSA/IC10aGlzLm56U3RlcCA6IHRoaXMubnpNaW47XG4gICAgfVxuICAgIHJldHVybiB0aGlzLnRvTnVtYmVyKHJlc3VsdCk7XG4gIH1cblxuICBzdGVwKHR5cGU6IHN0cmluZywgZTogTW91c2VFdmVudCB8IEtleWJvYXJkRXZlbnQsIHJhdGlvOiBudW1iZXIgPSAxKTogdm9pZCB7XG4gICAgdGhpcy5zdG9wKCk7XG4gICAgZS5wcmV2ZW50RGVmYXVsdCgpO1xuICAgIGlmICh0aGlzLm56RGlzYWJsZWQpIHtcbiAgICAgIHJldHVybjtcbiAgICB9XG4gICAgY29uc3QgdmFsdWUgPSB0aGlzLmdldEN1cnJlbnRWYWxpZFZhbHVlKHRoaXMuYWN0dWFsVmFsdWUpIHx8IDA7XG4gICAgbGV0IHZhbCA9IDA7XG4gICAgaWYgKHR5cGUgPT09ICd1cCcpIHtcbiAgICAgIHZhbCA9IHRoaXMudXBTdGVwKHZhbHVlLCByYXRpbyk7XG4gICAgfSBlbHNlIGlmICh0eXBlID09PSAnZG93bicpIHtcbiAgICAgIHZhbCA9IHRoaXMuZG93blN0ZXAodmFsdWUsIHJhdGlvKTtcbiAgICB9XG4gICAgY29uc3Qgb3V0T2ZSYW5nZSA9IHZhbCA+IHRoaXMubnpNYXggfHwgdmFsIDwgdGhpcy5uek1pbjtcbiAgICBpZiAodmFsID4gdGhpcy5uek1heCkge1xuICAgICAgdmFsID0gdGhpcy5uek1heDtcbiAgICB9IGVsc2UgaWYgKHZhbCA8IHRoaXMubnpNaW4pIHtcbiAgICAgIHZhbCA9IHRoaXMubnpNaW47XG4gICAgfVxuICAgIHRoaXMuc2V0VmFsdWUodmFsLCB0cnVlKTtcbiAgICB0aGlzLmlzRm9jdXNlZCA9IHRydWU7XG4gICAgaWYgKG91dE9mUmFuZ2UpIHtcbiAgICAgIHJldHVybjtcbiAgICB9XG4gICAgdGhpcy5hdXRvU3RlcFRpbWVyID0gc2V0VGltZW91dCgoKSA9PiB7XG4gICAgICB0aGlzW3R5cGVdKGUsIHJhdGlvLCB0cnVlKTtcbiAgICB9LCA2MDApO1xuICB9XG5cbiAgc3RvcCgpOiB2b2lkIHtcbiAgICBpZiAodGhpcy5hdXRvU3RlcFRpbWVyKSB7XG4gICAgICBjbGVhclRpbWVvdXQodGhpcy5hdXRvU3RlcFRpbWVyKTtcbiAgICB9XG4gIH1cblxuICBzZXRWYWx1ZSh2YWx1ZTogbnVtYmVyLCBlbWl0OiBib29sZWFuKTogdm9pZCB7XG4gICAgaWYgKGVtaXQgJiYgYCR7dGhpcy52YWx1ZX1gICE9PSBgJHt2YWx1ZX1gKSB7XG4gICAgICB0aGlzLm9uQ2hhbmdlKHZhbHVlKTtcbiAgICB9XG4gICAgdGhpcy52YWx1ZSA9IHZhbHVlO1xuICAgIHRoaXMuYWN0dWFsVmFsdWUgPSB2YWx1ZTtcbiAgICBjb25zdCBkaXNwbGF5VmFsdWUgPSBpc05vdE5pbCh0aGlzLm56Rm9ybWF0dGVyKHRoaXMudmFsdWUpKSA/IHRoaXMubnpGb3JtYXR0ZXIodGhpcy52YWx1ZSkgOiAnJztcbiAgICB0aGlzLmRpc3BsYXlWYWx1ZSA9IGRpc3BsYXlWYWx1ZTtcbiAgICB0aGlzLmlucHV0RWxlbWVudC5uYXRpdmVFbGVtZW50LnZhbHVlID0gZGlzcGxheVZhbHVlO1xuICAgIHRoaXMuZGlzYWJsZWRVcCA9IHRoaXMuZGlzYWJsZWREb3duID0gZmFsc2U7XG4gICAgaWYgKHZhbHVlIHx8IHZhbHVlID09PSAwKSB7XG4gICAgICBjb25zdCB2YWwgPSBOdW1iZXIodmFsdWUpO1xuICAgICAgaWYgKHZhbCA+PSB0aGlzLm56TWF4KSB7XG4gICAgICAgIHRoaXMuZGlzYWJsZWRVcCA9IHRydWU7XG4gICAgICB9XG4gICAgICBpZiAodmFsIDw9IHRoaXMubnpNaW4pIHtcbiAgICAgICAgdGhpcy5kaXNhYmxlZERvd24gPSB0cnVlO1xuICAgICAgfVxuICAgIH1cbiAgfVxuXG4gIG9uS2V5RG93bihlOiBLZXlib2FyZEV2ZW50KTogdm9pZCB7XG4gICAgaWYgKGUuY29kZSA9PT0gJ0Fycm93VXAnIHx8IGUua2V5Q29kZSA9PT0gVVBfQVJST1cpIHtcbiAgICAgIGNvbnN0IHJhdGlvID0gdGhpcy5nZXRSYXRpbyhlKTtcbiAgICAgIHRoaXMudXAoZSwgcmF0aW8pO1xuICAgICAgdGhpcy5zdG9wKCk7XG4gICAgfSBlbHNlIGlmIChlLmNvZGUgPT09ICdBcnJvd0Rvd24nIHx8IGUua2V5Q29kZSA9PT0gRE9XTl9BUlJPVykge1xuICAgICAgY29uc3QgcmF0aW8gPSB0aGlzLmdldFJhdGlvKGUpO1xuICAgICAgdGhpcy5kb3duKGUsIHJhdGlvKTtcbiAgICAgIHRoaXMuc3RvcCgpO1xuICAgIH0gZWxzZSBpZiAoZS5rZXlDb2RlID09PSBFTlRFUikge1xuICAgICAgdGhpcy5zZXRWYWxpZGF0ZVZhbHVlKCk7XG4gICAgfVxuICB9XG5cbiAgb25LZXlVcCgpOiB2b2lkIHtcbiAgICB0aGlzLnN0b3AoKTtcbiAgfVxuXG4gIHdyaXRlVmFsdWUodmFsdWU6IG51bWJlcik6IHZvaWQge1xuICAgIHRoaXMuc2V0VmFsdWUodmFsdWUsIGZhbHNlKTtcbiAgICB0aGlzLmNkci5tYXJrRm9yQ2hlY2soKTtcbiAgfVxuXG4gIHJlZ2lzdGVyT25DaGFuZ2UoZm46IChfOiBudW1iZXIpID0+IHZvaWQpOiB2b2lkIHtcbiAgICB0aGlzLm9uQ2hhbmdlID0gZm47XG4gIH1cblxuICByZWdpc3Rlck9uVG91Y2hlZChmbjogKCkgPT4gdm9pZCk6IHZvaWQge1xuICAgIHRoaXMub25Ub3VjaGVkID0gZm47XG4gIH1cblxuICBzZXREaXNhYmxlZFN0YXRlKGlzRGlzYWJsZWQ6IGJvb2xlYW4pOiB2b2lkIHtcbiAgICB0aGlzLm56RGlzYWJsZWQgPSBpc0Rpc2FibGVkO1xuICAgIHRoaXMuY2RyLm1hcmtGb3JDaGVjaygpO1xuICB9XG5cbiAgZm9jdXMoKTogdm9pZCB7XG4gICAgdGhpcy5mb2N1c01vbml0b3IuZm9jdXNWaWEodGhpcy5pbnB1dEVsZW1lbnQsICdrZXlib2FyZCcpO1xuICB9XG5cbiAgYmx1cigpOiB2b2lkIHtcbiAgICB0aGlzLmlucHV0RWxlbWVudC5uYXRpdmVFbGVtZW50LmJsdXIoKTtcbiAgfVxuXG4gIGNvbnN0cnVjdG9yKFxuICAgIHByaXZhdGUgZWxlbWVudFJlZjogRWxlbWVudFJlZixcbiAgICBwcml2YXRlIHJlbmRlcmVyOiBSZW5kZXJlcjIsXG4gICAgcHJpdmF0ZSBjZHI6IENoYW5nZURldGVjdG9yUmVmLFxuICAgIHByaXZhdGUgZm9jdXNNb25pdG9yOiBGb2N1c01vbml0b3JcbiAgKSB7XG4gICAgcmVuZGVyZXIuYWRkQ2xhc3MoZWxlbWVudFJlZi5uYXRpdmVFbGVtZW50LCAnYW50LWlucHV0LW51bWJlcicpO1xuICB9XG5cbiAgbmdPbkluaXQoKTogdm9pZCB7XG4gICAgdGhpcy5mb2N1c01vbml0b3IubW9uaXRvcih0aGlzLmVsZW1lbnRSZWYsIHRydWUpLnN1YnNjcmliZShmb2N1c09yaWdpbiA9PiB7XG4gICAgICBpZiAoIWZvY3VzT3JpZ2luKSB7XG4gICAgICAgIHRoaXMubnpCbHVyLmVtaXQoKTtcbiAgICAgICAgUHJvbWlzZS5yZXNvbHZlKCkudGhlbigoKSA9PiB0aGlzLm9uVG91Y2hlZCgpKTtcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIHRoaXMubnpGb2N1cy5lbWl0KCk7XG4gICAgICB9XG4gICAgfSk7XG4gIH1cblxuICBuZ09uQ2hhbmdlcyhjaGFuZ2VzOiBTaW1wbGVDaGFuZ2VzKTogdm9pZCB7XG4gICAgaWYgKGNoYW5nZXMubnpBdXRvRm9jdXMpIHtcbiAgICAgIHRoaXMudXBkYXRlQXV0b0ZvY3VzKCk7XG4gICAgfVxuICAgIGlmIChjaGFuZ2VzLm56Rm9ybWF0dGVyKSB7XG4gICAgICBjb25zdCB2YWx1ZSA9IHRoaXMuZ2V0Q3VycmVudFZhbGlkVmFsdWUodGhpcy5hY3R1YWxWYWx1ZSk7XG4gICAgICB0aGlzLnNldFZhbHVlKHZhbHVlLCB0cnVlKTtcbiAgICB9XG4gIH1cblxuICBuZ0FmdGVyVmlld0luaXQoKTogdm9pZCB7XG4gICAgaWYgKHRoaXMubnpBdXRvRm9jdXMpIHtcbiAgICAgIHRoaXMuZm9jdXMoKTtcbiAgICB9XG4gIH1cblxuICBuZ09uRGVzdHJveSgpOiB2b2lkIHtcbiAgICB0aGlzLmZvY3VzTW9uaXRvci5zdG9wTW9uaXRvcmluZyh0aGlzLmVsZW1lbnRSZWYpO1xuICB9XG59XG4iXX0=