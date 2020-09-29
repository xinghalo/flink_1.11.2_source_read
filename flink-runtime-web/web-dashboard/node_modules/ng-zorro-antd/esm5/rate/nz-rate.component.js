/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import * as tslib_1 from "tslib";
import { LEFT_ARROW, RIGHT_ARROW } from '@angular/cdk/keycodes';
import { forwardRef, ChangeDetectionStrategy, ChangeDetectorRef, Component, ElementRef, EventEmitter, Input, Output, Renderer2, TemplateRef, ViewChild, ViewEncapsulation } from '@angular/core';
import { NG_VALUE_ACCESSOR } from '@angular/forms';
import { InputBoolean } from '../core/util/convert';
var NzRateComponent = /** @class */ (function () {
    function NzRateComponent(renderer, cdr) {
        this.renderer = renderer;
        this.cdr = cdr;
        this.nzAllowClear = true;
        this.nzAllowHalf = false;
        this.nzDisabled = false;
        this.nzAutoFocus = false;
        this.nzTooltips = [];
        this.nzOnBlur = new EventEmitter();
        this.nzOnFocus = new EventEmitter();
        this.nzOnHoverChange = new EventEmitter();
        this.nzOnKeyDown = new EventEmitter();
        this.hasHalf = false;
        this.hoverValue = 0;
        this.prefixCls = 'ant-rate';
        this.innerPrefixCls = this.prefixCls + "-star";
        this.isFocused = false;
        this.isInit = false;
        this.starArray = [];
        this._count = 5;
        this._value = 0;
        this.onChange = (/**
         * @return {?}
         */
        function () { return null; });
        this.onTouched = (/**
         * @return {?}
         */
        function () { return null; });
    }
    Object.defineProperty(NzRateComponent.prototype, "nzCount", {
        get: /**
         * @return {?}
         */
        function () {
            return this._count;
        },
        set: /**
         * @param {?} value
         * @return {?}
         */
        function (value) {
            if (this._count === value) {
                return;
            }
            this._count = value;
            this.updateStarArray();
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzRateComponent.prototype, "nzValue", {
        get: /**
         * @return {?}
         */
        function () {
            return this._value;
        },
        set: /**
         * @param {?} input
         * @return {?}
         */
        function (input) {
            if (this._value === input) {
                return;
            }
            this._value = input;
            this.hasHalf = !Number.isInteger(input);
            this.hoverValue = Math.ceil(input);
        },
        enumerable: true,
        configurable: true
    });
    /**
     * @param {?} changes
     * @return {?}
     */
    NzRateComponent.prototype.ngOnChanges = /**
     * @param {?} changes
     * @return {?}
     */
    function (changes) {
        if (changes.nzAutoFocus && !changes.nzAutoFocus.isFirstChange()) {
            if (this.nzAutoFocus && !this.nzDisabled) {
                this.renderer.setAttribute(this.ulElement.nativeElement, 'autofocus', 'autofocus');
            }
            else {
                this.renderer.removeAttribute(this.ulElement.nativeElement, 'autofocus');
            }
        }
    };
    /**
     * @return {?}
     */
    NzRateComponent.prototype.ngOnInit = /**
     * @return {?}
     */
    function () {
        this.updateStarArray();
    };
    /**
     * @return {?}
     */
    NzRateComponent.prototype.ngAfterViewInit = /**
     * @return {?}
     */
    function () {
        this.isInit = true;
    };
    /**
     * @param {?} index
     * @param {?} isHalf
     * @return {?}
     */
    NzRateComponent.prototype.onItemClick = /**
     * @param {?} index
     * @param {?} isHalf
     * @return {?}
     */
    function (index, isHalf) {
        if (this.nzDisabled) {
            return;
        }
        this.hoverValue = index + 1;
        /** @type {?} */
        var actualValue = isHalf ? index + 0.5 : index + 1;
        if (this.nzValue === actualValue) {
            if (this.nzAllowClear) {
                this.nzValue = 0;
                this.onChange(this.nzValue);
            }
        }
        else {
            this.nzValue = actualValue;
            this.onChange(this.nzValue);
        }
    };
    /**
     * @param {?} index
     * @param {?} isHalf
     * @return {?}
     */
    NzRateComponent.prototype.onItemHover = /**
     * @param {?} index
     * @param {?} isHalf
     * @return {?}
     */
    function (index, isHalf) {
        if (this.nzDisabled || (this.hoverValue === index + 1 && isHalf === this.hasHalf)) {
            return;
        }
        this.hoverValue = index + 1;
        this.hasHalf = isHalf;
        this.nzOnHoverChange.emit(this.hoverValue);
    };
    /**
     * @return {?}
     */
    NzRateComponent.prototype.onRateLeave = /**
     * @return {?}
     */
    function () {
        this.hasHalf = !Number.isInteger(this.nzValue);
        this.hoverValue = Math.ceil(this.nzValue);
    };
    /**
     * @param {?} e
     * @return {?}
     */
    NzRateComponent.prototype.onFocus = /**
     * @param {?} e
     * @return {?}
     */
    function (e) {
        this.isFocused = true;
        this.nzOnFocus.emit(e);
    };
    /**
     * @param {?} e
     * @return {?}
     */
    NzRateComponent.prototype.onBlur = /**
     * @param {?} e
     * @return {?}
     */
    function (e) {
        this.isFocused = false;
        this.nzOnBlur.emit(e);
    };
    /**
     * @return {?}
     */
    NzRateComponent.prototype.focus = /**
     * @return {?}
     */
    function () {
        this.ulElement.nativeElement.focus();
    };
    /**
     * @return {?}
     */
    NzRateComponent.prototype.blur = /**
     * @return {?}
     */
    function () {
        this.ulElement.nativeElement.blur();
    };
    /**
     * @param {?} e
     * @return {?}
     */
    NzRateComponent.prototype.onKeyDown = /**
     * @param {?} e
     * @return {?}
     */
    function (e) {
        /** @type {?} */
        var oldVal = this.nzValue;
        if (e.keyCode === RIGHT_ARROW && this.nzValue < this.nzCount) {
            this.nzValue += this.nzAllowHalf ? 0.5 : 1;
        }
        else if (e.keyCode === LEFT_ARROW && this.nzValue > 0) {
            this.nzValue -= this.nzAllowHalf ? 0.5 : 1;
        }
        if (oldVal !== this.nzValue) {
            this.onChange(this.nzValue);
            this.nzOnKeyDown.emit(e);
            this.cdr.markForCheck();
        }
    };
    /**
     * @param {?} i
     * @return {?}
     */
    NzRateComponent.prototype.setClasses = /**
     * @param {?} i
     * @return {?}
     */
    function (i) {
        var _a;
        return _a = {},
            _a[this.innerPrefixCls + "-full"] = i + 1 < this.hoverValue || (!this.hasHalf && i + 1 === this.hoverValue),
            _a[this.innerPrefixCls + "-half"] = this.hasHalf && i + 1 === this.hoverValue,
            _a[this.innerPrefixCls + "-active"] = this.hasHalf && i + 1 === this.hoverValue,
            _a[this.innerPrefixCls + "-zero"] = i + 1 > this.hoverValue,
            _a[this.innerPrefixCls + "-focused"] = this.hasHalf && i + 1 === this.hoverValue && this.isFocused,
            _a;
    };
    /**
     * @private
     * @return {?}
     */
    NzRateComponent.prototype.updateStarArray = /**
     * @private
     * @return {?}
     */
    function () {
        this.starArray = Array(this.nzCount)
            .fill(0)
            .map((/**
         * @param {?} _
         * @param {?} i
         * @return {?}
         */
        function (_, i) { return i; }));
    };
    // #region Implement `ControlValueAccessor`
    // #region Implement `ControlValueAccessor`
    /**
     * @param {?} value
     * @return {?}
     */
    NzRateComponent.prototype.writeValue = 
    // #region Implement `ControlValueAccessor`
    /**
     * @param {?} value
     * @return {?}
     */
    function (value) {
        this.nzValue = value || 0;
        this.cdr.markForCheck();
    };
    /**
     * @param {?} isDisabled
     * @return {?}
     */
    NzRateComponent.prototype.setDisabledState = /**
     * @param {?} isDisabled
     * @return {?}
     */
    function (isDisabled) {
        this.nzDisabled = isDisabled;
    };
    /**
     * @param {?} fn
     * @return {?}
     */
    NzRateComponent.prototype.registerOnChange = /**
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
    NzRateComponent.prototype.registerOnTouched = /**
     * @param {?} fn
     * @return {?}
     */
    function (fn) {
        this.onTouched = fn;
    };
    NzRateComponent.decorators = [
        { type: Component, args: [{
                    changeDetection: ChangeDetectionStrategy.OnPush,
                    encapsulation: ViewEncapsulation.None,
                    selector: 'nz-rate',
                    preserveWhitespaces: false,
                    template: "<ul #ulElement\n  class=\"ant-rate\"\n  [class.ant-rate-disabled]=\"nzDisabled\"\n  [ngClass]=\"classMap\"\n  (blur)=\"onBlur($event)\"\n  (focus)=\"onFocus($event)\"\n  (keydown)=\"onKeyDown($event); $event.preventDefault();\"\n  (mouseleave)=\"onRateLeave(); $event.stopPropagation();\"\n  [tabindex]=\"nzDisabled ? -1 : 1\">\n  <li *ngFor=\"let star of starArray; let i = index\"\n    class=\"ant-rate-star\"\n    [ngClass]=\"setClasses(star)\"\n    nz-tooltip\n    [nzTitle]=\"nzTooltips[ i ]\">\n    <div nz-rate-item\n      [allowHalf]=\"nzAllowHalf\"\n      [character]=\"nzCharacter\"\n      (itemHover)=\"onItemHover(i, $event)\"\n      (itemClick)=\"onItemClick(i, $event)\">\n    </div>\n  </li>\n</ul>\n",
                    providers: [
                        {
                            provide: NG_VALUE_ACCESSOR,
                            useExisting: forwardRef((/**
                             * @return {?}
                             */
                            function () { return NzRateComponent; })),
                            multi: true
                        }
                    ]
                }] }
    ];
    /** @nocollapse */
    NzRateComponent.ctorParameters = function () { return [
        { type: Renderer2 },
        { type: ChangeDetectorRef }
    ]; };
    NzRateComponent.propDecorators = {
        ulElement: [{ type: ViewChild, args: ['ulElement',] }],
        nzAllowClear: [{ type: Input }],
        nzAllowHalf: [{ type: Input }],
        nzDisabled: [{ type: Input }],
        nzAutoFocus: [{ type: Input }],
        nzCharacter: [{ type: Input }],
        nzTooltips: [{ type: Input }],
        nzOnBlur: [{ type: Output }],
        nzOnFocus: [{ type: Output }],
        nzOnHoverChange: [{ type: Output }],
        nzOnKeyDown: [{ type: Output }],
        nzCount: [{ type: Input }]
    };
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Boolean)
    ], NzRateComponent.prototype, "nzAllowClear", void 0);
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Boolean)
    ], NzRateComponent.prototype, "nzAllowHalf", void 0);
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Boolean)
    ], NzRateComponent.prototype, "nzDisabled", void 0);
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Boolean)
    ], NzRateComponent.prototype, "nzAutoFocus", void 0);
    return NzRateComponent;
}());
export { NzRateComponent };
if (false) {
    /**
     * @type {?}
     * @private
     */
    NzRateComponent.prototype.ulElement;
    /** @type {?} */
    NzRateComponent.prototype.nzAllowClear;
    /** @type {?} */
    NzRateComponent.prototype.nzAllowHalf;
    /** @type {?} */
    NzRateComponent.prototype.nzDisabled;
    /** @type {?} */
    NzRateComponent.prototype.nzAutoFocus;
    /** @type {?} */
    NzRateComponent.prototype.nzCharacter;
    /** @type {?} */
    NzRateComponent.prototype.nzTooltips;
    /** @type {?} */
    NzRateComponent.prototype.nzOnBlur;
    /** @type {?} */
    NzRateComponent.prototype.nzOnFocus;
    /** @type {?} */
    NzRateComponent.prototype.nzOnHoverChange;
    /** @type {?} */
    NzRateComponent.prototype.nzOnKeyDown;
    /** @type {?} */
    NzRateComponent.prototype.classMap;
    /** @type {?} */
    NzRateComponent.prototype.hasHalf;
    /** @type {?} */
    NzRateComponent.prototype.hoverValue;
    /** @type {?} */
    NzRateComponent.prototype.prefixCls;
    /** @type {?} */
    NzRateComponent.prototype.innerPrefixCls;
    /** @type {?} */
    NzRateComponent.prototype.isFocused;
    /** @type {?} */
    NzRateComponent.prototype.isInit;
    /** @type {?} */
    NzRateComponent.prototype.starArray;
    /**
     * @type {?}
     * @private
     */
    NzRateComponent.prototype._count;
    /**
     * @type {?}
     * @private
     */
    NzRateComponent.prototype._value;
    /** @type {?} */
    NzRateComponent.prototype.onChange;
    /** @type {?} */
    NzRateComponent.prototype.onTouched;
    /**
     * @type {?}
     * @private
     */
    NzRateComponent.prototype.renderer;
    /**
     * @type {?}
     * @private
     */
    NzRateComponent.prototype.cdr;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotcmF0ZS5jb21wb25lbnQuanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsicmF0ZS9uei1yYXRlLmNvbXBvbmVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7OztBQUFBLE9BQU8sRUFBRSxVQUFVLEVBQUUsV0FBVyxFQUFFLE1BQU0sdUJBQXVCLENBQUM7QUFDaEUsT0FBTyxFQUNMLFVBQVUsRUFFVix1QkFBdUIsRUFDdkIsaUJBQWlCLEVBQ2pCLFNBQVMsRUFDVCxVQUFVLEVBQ1YsWUFBWSxFQUNaLEtBQUssRUFHTCxNQUFNLEVBQ04sU0FBUyxFQUVULFdBQVcsRUFDWCxTQUFTLEVBQ1QsaUJBQWlCLEVBQ2xCLE1BQU0sZUFBZSxDQUFDO0FBQ3ZCLE9BQU8sRUFBd0IsaUJBQWlCLEVBQUUsTUFBTSxnQkFBZ0IsQ0FBQztBQUd6RSxPQUFPLEVBQUUsWUFBWSxFQUFFLE1BQU0sc0JBQXNCLENBQUM7QUFFcEQ7SUFtRUUseUJBQW9CLFFBQW1CLEVBQVUsR0FBc0I7UUFBbkQsYUFBUSxHQUFSLFFBQVEsQ0FBVztRQUFVLFFBQUcsR0FBSCxHQUFHLENBQW1CO1FBbEQ5QyxpQkFBWSxHQUFZLElBQUksQ0FBQztRQUM3QixnQkFBVyxHQUFZLEtBQUssQ0FBQztRQUM3QixlQUFVLEdBQVksS0FBSyxDQUFDO1FBQzVCLGdCQUFXLEdBQVksS0FBSyxDQUFDO1FBRTdDLGVBQVUsR0FBYSxFQUFFLENBQUM7UUFDaEIsYUFBUSxHQUFHLElBQUksWUFBWSxFQUFjLENBQUM7UUFDMUMsY0FBUyxHQUFHLElBQUksWUFBWSxFQUFjLENBQUM7UUFDM0Msb0JBQWUsR0FBRyxJQUFJLFlBQVksRUFBVSxDQUFDO1FBQzdDLGdCQUFXLEdBQUcsSUFBSSxZQUFZLEVBQWlCLENBQUM7UUFHbkUsWUFBTyxHQUFHLEtBQUssQ0FBQztRQUNoQixlQUFVLEdBQUcsQ0FBQyxDQUFDO1FBQ2YsY0FBUyxHQUFHLFVBQVUsQ0FBQztRQUN2Qixt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RXpCLGlDQUFtQjs7Ozs7SUFDbkIsaUNBQW1COztJQXlKbkIsbUNBQStDOztJQUMvQyxvQ0FBbUM7Ozs7O0lBN0h2QixtQ0FBMkI7Ozs7O0lBQUUsOEJBQThCIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHsgTEVGVF9BUlJPVywgUklHSFRfQVJST1cgfSBmcm9tICdAYW5ndWxhci9jZGsva2V5Y29kZXMnO1xuaW1wb3J0IHtcbiAgZm9yd2FyZFJlZixcbiAgQWZ0ZXJWaWV3SW5pdCxcbiAgQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3ksXG4gIENoYW5nZURldGVjdG9yUmVmLFxuICBDb21wb25lbnQsXG4gIEVsZW1lbnRSZWYsXG4gIEV2ZW50RW1pdHRlcixcbiAgSW5wdXQsXG4gIE9uQ2hhbmdlcyxcbiAgT25Jbml0LFxuICBPdXRwdXQsXG4gIFJlbmRlcmVyMixcbiAgU2ltcGxlQ2hhbmdlcyxcbiAgVGVtcGxhdGVSZWYsXG4gIFZpZXdDaGlsZCxcbiAgVmlld0VuY2Fwc3VsYXRpb25cbn0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5pbXBvcnQgeyBDb250cm9sVmFsdWVBY2Nlc3NvciwgTkdfVkFMVUVfQUNDRVNTT1IgfSBmcm9tICdAYW5ndWxhci9mb3Jtcyc7XG5pbXBvcnQgeyBOZ0NsYXNzVHlwZSB9IGZyb20gJy4uL2NvcmUvdHlwZXMvbmctY2xhc3MnO1xuXG5pbXBvcnQgeyBJbnB1dEJvb2xlYW4gfSBmcm9tICcuLi9jb3JlL3V0aWwvY29udmVydCc7XG5cbkBDb21wb25lbnQoe1xuICBjaGFuZ2VEZXRlY3Rpb246IENoYW5nZURldGVjdGlvblN0cmF0ZWd5Lk9uUHVzaCxcbiAgZW5jYXBzdWxhdGlvbjogVmlld0VuY2Fwc3VsYXRpb24uTm9uZSxcbiAgc2VsZWN0b3I6ICduei1yYXRlJyxcbiAgcHJlc2VydmVXaGl0ZXNwYWNlczogZmFsc2UsXG4gIHRlbXBsYXRlVXJsOiAnLi9uei1yYXRlLmNvbXBvbmVudC5odG1sJyxcbiAgcHJvdmlkZXJzOiBbXG4gICAge1xuICAgICAgcHJvdmlkZTogTkdfVkFMVUVfQUNDRVNTT1IsXG4gICAgICB1c2VFeGlzdGluZzogZm9yd2FyZFJlZigoKSA9PiBOelJhdGVDb21wb25lbnQpLFxuICAgICAgbXVsdGk6IHRydWVcbiAgICB9XG4gIF1cbn0pXG5leHBvcnQgY2xhc3MgTnpSYXRlQ29tcG9uZW50IGltcGxlbWVudHMgT25Jbml0LCBDb250cm9sVmFsdWVBY2Nlc3NvciwgQWZ0ZXJWaWV3SW5pdCwgT25DaGFuZ2VzIHtcbiAgQFZpZXdDaGlsZCgndWxFbGVtZW50JykgcHJpdmF0ZSB1bEVsZW1lbnQ6IEVsZW1lbnRSZWY7XG5cbiAgQElucHV0KCkgQElucHV0Qm9vbGVhbigpIG56QWxsb3dDbGVhcjogYm9vbGVhbiA9IHRydWU7XG4gIEBJbnB1dCgpIEBJbnB1dEJvb2xlYW4oKSBuekFsbG93SGFsZjogYm9vbGVhbiA9IGZhbHNlO1xuICBASW5wdXQoKSBASW5wdXRCb29sZWFuKCkgbnpEaXNhYmxlZDogYm9vbGVhbiA9IGZhbHNlO1xuICBASW5wdXQoKSBASW5wdXRCb29sZWFuKCkgbnpBdXRvRm9jdXM6IGJvb2xlYW4gPSBmYWxzZTtcbiAgQElucHV0KCkgbnpDaGFyYWN0ZXI6IFRlbXBsYXRlUmVmPHZvaWQ+O1xuICBASW5wdXQoKSBuelRvb2x0aXBzOiBzdHJpbmdbXSA9IFtdO1xuICBAT3V0cHV0KCkgcmVhZG9ubHkgbnpPbkJsdXIgPSBuZXcgRXZlbnRFbWl0dGVyPEZvY3VzRXZlbnQ+KCk7XG4gIEBPdXRwdXQoKSByZWFkb25seSBuek9uRm9jdXMgPSBuZXcgRXZlbnRFbWl0dGVyPEZvY3VzRXZlbnQ+KCk7XG4gIEBPdXRwdXQoKSByZWFkb25seSBuek9uSG92ZXJDaGFuZ2UgPSBuZXcgRXZlbnRFbWl0dGVyPG51bWJlcj4oKTtcbiAgQE91dHB1dCgpIHJlYWRvbmx5IG56T25LZXlEb3duID0gbmV3IEV2ZW50RW1pdHRlcjxLZXlib2FyZEV2ZW50PigpO1xuXG4gIGNsYXNzTWFwOiBOZ0NsYXNzVHlwZTtcbiAgaGFzSGFsZiA9IGZhbHNlO1xuICBob3ZlclZhbHVlID0gMDtcbiAgcHJlZml4Q2xzID0gJ2FudC1yYXRlJztcbiAgaW5uZXJQcmVmaXhDbHMgPSBgJHt0aGlzLnByZWZpeENsc30tc3RhcmA7XG4gIGlzRm9jdXNlZCA9IGZhbHNlO1xuICBpc0luaXQgPSBmYWxzZTtcbiAgc3RhckFycmF5OiBudW1iZXJbXSA9IFtdO1xuXG4gIHByaXZhdGUgX2NvdW50ID0gNTtcbiAgcHJpdmF0ZSBfdmFsdWUgPSAwO1xuXG4gIEBJbnB1dCgpXG4gIHNldCBuekNvdW50KHZhbHVlOiBudW1iZXIpIHtcbiAgICBpZiAodGhpcy5fY291bnQgPT09IHZhbHVlKSB7XG4gICAgICByZXR1cm47XG4gICAgfVxuICAgIHRoaXMuX2NvdW50ID0gdmFsdWU7XG4gICAgdGhpcy51cGRhdGVTdGFyQXJyYXkoKTtcbiAgfVxuXG4gIGdldCBuekNvdW50KCk6IG51bWJlciB7XG4gICAgcmV0dXJuIHRoaXMuX2NvdW50O1xuICB9XG5cbiAgZ2V0IG56VmFsdWUoKTogbnVtYmVyIHtcbiAgICByZXR1cm4gdGhpcy5fdmFsdWU7XG4gIH1cblxuICBzZXQgbnpWYWx1ZShpbnB1dDogbnVtYmVyKSB7XG4gICAgaWYgKHRoaXMuX3ZhbHVlID09PSBpbnB1dCkge1xuICAgICAgcmV0dXJuO1xuICAgIH1cblxuICAgIHRoaXMuX3ZhbHVlID0gaW5wdXQ7XG4gICAgdGhpcy5oYXNIYWxmID0gIU51bWJlci5pc0ludGVnZXIoaW5wdXQpO1xuICAgIHRoaXMuaG92ZXJWYWx1ZSA9IE1hdGguY2VpbChpbnB1dCk7XG4gIH1cblxuICBjb25zdHJ1Y3Rvcihwcml2YXRlIHJlbmRlcmVyOiBSZW5kZXJlcjIsIHByaXZhdGUgY2RyOiBDaGFuZ2VEZXRlY3RvclJlZikge31cblxuICBuZ09uQ2hhbmdlcyhjaGFuZ2VzOiBTaW1wbGVDaGFuZ2VzKTogdm9pZCB7XG4gICAgaWYgKGNoYW5nZXMubnpBdXRvRm9jdXMgJiYgIWNoYW5nZXMubnpBdXRvRm9jdXMuaXNGaXJzdENoYW5nZSgpKSB7XG4gICAgICBpZiAodGhpcy5uekF1dG9Gb2N1cyAmJiAhdGhpcy5uekRpc2FibGVkKSB7XG4gICAgICAgIHRoaXMucmVuZGVyZXIuc2V0QXR0cmlidXRlKHRoaXMudWxFbGVtZW50Lm5hdGl2ZUVsZW1lbnQsICdhdXRvZm9jdXMnLCAnYXV0b2ZvY3VzJyk7XG4gICAgICB9IGVsc2Uge1xuICAgICAgICB0aGlzLnJlbmRlcmVyLnJlbW92ZUF0dHJpYnV0ZSh0aGlzLnVsRWxlbWVudC5uYXRpdmVFbGVtZW50LCAnYXV0b2ZvY3VzJyk7XG4gICAgICB9XG4gICAgfVxuICB9XG5cbiAgbmdPbkluaXQoKTogdm9pZCB7XG4gICAgdGhpcy51cGRhdGVTdGFyQXJyYXkoKTtcbiAgfVxuXG4gIG5nQWZ0ZXJWaWV3SW5pdCgpOiB2b2lkIHtcbiAgICB0aGlzLmlzSW5pdCA9IHRydWU7XG4gIH1cblxuICBvbkl0ZW1DbGljayhpbmRleDogbnVtYmVyLCBpc0hhbGY6IGJvb2xlYW4pOiB2b2lkIHtcbiAgICBpZiAodGhpcy5uekRpc2FibGVkKSB7XG4gICAgICByZXR1cm47XG4gICAgfVxuXG4gICAgdGhpcy5ob3ZlclZhbHVlID0gaW5kZXggKyAxO1xuXG4gICAgY29uc3QgYWN0dWFsVmFsdWUgPSBpc0hhbGYgPyBpbmRleCArIDAuNSA6IGluZGV4ICsgMTtcblxuICAgIGlmICh0aGlzLm56VmFsdWUgPT09IGFjdHVhbFZhbHVlKSB7XG4gICAgICBpZiAodGhpcy5uekFsbG93Q2xlYXIpIHtcbiAgICAgICAgdGhpcy5uelZhbHVlID0gMDtcbiAgICAgICAgdGhpcy5vbkNoYW5nZSh0aGlzLm56VmFsdWUpO1xuICAgICAgfVxuICAgIH0gZWxzZSB7XG4gICAgICB0aGlzLm56VmFsdWUgPSBhY3R1YWxWYWx1ZTtcbiAgICAgIHRoaXMub25DaGFuZ2UodGhpcy5uelZhbHVlKTtcbiAgICB9XG4gIH1cblxuICBvbkl0ZW1Ib3ZlcihpbmRleDogbnVtYmVyLCBpc0hhbGY6IGJvb2xlYW4pOiB2b2lkIHtcbiAgICBpZiAodGhpcy5uekRpc2FibGVkIHx8ICh0aGlzLmhvdmVyVmFsdWUgPT09IGluZGV4ICsgMSAmJiBpc0hhbGYgPT09IHRoaXMuaGFzSGFsZikpIHtcbiAgICAgIHJldHVybjtcbiAgICB9XG5cbiAgICB0aGlzLmhvdmVyVmFsdWUgPSBpbmRleCArIDE7XG4gICAgdGhpcy5oYXNIYWxmID0gaXNIYWxmO1xuICAgIHRoaXMubnpPbkhvdmVyQ2hhbmdlLmVtaXQodGhpcy5ob3ZlclZhbHVlKTtcbiAgfVxuXG4gIG9uUmF0ZUxlYXZlKCk6IHZvaWQge1xuICAgIHRoaXMuaGFzSGFsZiA9ICFOdW1iZXIuaXNJbnRlZ2VyKHRoaXMubnpWYWx1ZSk7XG4gICAgdGhpcy5ob3ZlclZhbHVlID0gTWF0aC5jZWlsKHRoaXMubnpWYWx1ZSk7XG4gIH1cblxuICBvbkZvY3VzKGU6IEZvY3VzRXZlbnQpOiB2b2lkIHtcbiAgICB0aGlzLmlzRm9jdXNlZCA9IHRydWU7XG4gICAgdGhpcy5uek9uRm9jdXMuZW1pdChlKTtcbiAgfVxuXG4gIG9uQmx1cihlOiBGb2N1c0V2ZW50KTogdm9pZCB7XG4gICAgdGhpcy5pc0ZvY3VzZWQgPSBmYWxzZTtcbiAgICB0aGlzLm56T25CbHVyLmVtaXQoZSk7XG4gIH1cblxuICBmb2N1cygpOiB2b2lkIHtcbiAgICB0aGlzLnVsRWxlbWVudC5uYXRpdmVFbGVtZW50LmZvY3VzKCk7XG4gIH1cblxuICBibHVyKCk6IHZvaWQge1xuICAgIHRoaXMudWxFbGVtZW50Lm5hdGl2ZUVsZW1lbnQuYmx1cigpO1xuICB9XG5cbiAgb25LZXlEb3duKGU6IEtleWJvYXJkRXZlbnQpOiB2b2lkIHtcbiAgICBjb25zdCBvbGRWYWwgPSB0aGlzLm56VmFsdWU7XG5cbiAgICBpZiAoZS5rZXlDb2RlID09PSBSSUdIVF9BUlJPVyAmJiB0aGlzLm56VmFsdWUgPCB0aGlzLm56Q291bnQpIHtcbiAgICAgIHRoaXMubnpWYWx1ZSArPSB0aGlzLm56QWxsb3dIYWxmID8gMC41IDogMTtcbiAgICB9IGVsc2UgaWYgKGUua2V5Q29kZSA9PT0gTEVGVF9BUlJPVyAmJiB0aGlzLm56VmFsdWUgPiAwKSB7XG4gICAgICB0aGlzLm56VmFsdWUgLT0gdGhpcy5uekFsbG93SGFsZiA/IDAuNSA6IDE7XG4gICAgfVxuXG4gICAgaWYgKG9sZFZhbCAhPT0gdGhpcy5uelZhbHVlKSB7XG4gICAgICB0aGlzLm9uQ2hhbmdlKHRoaXMubnpWYWx1ZSk7XG4gICAgICB0aGlzLm56T25LZXlEb3duLmVtaXQoZSk7XG4gICAgICB0aGlzLmNkci5tYXJrRm9yQ2hlY2soKTtcbiAgICB9XG4gIH1cblxuICBzZXRDbGFzc2VzKGk6IG51bWJlcik6IG9iamVjdCB7XG4gICAgcmV0dXJuIHtcbiAgICAgIFtgJHt0aGlzLmlubmVyUHJlZml4Q2xzfS1mdWxsYF06IGkgKyAxIDwgdGhpcy5ob3ZlclZhbHVlIHx8ICghdGhpcy5oYXNIYWxmICYmIGkgKyAxID09PSB0aGlzLmhvdmVyVmFsdWUpLFxuICAgICAgW2Ake3RoaXMuaW5uZXJQcmVmaXhDbHN9LWhhbGZgXTogdGhpcy5oYXNIYWxmICYmIGkgKyAxID09PSB0aGlzLmhvdmVyVmFsdWUsXG4gICAgICBbYCR7dGhpcy5pbm5lclByZWZpeENsc30tYWN0aXZlYF06IHRoaXMuaGFzSGFsZiAmJiBpICsgMSA9PT0gdGhpcy5ob3ZlclZhbHVlLFxuICAgICAgW2Ake3RoaXMuaW5uZXJQcmVmaXhDbHN9LXplcm9gXTogaSArIDEgPiB0aGlzLmhvdmVyVmFsdWUsXG4gICAgICBbYCR7dGhpcy5pbm5lclByZWZpeENsc30tZm9jdXNlZGBdOiB0aGlzLmhhc0hhbGYgJiYgaSArIDEgPT09IHRoaXMuaG92ZXJWYWx1ZSAmJiB0aGlzLmlzRm9jdXNlZFxuICAgIH07XG4gIH1cblxuICBwcml2YXRlIHVwZGF0ZVN0YXJBcnJheSgpOiB2b2lkIHtcbiAgICB0aGlzLnN0YXJBcnJheSA9IEFycmF5KHRoaXMubnpDb3VudClcbiAgICAgIC5maWxsKDApXG4gICAgICAubWFwKChfLCBpKSA9PiBpKTtcbiAgfVxuXG4gIC8vICNyZWdpb24gSW1wbGVtZW50IGBDb250cm9sVmFsdWVBY2Nlc3NvcmBcblxuICB3cml0ZVZhbHVlKHZhbHVlOiBudW1iZXIgfCBudWxsKTogdm9pZCB7XG4gICAgdGhpcy5uelZhbHVlID0gdmFsdWUgfHwgMDtcbiAgICB0aGlzLmNkci5tYXJrRm9yQ2hlY2soKTtcbiAgfVxuXG4gIHNldERpc2FibGVkU3RhdGUoaXNEaXNhYmxlZDogYm9vbGVhbik6IHZvaWQge1xuICAgIHRoaXMubnpEaXNhYmxlZCA9IGlzRGlzYWJsZWQ7XG4gIH1cblxuICByZWdpc3Rlck9uQ2hhbmdlKGZuOiAoXzogbnVtYmVyKSA9PiB2b2lkKTogdm9pZCB7XG4gICAgdGhpcy5vbkNoYW5nZSA9IGZuO1xuICB9XG5cbiAgcmVnaXN0ZXJPblRvdWNoZWQoZm46ICgpID0+IHZvaWQpOiB2b2lkIHtcbiAgICB0aGlzLm9uVG91Y2hlZCA9IGZuO1xuICB9XG5cbiAgb25DaGFuZ2U6ICh2YWx1ZTogbnVtYmVyKSA9PiB2b2lkID0gKCkgPT4gbnVsbDtcbiAgb25Ub3VjaGVkOiAoKSA9PiB2b2lkID0gKCkgPT4gbnVsbDtcblxuICAvLyAjZW5kcmVnaW9uXG59XG4iXX0=