/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import * as tslib_1 from "tslib";
import { FocusMonitor } from '@angular/cdk/a11y';
import { forwardRef, ChangeDetectionStrategy, ChangeDetectorRef, Component, ElementRef, HostListener, Input, Renderer2, ViewChild, ViewEncapsulation } from '@angular/core';
import { NG_VALUE_ACCESSOR } from '@angular/forms';
import { Subject } from 'rxjs';
import { InputBoolean } from '../core/util/convert';
export class NzRadioComponent {
    /* tslint:disable-next-line:no-any */
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
        this.select$ = new Subject();
        this.touched$ = new Subject();
        this.checked = false;
        this.isNgModel = false;
        this.onChange = (/**
         * @return {?}
         */
        () => null);
        this.onTouched = (/**
         * @return {?}
         */
        () => null);
        this.nzDisabled = false;
        this.nzAutoFocus = false;
        this.renderer.addClass(elementRef.nativeElement, 'ant-radio-wrapper');
    }
    /**
     * @return {?}
     */
    updateAutoFocus() {
        if (this.inputElement) {
            if (this.nzAutoFocus) {
                this.renderer.setAttribute(this.inputElement.nativeElement, 'autofocus', 'autofocus');
            }
            else {
                this.renderer.removeAttribute(this.inputElement.nativeElement, 'autofocus');
            }
        }
    }
    /**
     * @param {?} event
     * @return {?}
     */
    onClick(event) {
        // Prevent label click triggered twice.
        event.stopPropagation();
        event.preventDefault();
        if (!this.nzDisabled && !this.checked) {
            this.select$.next(this);
            if (this.isNgModel) {
                this.checked = true;
                this.onChange(true);
            }
        }
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
    markForCheck() {
        this.cdr.markForCheck();
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
     * @param {?} value
     * @return {?}
     */
    writeValue(value) {
        this.checked = value;
        this.cdr.markForCheck();
    }
    /**
     * @param {?} fn
     * @return {?}
     */
    registerOnChange(fn) {
        this.isNgModel = true;
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
     * @return {?}
     */
    ngAfterViewInit() {
        this.focusMonitor.monitor(this.elementRef, true).subscribe((/**
         * @param {?} focusOrigin
         * @return {?}
         */
        focusOrigin => {
            if (!focusOrigin) {
                Promise.resolve().then((/**
                 * @return {?}
                 */
                () => this.onTouched()));
                this.touched$.next();
            }
        }));
        this.updateAutoFocus();
    }
    /**
     * @param {?} changes
     * @return {?}
     */
    ngOnChanges(changes) {
        if (changes.nzAutoFocus) {
            this.updateAutoFocus();
        }
    }
}
NzRadioComponent.decorators = [
    { type: Component, args: [{
                selector: '[nz-radio]',
                preserveWhitespaces: false,
                template: "<span class=\"ant-radio\" [class.ant-radio-checked]=\"checked\" [class.ant-radio-disabled]=\"nzDisabled\">\n  <input #inputElement type=\"radio\" class=\"ant-radio-input\" [disabled]=\"nzDisabled\" [checked]=\"checked\" [attr.name]=\"name\">\n  <span class=\"ant-radio-inner\"></span>\n</span>\n<span><ng-content></ng-content></span>",
                encapsulation: ViewEncapsulation.None,
                changeDetection: ChangeDetectionStrategy.OnPush,
                providers: [
                    {
                        provide: NG_VALUE_ACCESSOR,
                        useExisting: forwardRef((/**
                         * @return {?}
                         */
                        () => NzRadioComponent)),
                        multi: true
                    }
                ],
                host: {
                    '[class.ant-radio-wrapper-checked]': 'checked',
                    '[class.ant-radio-wrapper-disabled]': 'nzDisabled'
                }
            }] }
];
/** @nocollapse */
NzRadioComponent.ctorParameters = () => [
    { type: ElementRef },
    { type: Renderer2 },
    { type: ChangeDetectorRef },
    { type: FocusMonitor }
];
NzRadioComponent.propDecorators = {
    inputElement: [{ type: ViewChild, args: ['inputElement',] }],
    nzValue: [{ type: Input }],
    nzDisabled: [{ type: Input }],
    nzAutoFocus: [{ type: Input }],
    onClick: [{ type: HostListener, args: ['click', ['$event'],] }]
};
tslib_1.__decorate([
    InputBoolean(),
    tslib_1.__metadata("design:type", Object)
], NzRadioComponent.prototype, "nzDisabled", void 0);
tslib_1.__decorate([
    InputBoolean(),
    tslib_1.__metadata("design:type", Object)
], NzRadioComponent.prototype, "nzAutoFocus", void 0);
if (false) {
    /** @type {?} */
    NzRadioComponent.prototype.select$;
    /** @type {?} */
    NzRadioComponent.prototype.touched$;
    /** @type {?} */
    NzRadioComponent.prototype.checked;
    /** @type {?} */
    NzRadioComponent.prototype.name;
    /** @type {?} */
    NzRadioComponent.prototype.isNgModel;
    /** @type {?} */
    NzRadioComponent.prototype.onChange;
    /** @type {?} */
    NzRadioComponent.prototype.onTouched;
    /** @type {?} */
    NzRadioComponent.prototype.inputElement;
    /** @type {?} */
    NzRadioComponent.prototype.nzValue;
    /** @type {?} */
    NzRadioComponent.prototype.nzDisabled;
    /** @type {?} */
    NzRadioComponent.prototype.nzAutoFocus;
    /**
     * @type {?}
     * @private
     */
    NzRadioComponent.prototype.elementRef;
    /**
     * @type {?}
     * @private
     */
    NzRadioComponent.prototype.renderer;
    /**
     * @type {?}
     * @private
     */
    NzRadioComponent.prototype.cdr;
    /**
     * @type {?}
     * @private
     */
    NzRadioComponent.prototype.focusMonitor;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotcmFkaW8uY29tcG9uZW50LmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbInJhZGlvL256LXJhZGlvLmNvbXBvbmVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7OztBQUFBLE9BQU8sRUFBRSxZQUFZLEVBQUUsTUFBTSxtQkFBbUIsQ0FBQztBQUNqRCxPQUFPLEVBQ0wsVUFBVSxFQUVWLHVCQUF1QixFQUN2QixpQkFBaUIsRUFDakIsU0FBUyxFQUNULFVBQVUsRUFDVixZQUFZLEVBQ1osS0FBSyxFQUVMLFNBQVMsRUFFVCxTQUFTLEVBQ1QsaUJBQWlCLEVBQ2xCLE1BQU0sZUFBZSxDQUFDO0FBQ3ZCLE9BQU8sRUFBd0IsaUJBQWlCLEVBQUUsTUFBTSxnQkFBZ0IsQ0FBQztBQUN6RSxPQUFPLEVBQUUsT0FBTyxFQUFFLE1BQU0sTUFBTSxDQUFDO0FBQy9CLE9BQU8sRUFBRSxZQUFZLEVBQUUsTUFBTSxzQkFBc0IsQ0FBQztBQW9CcEQsTUFBTSxPQUFPLGdCQUFnQjs7Ozs7Ozs7SUFtRDNCLFlBQ1UsVUFBc0IsRUFDdEIsUUFBbUIsRUFDbkIsR0FBc0IsRUFDdEIsWUFBMEI7UUFIMUIsZUFBVSxHQUFWLFVBQVUsQ0FBWTtRQUN0QixhQUFRLEdBQVIsUUFBUSxDQUFXO1FBQ25CLFFBQUcsR0FBSCxHQUFHLENBQW1CO1FBQ3RCLGlCQUFZLEdBQVosWUFBWSxDQUFjO1FBdERwQyxZQUFPLEdBQUcsSUFBSSxPQUFPLEVBQW9CLENBQUM7UUFDMUMsYUFBUSxHQUFHLElBQUksT0FBTyxFQUFRLENBQUM7UUFDL0IsWUFBTyxHQUFHLEtBQUssQ0FBQztRQUVoQixjQUFTLEdBQUcsS0FBSyxDQUFDO1FBQ2xCLGFBQVE7OztRQUF5QixHQUFHLEVBQUUsQ0FBQyxJQUFJLEVBQUM7UUFDNUMsY0FBUzs7O1FBQWUsR0FBRyxFQUFFLENBQUMsSUFBSSxFQUFDO1FBSVYsZUFBVSxHQUFHLEtBQUssQ0FBQztRQUNuQixnQkFB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SCxDQUFDOzs7WUEvR0YsU0FBUyxTQUFDO2dCQUNULFFBQVEsRUFBRSxZQUFZO2dCQUN0QixtQkFBbUIsRUFBRSxLQUFLO2dCQUMxQix5VkFBd0M7Z0JBQ3hDLGFBQWEsRUFBRSxpQkFBaUIsQ0FBQyxJQUFJO2dCQUNyQyxlQUFlLEVBQUUsdUJBQXVCLENBQUMsTUFBTTtnQkFDL0MsU0FBUyxFQUFFO29CQUNUO3dCQUNFLE9BQU8sRUFBRSxpQkFBaUI7d0JBQzFCLFdBQVcsRUFBRSxVQUFVOzs7d0JBQUMsR0FBRyxFQUFFLENBQUMsZ0JBQWdCLEVBQUM7d0JBQy9DLEtBQUssRUFBRSxJQUFJO3FCQUNaO2lCQUNGO2dCQUNELElBQUksRUFBRTtvQkFDSixtQ0FBbUMsRUFBRSxTQUFTO29CQUM5QyxvQ0FBb0MsRUFBRSxZQUFZO2lCQUNuRDthQUNGOzs7O1lBOUJDLFVBQVU7WUFJVixTQUFTO1lBTlQsaUJBQWlCO1lBTFYsWUFBWTs7OzJCQThDbEIsU0FBUyxTQUFDLGNBQWM7c0JBRXhCLEtBQUs7eUJBQ0wsS0FBSzswQkFDTCxLQUFLO3NCQVlMLFlBQVksU0FBQyxPQUFPLEVBQUUsQ0FBQyxRQUFRLENBQUM7O0FBYlI7SUFBZixZQUFZLEVBQUU7O29EQUFvQjtBQUNuQjtJQUFmLFlBQVksRUFBRTs7cURBQXFCOzs7SUFYN0MsbUNBQTBDOztJQUMxQyxvQ0FBK0I7O0lBQy9CLG1DQUFnQjs7SUFDaEIsZ0NBQWE7O0lBQ2IscUNBQWtCOztJQUNsQixvQ0FBNEM7O0lBQzVDLHFDQUFtQzs7SUFDbkMsd0NBQW9EOztJQUVwRCxtQ0FBc0I7O0lBQ3RCLHNDQUE0Qzs7SUFDNUMsdUNBQTZDOzs7OztJQXdDM0Msc0NBQThCOzs7OztJQUM5QixvQ0FBMkI7Ozs7O0lBQzNCLCtCQUE4Qjs7Ozs7SUFDOUIsd0NBQWtDIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHsgRm9jdXNNb25pdG9yIH0gZnJvbSAnQGFuZ3VsYXIvY2RrL2ExMXknO1xuaW1wb3J0IHtcbiAgZm9yd2FyZFJlZixcbiAgQWZ0ZXJWaWV3SW5pdCxcbiAgQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3ksXG4gIENoYW5nZURldGVjdG9yUmVmLFxuICBDb21wb25lbnQsXG4gIEVsZW1lbnRSZWYsXG4gIEhvc3RMaXN0ZW5lcixcbiAgSW5wdXQsXG4gIE9uQ2hhbmdlcyxcbiAgUmVuZGVyZXIyLFxuICBTaW1wbGVDaGFuZ2VzLFxuICBWaWV3Q2hpbGQsXG4gIFZpZXdFbmNhcHN1bGF0aW9uXG59IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuaW1wb3J0IHsgQ29udHJvbFZhbHVlQWNjZXNzb3IsIE5HX1ZBTFVFX0FDQ0VTU09SIH0gZnJvbSAnQGFuZ3VsYXIvZm9ybXMnO1xuaW1wb3J0IHsgU3ViamVjdCB9IGZyb20gJ3J4anMnO1xuaW1wb3J0IHsgSW5wdXRCb29sZWFuIH0gZnJvbSAnLi4vY29yZS91dGlsL2NvbnZlcnQnO1xuXG5AQ29tcG9uZW50KHtcbiAgc2VsZWN0b3I6ICdbbnotcmFkaW9dJyxcbiAgcHJlc2VydmVXaGl0ZXNwYWNlczogZmFsc2UsXG4gIHRlbXBsYXRlVXJsOiAnLi9uei1yYWRpby5jb21wb25lbnQuaHRtbCcsXG4gIGVuY2Fwc3VsYXRpb246IFZpZXdFbmNhcHN1bGF0aW9uLk5vbmUsXG4gIGNoYW5nZURldGVjdGlvbjogQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3kuT25QdXNoLFxuICBwcm92aWRlcnM6IFtcbiAgICB7XG4gICAgICBwcm92aWRlOiBOR19WQUxVRV9BQ0NFU1NPUixcbiAgICAgIHVzZUV4aXN0aW5nOiBmb3J3YXJkUmVmKCgpID0+IE56UmFkaW9Db21wb25lbnQpLFxuICAgICAgbXVsdGk6IHRydWVcbiAgICB9XG4gIF0sXG4gIGhvc3Q6IHtcbiAgICAnW2NsYXNzLmFudC1yYWRpby13cmFwcGVyLWNoZWNrZWRdJzogJ2NoZWNrZWQnLFxuICAgICdbY2xhc3MuYW50LXJhZGlvLXdyYXBwZXItZGlzYWJsZWRdJzogJ256RGlzYWJsZWQnXG4gIH1cbn0pXG5leHBvcnQgY2xhc3MgTnpSYWRpb0NvbXBvbmVudCBpbXBsZW1lbnRzIENvbnRyb2xWYWx1ZUFjY2Vzc29yLCBBZnRlclZpZXdJbml0LCBPbkNoYW5nZXMge1xuICBzZWxlY3QkID0gbmV3IFN1YmplY3Q8TnpSYWRpb0NvbXBvbmVudD4oKTtcbiAgdG91Y2hlZCQgPSBuZXcgU3ViamVjdDx2b2lkPigpO1xuICBjaGVja2VkID0gZmFsc2U7XG4gIG5hbWU6IHN0cmluZztcbiAgaXNOZ01vZGVsID0gZmFsc2U7XG4gIG9uQ2hhbmdlOiAoXzogYm9vbGVhbikgPT4gdm9pZCA9ICgpID0+IG51bGw7XG4gIG9uVG91Y2hlZDogKCkgPT4gdm9pZCA9ICgpID0+IG51bGw7XG4gIEBWaWV3Q2hpbGQoJ2lucHV0RWxlbWVudCcpIGlucHV0RWxlbWVudDogRWxlbWVudFJlZjtcbiAgLyogdHNsaW50OmRpc2FibGUtbmV4dC1saW5lOm5vLWFueSAqL1xuICBASW5wdXQoKSBuelZhbHVlOiBhbnk7XG4gIEBJbnB1dCgpIEBJbnB1dEJvb2xlYW4oKSBuekRpc2FibGVkID0gZmFsc2U7XG4gIEBJbnB1dCgpIEBJbnB1dEJvb2xlYW4oKSBuekF1dG9Gb2N1cyA9IGZhbHNlO1xuXG4gIHVwZGF0ZUF1dG9Gb2N1cygpOiB2b2lkIHtcbiAgICBpZiAodGhpcy5pbnB1dEVsZW1lbnQpIHtcbiAgICAgIGlmICh0aGlzLm56QXV0b0ZvY3VzKSB7XG4gICAgICAgIHRoaXMucmVuZGVyZXIuc2V0QXR0cmlidXRlKHRoaXMuaW5wdXRFbGVtZW50Lm5hdGl2ZUVsZW1lbnQsICdhdXRvZm9jdXMnLCAnYXV0b2ZvY3VzJyk7XG4gICAgICB9IGVsc2Uge1xuICAgICAgICB0aGlzLnJlbmRlcmVyLnJlbW92ZUF0dHJpYnV0ZSh0aGlzLmlucHV0RWxlbWVudC5uYXRpdmVFbGVtZW50LCAnYXV0b2ZvY3VzJyk7XG4gICAgICB9XG4gICAgfVxuICB9XG5cbiAgQEhvc3RMaXN0ZW5lcignY2xpY2snLCBbJyRldmVudCddKVxuICBvbkNsaWNrKGV2ZW50OiBNb3VzZUV2ZW50KTogdm9pZCB7XG4gICAgLy8gUHJldmVudCBsYWJlbCBjbGljayB0cmlnZ2VyZWQgdHdpY2UuXG4gICAgZXZlbnQuc3RvcFByb3BhZ2F0aW9uKCk7XG4gICAgZXZlbnQucHJldmVudERlZmF1bHQoKTtcbiAgICBpZiAoIXRoaXMubnpEaXNhYmxlZCAmJiAhdGhpcy5jaGVja2VkKSB7XG4gICAgICB0aGlzLnNlbGVjdCQubmV4dCh0aGlzKTtcbiAgICAgIGlmICh0aGlzLmlzTmdNb2RlbCkge1xuICAgICAgICB0aGlzLmNoZWNrZWQgPSB0cnVlO1xuICAgICAgICB0aGlzLm9uQ2hhbmdlKHRydWUpO1xuICAgICAgfVxuICAgIH1cbiAgfVxuXG4gIGZvY3VzKCk6IHZvaWQge1xuICAgIHRoaXMuZm9jdXNNb25pdG9yLmZvY3VzVmlhKHRoaXMuaW5wdXRFbGVtZW50LCAna2V5Ym9hcmQnKTtcbiAgfVxuXG4gIGJsdXIoKTogdm9pZCB7XG4gICAgdGhpcy5pbnB1dEVsZW1lbnQubmF0aXZlRWxlbWVudC5ibHVyKCk7XG4gIH1cblxuICBtYXJrRm9yQ2hlY2soKTogdm9pZCB7XG4gICAgdGhpcy5jZHIubWFya0ZvckNoZWNrKCk7XG4gIH1cblxuICAvKiB0c2xpbnQ6ZGlzYWJsZS1uZXh0LWxpbmU6bm8tYW55ICovXG4gIGNvbnN0cnVjdG9yKFxuICAgIHByaXZhdGUgZWxlbWVudFJlZjogRWxlbWVudFJlZixcbiAgICBwcml2YXRlIHJlbmRlcmVyOiBSZW5kZXJlcjIsXG4gICAgcHJpdmF0ZSBjZHI6IENoYW5nZURldGVjdG9yUmVmLFxuICAgIHByaXZhdGUgZm9jdXNNb25pdG9yOiBGb2N1c01vbml0b3JcbiAgKSB7XG4gICAgdGhpcy5yZW5kZXJlci5hZGRDbGFzcyhlbGVtZW50UmVmLm5hdGl2ZUVsZW1lbnQsICdhbnQtcmFkaW8td3JhcHBlcicpO1xuICB9XG5cbiAgc2V0RGlzYWJsZWRTdGF0ZShpc0Rpc2FibGVkOiBib29sZWFuKTogdm9pZCB7XG4gICAgdGhpcy5uekRpc2FibGVkID0gaXNEaXNhYmxlZDtcbiAgICB0aGlzLmNkci5tYXJrRm9yQ2hlY2soKTtcbiAgfVxuXG4gIHdyaXRlVmFsdWUodmFsdWU6IGJvb2xlYW4pOiB2b2lkIHtcbiAgICB0aGlzLmNoZWNrZWQgPSB2YWx1ZTtcbiAgICB0aGlzLmNkci5tYXJrRm9yQ2hlY2soKTtcbiAgfVxuXG4gIHJlZ2lzdGVyT25DaGFuZ2UoZm46IChfOiBib29sZWFuKSA9PiB7fSk6IHZvaWQge1xuICAgIHRoaXMuaXNOZ01vZGVsID0gdHJ1ZTtcbiAgICB0aGlzLm9uQ2hhbmdlID0gZm47XG4gIH1cblxuICByZWdpc3Rlck9uVG91Y2hlZChmbjogKCkgPT4ge30pOiB2b2lkIHtcbiAgICB0aGlzLm9uVG91Y2hlZCA9IGZuO1xuICB9XG5cbiAgbmdBZnRlclZpZXdJbml0KCk6IHZvaWQge1xuICAgIHRoaXMuZm9jdXNNb25pdG9yLm1vbml0b3IodGhpcy5lbGVtZW50UmVmLCB0cnVlKS5zdWJzY3JpYmUoZm9jdXNPcmlnaW4gPT4ge1xuICAgICAgaWYgKCFmb2N1c09yaWdpbikge1xuICAgICAgICBQcm9taXNlLnJlc29sdmUoKS50aGVuKCgpID0+IHRoaXMub25Ub3VjaGVkKCkpO1xuICAgICAgICB0aGlzLnRvdWNoZWQkLm5leHQoKTtcbiAgICAgIH1cbiAgICB9KTtcbiAgICB0aGlzLnVwZGF0ZUF1dG9Gb2N1cygpO1xuICB9XG5cbiAgbmdPbkNoYW5nZXMoY2hhbmdlczogU2ltcGxlQ2hhbmdlcyk6IHZvaWQge1xuICAgIGlmIChjaGFuZ2VzLm56QXV0b0ZvY3VzKSB7XG4gICAgICB0aGlzLnVwZGF0ZUF1dG9Gb2N1cygpO1xuICAgIH1cbiAgfVxufVxuIl19