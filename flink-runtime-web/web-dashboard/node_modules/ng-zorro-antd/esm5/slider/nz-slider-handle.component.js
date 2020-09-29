/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import * as tslib_1 from "tslib";
import { ChangeDetectionStrategy, ChangeDetectorRef, Component, Input, ViewChild, ViewEncapsulation } from '@angular/core';
import { Subscription } from 'rxjs';
import { InputBoolean } from '../core/util/convert';
import { NzToolTipComponent } from '../tooltip/nz-tooltip.component';
import { NzSliderComponent } from './nz-slider.component';
var NzSliderHandleComponent = /** @class */ (function () {
    function NzSliderHandleComponent(sliderComponent, cdr) {
        var _this = this;
        this.sliderComponent = sliderComponent;
        this.cdr = cdr;
        this.nzTooltipVisible = 'default';
        this.nzActive = false;
        this.style = {};
        this.hovers_ = new Subscription();
        this.enterHandle = (/**
         * @return {?}
         */
        function () {
            if (!_this.sliderComponent.isDragging) {
                _this.toggleTooltip(true);
                _this.updateTooltipPosition();
                _this.cdr.detectChanges();
            }
        });
        this.leaveHandle = (/**
         * @return {?}
         */
        function () {
            if (!_this.sliderComponent.isDragging) {
                _this.toggleTooltip(false);
                _this.cdr.detectChanges();
            }
        });
    }
    /**
     * @param {?} changes
     * @return {?}
     */
    NzSliderHandleComponent.prototype.ngOnChanges = /**
     * @param {?} changes
     * @return {?}
     */
    function (changes) {
        var _this = this;
        var nzOffset = changes.nzOffset, nzValue = changes.nzValue, nzActive = changes.nzActive, nzTooltipVisible = changes.nzTooltipVisible;
        if (nzOffset) {
            this.updateStyle();
        }
        if (nzValue) {
            this.updateTooltipTitle();
            this.updateTooltipPosition();
        }
        if (nzActive) {
            if (nzActive.currentValue) {
                this.toggleTooltip(true);
            }
            else {
                this.toggleTooltip(false);
            }
        }
        if (nzTooltipVisible && nzTooltipVisible.currentValue === 'always') {
            Promise.resolve().then((/**
             * @return {?}
             */
            function () { return _this.toggleTooltip(true, true); }));
        }
    };
    /**
     * @return {?}
     */
    NzSliderHandleComponent.prototype.ngOnDestroy = /**
     * @return {?}
     */
    function () {
        this.hovers_.unsubscribe();
    };
    /**
     * @private
     * @param {?} show
     * @param {?=} force
     * @return {?}
     */
    NzSliderHandleComponent.prototype.toggleTooltip = /**
     * @private
     * @param {?} show
     * @param {?=} force
     * @return {?}
     */
    function (show, force) {
        if (force === void 0) { force = false; }
        if (!force && (this.nzTooltipVisible !== 'default' || !this.tooltip)) {
            return;
        }
        if (show) {
            this.tooltip.show();
        }
        else {
            this.tooltip.hide();
        }
    };
    /**
     * @private
     * @return {?}
     */
    NzSliderHandleComponent.prototype.updateTooltipTitle = /**
     * @private
     * @return {?}
     */
    function () {
        this.tooltipTitle = this.nzTipFormatter ? this.nzTipFormatter(this.nzValue) : "" + this.nzValue;
    };
    /**
     * @private
     * @return {?}
     */
    NzSliderHandleComponent.prototype.updateTooltipPosition = /**
     * @private
     * @return {?}
     */
    function () {
        var _this = this;
        if (this.tooltip) {
            Promise.resolve().then((/**
             * @return {?}
             */
            function () { return _this.tooltip.updatePosition(); }));
        }
    };
    /**
     * @private
     * @return {?}
     */
    NzSliderHandleComponent.prototype.updateStyle = /**
     * @private
     * @return {?}
     */
    function () {
        this.style[this.nzVertical ? 'bottom' : 'left'] = this.nzOffset + "%";
    };
    NzSliderHandleComponent.decorators = [
        { type: Component, args: [{
                    changeDetection: ChangeDetectionStrategy.OnPush,
                    encapsulation: ViewEncapsulation.None,
                    selector: 'nz-slider-handle',
                    preserveWhitespaces: false,
                    template: "<nz-tooltip\n  *ngIf=\"nzTipFormatter !== null && nzTooltipVisible !== 'never'\"\n  [nzTitle]=\"tooltipTitle\"\n  [nzTrigger]=\"null\">\n  <div nz-tooltip class=\"ant-slider-handle\" [ngStyle]=\"style\"></div>\n</nz-tooltip>\n<div *ngIf=\"nzTipFormatter === null || nzTooltipVisible === 'never'\" class=\"ant-slider-handle\" [ngStyle]=\"style\"></div>\n",
                    host: {
                        '(mouseenter)': 'enterHandle()',
                        '(mouseleave)': 'leaveHandle()'
                    }
                }] }
    ];
    /** @nocollapse */
    NzSliderHandleComponent.ctorParameters = function () { return [
        { type: NzSliderComponent },
        { type: ChangeDetectorRef }
    ]; };
    NzSliderHandleComponent.propDecorators = {
        tooltip: [{ type: ViewChild, args: [NzToolTipComponent,] }],
        nzVertical: [{ type: Input }],
        nzOffset: [{ type: Input }],
        nzValue: [{ type: Input }],
        nzTooltipVisible: [{ type: Input }],
        nzTipFormatter: [{ type: Input }],
        nzActive: [{ type: Input }]
    };
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Object)
    ], NzSliderHandleComponent.prototype, "nzActive", void 0);
    return NzSliderHandleComponent;
}());
export { NzSliderHandleComponent };
if (false) {
    /** @type {?} */
    NzSliderHandleComponent.prototype.tooltip;
    /** @type {?} */
    NzSliderHandleComponent.prototype.nzVertical;
    /** @type {?} */
    NzSliderHandleComponent.prototype.nzOffset;
    /** @type {?} */
    NzSliderHandleComponent.prototype.nzValue;
    /** @type {?} */
    NzSliderHandleComponent.prototype.nzTooltipVisible;
    /** @type {?} */
    NzSliderHandleComponent.prototype.nzTipFormatter;
    /** @type {?} */
    NzSliderHandleComponent.prototype.nzActive;
    /** @type {?} */
    NzSliderHandleComponent.prototype.tooltipTitle;
    /** @type {?} */
    NzSliderHandleComponent.prototype.style;
    /**
     * @type {?}
     * @private
     */
    NzSliderHandleComponent.prototype.hovers_;
    /** @type {?} */
    NzSliderHandleComponent.prototype.enterHandle;
    /** @type {?} */
    NzSliderHandleComponent.prototype.leaveHandle;
    /**
     * @type {?}
     * @private
     */
    NzSliderHandleComponent.prototype.sliderComponent;
    /**
     * @type {?}
     * @private
     */
    NzSliderHandleComponent.prototype.cdr;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotc2xpZGVyLWhhbmRsZS5jb21wb25lbnQuanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsic2xpZGVyL256LXNsaWRlci1oYW5kbGUuY29tcG9uZW50LnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7O0FBQUEsT0FBTyxFQUNMLHVCQUF1QixFQUN2QixpQkFBaUIsRUFDakIsU0FBUyxFQUNULEtBQUssRUFJTCxTQUFTLEVBQ1QsaUJBQWlCLEVBQ2xCLE1BQU0sZUFBZSxDQUFDO0FBQ3ZCLE9BQU8sRUFBRSxZQUFZLEVBQUUsTUFBTSxNQUFNLENBQUM7QUFHcEMsT0FBTyxFQUFFLFlBQVksRUFBRSxNQUFNLHNCQUFzQixDQUFDO0FBQ3BELE9BQU8sRUFBRSxrQkFBa0IsRUFBRSxNQUFNLGlDQUFpQyxDQUFDO0FBR3JFLE9BQU8sRUFBRSxpQkFBaUIsRUFBRSxNQUFNLHVCQUF1QixDQUFDO0FBRTFEO0lBMEJFLGlDQUFvQixlQUFrQyxFQUFVLEdBQXNCO1FBQXRGLGlCQUEwRjtRQUF0RSxvQkFBZSxHQUFmLGVBQWUsQ0FBbUI7UUFBVSxRQUFHLEdBQUgsR0FBRyxDQUFtQjtRQVQ3RSxxQkFBZ0IsR0FBc0IsU0FBUyxDQUFDO1FBRWhDLGFBQVEsR0FBRyxLQUFLLENBQUM7UUFHMUMsVUFBSyxHQUFxQixFQUFFLENBQUM7UUFFckIsWUFBTyxHQUFHLElBQUksWUFBWSxFQUFFLENBQUM7UUE4QnJDLGdCQUFXOzs7UUFBRztZQUNaLElBQUksQ0FBQyxLQUFJLENBQUMsZUFBZSxDQUFDLFVBQVUsRUFBRTtnQkFDcEMsS0FBSSxDQUFDLGFBQWEsQ0FBQyxJQUFJLENBQUMsQ0FBQztnQkFDekIsS0FBSSxDQUFDLHFCQUFxQixFQUFFLENBQUM7Z0JBQzdCLEtBQUksQ0FBQyxHQUFHLENBQUMsYUFBYSxFQUFFLENBQUM7YUFDMUI7UUFDSCxDQUFDLEVBQUM7UUFFRixnQkFBVzs7O1FBQUc7WUFDWixJQUFJLENBQUMsS0FB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RSxlQUFlO3dCQUMvQixjQUFjLEVBQUUsZUFBZTtxQkFDaEM7aUJBQ0Y7Ozs7Z0JBWlEsaUJBQWlCO2dCQWhCeEIsaUJBQWlCOzs7MEJBOEJoQixTQUFTLFNBQUMsa0JBQWtCOzZCQUU1QixLQUFLOzJCQUNMLEtBQUs7MEJBQ0wsS0FBSzttQ0FDTCxLQUFLO2lDQUNMLEtBQUs7MkJBQ0wsS0FBSzs7SUFBbUI7UUFBZixZQUFZLEVBQUU7OzZEQUFrQjtJQTJFNUMsOEJBQUM7Q0FBQSxBQTlGRCxJQThGQztTQW5GWSx1QkFBdUI7OztJQUNsQywwQ0FBMkQ7O0lBRTNELDZDQUE0Qjs7SUFDNUIsMkNBQTBCOztJQUMxQiwwQ0FBeUI7O0lBQ3pCLG1EQUF5RDs7SUFDekQsaURBQW1EOztJQUNuRCwyQ0FBMEM7O0lBRTFDLCtDQUFxQjs7SUFDckIsd0NBQTZCOzs7OztJQUU3QiwwQ0FBcUM7O0lBOEJyQyw4Q0FNRTs7SUFFRiw4Q0FLRTs7Ozs7SUF6Q1Usa0RBQTBDOzs7OztJQUFFLHNDQUE4QiIsInNvdXJjZXNDb250ZW50IjpbImltcG9ydCB7XG4gIENoYW5nZURldGVjdGlvblN0cmF0ZWd5LFxuICBDaGFuZ2VEZXRlY3RvclJlZixcbiAgQ29tcG9uZW50LFxuICBJbnB1dCxcbiAgT25DaGFuZ2VzLFxuICBPbkRlc3Ryb3ksXG4gIFNpbXBsZUNoYW5nZXMsXG4gIFZpZXdDaGlsZCxcbiAgVmlld0VuY2Fwc3VsYXRpb25cbn0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5pbXBvcnQgeyBTdWJzY3JpcHRpb24gfSBmcm9tICdyeGpzJztcbmltcG9ydCB7IE5HU3R5bGVJbnRlcmZhY2UgfSBmcm9tICcuLi9jb3JlL3R5cGVzL25nLWNsYXNzJztcblxuaW1wb3J0IHsgSW5wdXRCb29sZWFuIH0gZnJvbSAnLi4vY29yZS91dGlsL2NvbnZlcnQnO1xuaW1wb3J0IHsgTnpUb29sVGlwQ29tcG9uZW50IH0gZnJvbSAnLi4vdG9vbHRpcC9uei10b29sdGlwLmNvbXBvbmVudCc7XG5cbmltcG9ydCB7IFNsaWRlclNob3dUb29sdGlwIH0gZnJvbSAnLi9uei1zbGlkZXItZGVmaW5pdGlvbnMnO1xuaW1wb3J0IHsgTnpTbGlkZXJDb21wb25lbnQgfSBmcm9tICcuL256LXNsaWRlci5jb21wb25lbnQnO1xuXG5AQ29tcG9uZW50KHtcbiAgY2hhbmdlRGV0ZWN0aW9uOiBDaGFuZ2VEZXRlY3Rpb25TdHJhdGVneS5PblB1c2gsXG4gIGVuY2Fwc3VsYXRpb246IFZpZXdFbmNhcHN1bGF0aW9uLk5vbmUsXG4gIHNlbGVjdG9yOiAnbnotc2xpZGVyLWhhbmRsZScsXG4gIHByZXNlcnZlV2hpdGVzcGFjZXM6IGZhbHNlLFxuICB0ZW1wbGF0ZVVybDogJy4vbnotc2xpZGVyLWhhbmRsZS5jb21wb25lbnQuaHRtbCcsXG4gIGhvc3Q6IHtcbiAgICAnKG1vdXNlZW50ZXIpJzogJ2VudGVySGFuZGxlKCknLFxuICAgICcobW91c2VsZWF2ZSknOiAnbGVhdmVIYW5kbGUoKSdcbiAgfVxufSlcbmV4cG9ydCBjbGFzcyBOelNsaWRlckhhbmRsZUNvbXBvbmVudCBpbXBsZW1lbnRzIE9uQ2hhbmdlcywgT25EZXN0cm95IHtcbiAgQFZpZXdDaGlsZChOelRvb2xUaXBDb21wb25lbnQpIHRvb2x0aXA6IE56VG9vbFRpcENvbXBvbmVudDtcblxuICBASW5wdXQoKSBuelZlcnRpY2FsOiBzdHJpbmc7XG4gIEBJbnB1dCgpIG56T2Zmc2V0OiBudW1iZXI7XG4gIEBJbnB1dCgpIG56VmFsdWU6IG51bWJlcjtcbiAgQElucHV0KCkgbnpUb29sdGlwVmlzaWJsZTogU2xpZGVyU2hvd1Rvb2x0aXAgPSAnZGVmYXVsdCc7XG4gIEBJbnB1dCgpIG56VGlwRm9ybWF0dGVyOiAodmFsdWU6IG51bWJlcikgPT4gc3RyaW5nO1xuICBASW5wdXQoKSBASW5wdXRCb29sZWFuKCkgbnpBY3RpdmUgPSBmYWxzZTtcblxuICB0b29sdGlwVGl0bGU6IHN0cmluZztcbiAgc3R5bGU6IE5HU3R5bGVJbnRlcmZhY2UgPSB7fTtcblxuICBwcml2YXRlIGhvdmVyc18gPSBuZXcgU3Vic2NyaXB0aW9uKCk7XG5cbiAgY29uc3RydWN0b3IocHJpdmF0ZSBzbGlkZXJDb21wb25lbnQ6IE56U2xpZGVyQ29tcG9uZW50LCBwcml2YXRlIGNkcjogQ2hhbmdlRGV0ZWN0b3JSZWYpIHt9XG5cbiAgbmdPbkNoYW5nZXMoY2hhbmdlczogU2ltcGxlQ2hhbmdlcyk6IHZvaWQge1xuICAgIGNvbnN0IHsgbnpPZmZzZXQsIG56VmFsdWUsIG56QWN0aXZlLCBuelRvb2x0aXBWaXNpYmxlIH0gPSBjaGFuZ2VzO1xuXG4gICAgaWYgKG56T2Zmc2V0KSB7XG4gICAgICB0aGlzLnVwZGF0ZVN0eWxlKCk7XG4gICAgfVxuICAgIGlmIChuelZhbHVlKSB7XG4gICAgICB0aGlzLnVwZGF0ZVRvb2x0aXBUaXRsZSgpO1xuICAgICAgdGhpcy51cGRhdGVUb29sdGlwUG9zaXRpb24oKTtcbiAgICB9XG4gICAgaWYgKG56QWN0aXZlKSB7XG4gICAgICBpZiAobnpBY3RpdmUuY3VycmVudFZhbHVlKSB7XG4gICAgICAgIHRoaXMudG9nZ2xlVG9vbHRpcCh0cnVlKTtcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIHRoaXMudG9nZ2xlVG9vbHRpcChmYWxzZSk7XG4gICAgICB9XG4gICAgfVxuICAgIGlmIChuelRvb2x0aXBWaXNpYmxlICYmIG56VG9vbHRpcFZpc2libGUuY3VycmVudFZhbHVlID09PSAnYWx3YXlzJykge1xuICAgICAgUHJvbWlzZS5yZXNvbHZlKCkudGhlbigoKSA9PiB0aGlzLnRvZ2dsZVRvb2x0aXAodHJ1ZSwgdHJ1ZSkpO1xuICAgIH1cbiAgfVxuXG4gIG5nT25EZXN0cm95KCk6IHZvaWQge1xuICAgIHRoaXMuaG92ZXJzXy51bnN1YnNjcmliZSgpO1xuICB9XG5cbiAgZW50ZXJIYW5kbGUgPSAoKSA9PiB7XG4gICAgaWYgKCF0aGlzLnNsaWRlckNvbXBvbmVudC5pc0RyYWdnaW5nKSB7XG4gICAgICB0aGlzLnRvZ2dsZVRvb2x0aXAodHJ1ZSk7XG4gICAgICB0aGlzLnVwZGF0ZVRvb2x0aXBQb3NpdGlvbigpO1xuICAgICAgdGhpcy5jZHIuZGV0ZWN0Q2hhbmdlcygpO1xuICAgIH1cbiAgfTtcblxuICBsZWF2ZUhhbmRsZSA9ICgpID0+IHtcbiAgICBpZiAoIXRoaXMuc2xpZGVyQ29tcG9uZW50LmlzRHJhZ2dpbmcpIHtcbiAgICAgIHRoaXMudG9nZ2xlVG9vbHRpcChmYWxzZSk7XG4gICAgICB0aGlzLmNkci5kZXRlY3RDaGFuZ2VzKCk7XG4gICAgfVxuICB9O1xuXG4gIHByaXZhdGUgdG9nZ2xlVG9vbHRpcChzaG93OiBib29sZWFuLCBmb3JjZTogYm9vbGVhbiA9IGZhbHNlKTogdm9pZCB7XG4gICAgaWYgKCFmb3JjZSAmJiAodGhpcy5uelRvb2x0aXBWaXNpYmxlICE9PSAnZGVmYXVsdCcgfHwgIXRoaXMudG9vbHRpcCkpIHtcbiAgICAgIHJldHVybjtcbiAgICB9XG5cbiAgICBpZiAoc2hvdykge1xuICAgICAgdGhpcy50b29sdGlwLnNob3coKTtcbiAgICB9IGVsc2Uge1xuICAgICAgdGhpcy50b29sdGlwLmhpZGUoKTtcbiAgICB9XG4gIH1cblxuICBwcml2YXRlIHVwZGF0ZVRvb2x0aXBUaXRsZSgpOiB2b2lkIHtcbiAgICB0aGlzLnRvb2x0aXBUaXRsZSA9IHRoaXMubnpUaXBGb3JtYXR0ZXIgPyB0aGlzLm56VGlwRm9ybWF0dGVyKHRoaXMubnpWYWx1ZSkgOiBgJHt0aGlzLm56VmFsdWV9YDtcbiAgfVxuXG4gIHByaXZhdGUgdXBkYXRlVG9vbHRpcFBvc2l0aW9uKCk6IHZvaWQge1xuICAgIGlmICh0aGlzLnRvb2x0aXApIHtcbiAgICAgIFByb21pc2UucmVzb2x2ZSgpLnRoZW4oKCkgPT4gdGhpcy50b29sdGlwLnVwZGF0ZVBvc2l0aW9uKCkpO1xuICAgIH1cbiAgfVxuXG4gIHByaXZhdGUgdXBkYXRlU3R5bGUoKTogdm9pZCB7XG4gICAgdGhpcy5zdHlsZVt0aGlzLm56VmVydGljYWwgPyAnYm90dG9tJyA6ICdsZWZ0J10gPSBgJHt0aGlzLm56T2Zmc2V0fSVgO1xuICB9XG59XG4iXX0=