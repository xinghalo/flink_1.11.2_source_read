/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import * as tslib_1 from "tslib";
import { Directive, ElementRef, Input, NgZone, Renderer2 } from '@angular/core';
import { InputBoolean } from '../core/util/convert';
export class NzTabsInkBarDirective {
    /**
     * @param {?} renderer
     * @param {?} elementRef
     * @param {?} ngZone
     */
    constructor(renderer, elementRef, ngZone) {
        this.renderer = renderer;
        this.elementRef = elementRef;
        this.ngZone = ngZone;
        this.nzAnimated = false;
        this.nzPositionMode = 'horizontal';
        renderer.addClass(elementRef.nativeElement, 'ant-tabs-ink-bar');
    }
    /**
     * @param {?} element
     * @return {?}
     */
    alignToElement(element) {
        if (typeof requestAnimationFrame !== 'undefined') {
            this.ngZone.runOutsideAngular((/**
             * @return {?}
             */
            () => {
                requestAnimationFrame((/**
                 * @return {?}
                 */
                () => this.setStyles(element)));
            }));
        }
        else {
            this.setStyles(element);
        }
    }
    /**
     * @param {?} element
     * @return {?}
     */
    setStyles(element) {
        /** when horizontal remove height style and add transform left **/
        if (this.nzPositionMode === 'horizontal') {
            this.renderer.removeStyle(this.elementRef.nativeElement, 'height');
            this.renderer.setStyle(this.elementRef.nativeElement, 'transform', `translate3d(${this.getLeftPosition(element)}, 0px, 0px)`);
            this.renderer.setStyle(this.elementRef.nativeElement, 'width', this.getElementWidth(element));
        }
        else {
            /** when vertical remove width style and add transform top **/
            this.renderer.removeStyle(this.elementRef.nativeElement, 'width');
            this.renderer.setStyle(this.elementRef.nativeElement, 'transform', `translate3d(0px, ${this.getTopPosition(element)}, 0px)`);
            this.renderer.setStyle(this.elementRef.nativeElement, 'height', this.getElementHeight(element));
        }
    }
    /**
     * @param {?} element
     * @return {?}
     */
    getLeftPosition(element) {
        return element ? element.offsetLeft + 'px' : '0';
    }
    /**
     * @param {?} element
     * @return {?}
     */
    getElementWidth(element) {
        return element ? element.offsetWidth + 'px' : '0';
    }
    /**
     * @param {?} element
     * @return {?}
     */
    getTopPosition(element) {
        return element ? element.offsetTop + 'px' : '0';
    }
    /**
     * @param {?} element
     * @return {?}
     */
    getElementHeight(element) {
        return element ? element.offsetHeight + 'px' : '0';
    }
}
NzTabsInkBarDirective.decorators = [
    { type: Directive, args: [{
                selector: '[nz-tabs-ink-bar]',
                host: {
                    '[class.ant-tabs-ink-bar-animated]': 'nzAnimated',
                    '[class.ant-tabs-ink-bar-no-animated]': '!nzAnimated'
                }
            },] }
];
/** @nocollapse */
NzTabsInkBarDirective.ctorParameters = () => [
    { type: Renderer2 },
    { type: ElementRef },
    { type: NgZone }
];
NzTabsInkBarDirective.propDecorators = {
    nzAnimated: [{ type: Input }],
    nzPositionMode: [{ type: Input }]
};
tslib_1.__decorate([
    InputBoolean(),
    tslib_1.__metadata("design:type", Object)
], NzTabsInkBarDirective.prototype, "nzAnimated", void 0);
if (false) {
    /** @type {?} */
    NzTabsInkBarDirective.prototype.nzAnimated;
    /** @type {?} */
    NzTabsInkBarDirective.prototype.nzPositionMode;
    /**
     * @type {?}
     * @private
     */
    NzTabsInkBarDirective.prototype.renderer;
    /**
     * @type {?}
     * @private
     */
    NzTabsInkBarDirective.prototype.elementRef;
    /**
     * @type {?}
     * @private
     */
    NzTabsInkBarDirective.prototype.ngZone;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotdGFicy1pbmstYmFyLmRpcmVjdGl2ZS5qcyIsInNvdXJjZVJvb3QiOiJuZzovL25nLXpvcnJvLWFudGQvIiwic291cmNlcyI6WyJ0YWJzL256LXRhYnMtaW5rLWJhci5kaXJlY3RpdmUudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7QUFBQSxPQUFPLEVBQUUsU0FBUyxFQUFFLFVBQVUsRUFBRSxLQUFLLEVBQUUsTUFBTSxFQUFFLFNBQVMsRUFBRSxNQUFNLGVBQWUsQ0FBQztBQUVoRixPQUFPLEVBQUUsWUFBWSxFQUFFLE1BQU0sc0JBQXNCLENBQUM7QUFXcEQsTUFBTSxPQUFPLHFCQUFxQjs7Ozs7O0lBS2hDLFlBQW9CLFFBQW1CLEVBQVUsVUFBc0IsRUFBVSxNQUFjO1FBQTNFLGFBQVEsR0FBUixRQUFRLENBQVc7UUFBVSxlQUFVLEdBQVYsVUFBVSxDQUFZO1FBQVUsV0FBTSxHQUFOLE1BQU0sQ0FBUTtRQUp0RSxlQUFVLEdBQUcsS0FBSyxDQUFDO1FBRW5DLG1CQUFjLEdBQXNCLFlBQVksQ0FBQztRQUd4RCxRQUFRLENBQUMsUUFBUSxDQUFDLFVBQVUsQ0FBQyxhQUFhLEVBQUUsa0JBQWtCLENBQUMsQ0FBQztJQUNsRSxDQUFDOzs7OztJQUVELGNBQWMsQ0FBQyxPQUFvQjtRQUNqQyxJQUFJLE9BQU8scUJBQXFCLEtBQUssV0FBVyxFQUFFO1lBQ2hELElBQUksQ0FBQyxNQUFNLENBQUMsaUJBQWlCOzs7WUFBQyxHQUFHLEVBQUU7Z0JBQ2pDLHFCQUFxQjs7O2dCQUFDLEdBQUcsRUFBRSxDQUFDLElBQUksQ0FBQyxTQUFTLENBQUMsT0FBTyxDQUFDLEVBQUMsQ0FBQztZQUN2RCxDQUFDLEVBQUMsQ0FBQztTQUNKO2FBQU07WUFDTCxJQUFJLENBQUMsU0FBUyxDQUFDLE9BQU8sQ0FBQyxDQUFDO1NBQ3pCO0lBQ0gsQ0FBQzs7Ozs7SUFFRCxTQUFTLENBQUMsT0FBb0I7UUFDNUIsa0VBQWtFO1FBQ2xFLElBQUksSUFBSSxDQUFDLGNBQWMsS0FBSyxZQUFZLEVBQUU7WUFDeEMsSUFBSSxDQUFDLFFBQVEsQ0FBQyxXQUFXLENBQUMsSUFBSSxDQUFDLFVBQVUsQ0FBQyxhQUFhLEVBQUUsUUFBUSxDQUFDLENBQUM7WUFDbkUsSUFBSSxDQUFDLFFBQVEsQ0FBQyxRQUFRLENBQ3BCLElBQUksQ0FBQyxVQUFVLENBQUMsYUFBYSxFQUM3QixXQUFXLEVBQ1gsZUFBZSxJQUFJLENBQUMsZUFBZSxDQUFDLE9BQU8sQ0FBQyxhQUFhLENBQzFELENBQUM7WUFDRixJQUFJLENBQUMsUUFBUSxDQUFDLFFBQVEsQ0FBQyxJQUFJLENBQUMsVUFBVSxDQUFDLGFBQWEsRUFBRSxPQUFPLEVBQUUsSUFBSSxDQUFDLGVBQWUsQ0FBQyxPQUFPLENBQUMsQ0FBQyxDQUFDO1NBQy9GO2FBQU07WUFDTCw4REFBOEQ7WUFDOUQsSUFBSSxDQUFDLFFBQVEsQ0FBQyxXQUFXLENBQUMsSUFBSSxDQUFDLFVBQVUsQ0FBQyxhQUFhLEVBQUUsT0FBTyxDQUFDLENBQUM7WUFDbEUsSUFBSSxDQUFDLFFBQVEsQ0FBQyxRQUFRLENBQ3BCLElBQUksQ0FBQyxVQUFVLENBQUMsYUFBYSxFQUM3QixXQUFXLEVBQ1gsb0JBQW9CLElBQUksQ0FBQyxjQUFjLENBQUMsT0FBTyxDQUFDLFFBQVEsQ0FDekQsQ0FBQztZQUNGLElBQUksQ0FBQyxRQUFRLENBQUMsUUFBUSxDQUFDLElBQUksQ0FBQyxVQUFVLENBQUMsYUFBYSxFQUFFLFFBQVEsRUFBRSxJQUFJLENBQUMsZ0JBQWdCLENBQUMsT0FBTyxDQUFDLENBQUMsQ0FBQztTQUNqRztJQUNILENBQUM7Ozs7O0lBRUQsZUFBZSxDQUFDLE9BQW9CO1FBQ2xDLE9BQU8sT0FBTyxDQUFDLENBQUMsQ0FBQyxPQUFPLENBQUMsVUFBVSxHQUFHLElBQUksQ0FBQyxDQUFDLENBQUMsR0FBRyxDQUFDO0lBQ25ELENBQUM7Ozs7O0lBRUQsZUFBZSxDQUFDLE9BQW9CO1FBQ2xDLE9BQU8sT0FBTyxDQUFDLENBQUMsQ0FBQyxPQUFPLENBQUMsV0FBVyxHQUFHLElBQUksQ0FBQyxDQUFDLENBQUMsR0FBRyxDQUFDO0lBQ3BELENBQUM7Ozs7O0lBRUQsY0FBYyxDQUFDLE9BQW9CO1FBQ2pDLE9BQU8sT0FBTyxDQUFDLENBQUMsQ0FBQyxPQUFPLENBQUMsU0FBUyxHQUFHLElBQUksQ0FBQyxDQUFDLENBQUMsR0FBRyxDQUFDO0lBQ2xELENBQUM7Ozs7O0lBRUQsZ0JBQWdCLENBQUMsT0FBb0I7UUFDbkMsT0FBTyxPQUFPLENBQUMsQ0FBQyxDQUFDLE9BQU8sQ0FBQyxZQUFZLEdBQUcsSUFBSSxDQUFDLENBQUMsQ0FBQyxHQUFHLENBQUM7SUFDckQsQ0FBQzs7O1lBOURGLFNBQVMsU0FBQztnQkFDVCxRQUFRLEVBQUUsbUJBQW1CO2dCQUM3QixJQUFJLEVBQUU7b0JBQ0osbUNBQW1DLEVBQUUsWUFBWTtvQkFDakQsc0NBQXNDLEVBQUUsYUFBYTtpQkFDdEQ7YUFDRjs7OztZQVo4QyxTQUFTO1lBQXBDLFVBQVU7WUFBUyxNQUFNOzs7eUJBYzFDLEtBQUs7NkJBRUwsS0FBSzs7QUFGbUI7SUFBZixZQUFZLEVBQUU7O3lEQUFvQjs7O0lBQTVDLDJDQUE0Qzs7SUFFNUMsK0NBQTBEOzs7OztJQUU5Qyx5Q0FBMkI7Ozs7O0lBQUUsMkNBQThCOzs7OztJQUFFLHVDQUFzQiIsInNvdXJjZXNDb250ZW50IjpbImltcG9ydCB7IERpcmVjdGl2ZSwgRWxlbWVudFJlZiwgSW5wdXQsIE5nWm9uZSwgUmVuZGVyZXIyIH0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5cbmltcG9ydCB7IElucHV0Qm9vbGVhbiB9IGZyb20gJy4uL2NvcmUvdXRpbC9jb252ZXJ0JztcblxuaW1wb3J0IHsgTnpUYWJQb3NpdGlvbk1vZGUgfSBmcm9tICcuL256LXRhYnNldC5jb21wb25lbnQnO1xuXG5ARGlyZWN0aXZlKHtcbiAgc2VsZWN0b3I6ICdbbnotdGFicy1pbmstYmFyXScsXG4gIGhvc3Q6IHtcbiAgICAnW2NsYXNzLmFudC10YWJzLWluay1iYXItYW5pbWF0ZWRdJzogJ256QW5pbWF0ZWQnLFxuICAgICdbY2xhc3MuYW50LXRhYnMtaW5rLWJhci1uby1hbmltYXRlZF0nOiAnIW56QW5pbWF0ZWQnXG4gIH1cbn0pXG5leHBvcnQgY2xhc3MgTnpUYWJzSW5rQmFyRGlyZWN0aXZlIHtcbiAgQElucHV0KCkgQElucHV0Qm9vbGVhbigpIG56QW5pbWF0ZWQgPSBmYWxzZTtcblxuICBASW5wdXQoKSBuelBvc2l0aW9uTW9kZTogTnpUYWJQb3NpdGlvbk1vZGUgPSAnaG9yaXpvbnRhbCc7XG5cbiAgY29uc3RydWN0b3IocHJpdmF0ZSByZW5kZXJlcjogUmVuZGVyZXIyLCBwcml2YXRlIGVsZW1lbnRSZWY6IEVsZW1lbnRSZWYsIHByaXZhdGUgbmdab25lOiBOZ1pvbmUpIHtcbiAgICByZW5kZXJlci5hZGRDbGFzcyhlbGVtZW50UmVmLm5hdGl2ZUVsZW1lbnQsICdhbnQtdGFicy1pbmstYmFyJyk7XG4gIH1cblxuICBhbGlnblRvRWxlbWVudChlbGVtZW50OiBIVE1MRWxlbWVudCk6IHZvaWQge1xuICAgIGlmICh0eXBlb2YgcmVxdWVzdEFuaW1hdGlvbkZyYW1lICE9PSAndW5kZWZpbmVkJykge1xuICAgICAgdGhpcy5uZ1pvbmUucnVuT3V0c2lkZUFuZ3VsYXIoKCkgPT4ge1xuICAgICAgICByZXF1ZXN0QW5pbWF0aW9uRnJhbWUoKCkgPT4gdGhpcy5zZXRTdHlsZXMoZWxlbWVudCkpO1xuICAgICAgfSk7XG4gICAgfSBlbHNlIHtcbiAgICAgIHRoaXMuc2V0U3R5bGVzKGVsZW1lbnQpO1xuICAgIH1cbiAgfVxuXG4gIHNldFN0eWxlcyhlbGVtZW50OiBIVE1MRWxlbWVudCk6IHZvaWQge1xuICAgIC8qKiB3aGVuIGhvcml6b250YWwgcmVtb3ZlIGhlaWdodCBzdHlsZSBhbmQgYWRkIHRyYW5zZm9ybSBsZWZ0ICoqL1xuICAgIGlmICh0aGlzLm56UG9zaXRpb25Nb2RlID09PSAnaG9yaXpvbnRhbCcpIHtcbiAgICAgIHRoaXMucmVuZGVyZXIucmVtb3ZlU3R5bGUodGhpcy5lbGVtZW50UmVmLm5hdGl2ZUVsZW1lbnQsICdoZWlnaHQnKTtcbiAgICAgIHRoaXMucmVuZGVyZXIuc2V0U3R5bGUoXG4gICAgICAgIHRoaXMuZWxlbWVudFJlZi5uYXRpdmVFbGVtZW50LFxuICAgICAgICAndHJhbnNmb3JtJyxcbiAgICAgICAgYHRyYW5zbGF0ZTNkKCR7dGhpcy5nZXRMZWZ0UG9zaXRpb24oZWxlbWVudCl9LCAwcHgsIDBweClgXG4gICAgICApO1xuICAgICAgdGhpcy5yZW5kZXJlci5zZXRTdHlsZSh0aGlzLmVsZW1lbnRSZWYubmF0aXZlRWxlbWVudCwgJ3dpZHRoJywgdGhpcy5nZXRFbGVtZW50V2lkdGgoZWxlbWVudCkpO1xuICAgIH0gZWxzZSB7XG4gICAgICAvKiogd2hlbiB2ZXJ0aWNhbCByZW1vdmUgd2lkdGggc3R5bGUgYW5kIGFkZCB0cmFuc2Zvcm0gdG9wICoqL1xuICAgICAgdGhpcy5yZW5kZXJlci5yZW1vdmVTdHlsZSh0aGlzLmVsZW1lbnRSZWYubmF0aXZlRWxlbWVudCwgJ3dpZHRoJyk7XG4gICAgICB0aGlzLnJlbmRlcmVyLnNldFN0eWxlKFxuICAgICAgICB0aGlzLmVsZW1lbnRSZWYubmF0aXZlRWxlbWVudCxcbiAgICAgICAgJ3RyYW5zZm9ybScsXG4gICAgICAgIGB0cmFuc2xhdGUzZCgwcHgsICR7dGhpcy5nZXRUb3BQb3NpdGlvbihlbGVtZW50KX0sIDBweClgXG4gICAgICApO1xuICAgICAgdGhpcy5yZW5kZXJlci5zZXRTdHlsZSh0aGlzLmVsZW1lbnRSZWYubmF0aXZlRWxlbWVudCwgJ2hlaWdodCcsIHRoaXMuZ2V0RWxlbWVudEhlaWdodChlbGVtZW50KSk7XG4gICAgfVxuICB9XG5cbiAgZ2V0TGVmdFBvc2l0aW9uKGVsZW1lbnQ6IEhUTUxFbGVtZW50KTogc3RyaW5nIHtcbiAgICByZXR1cm4gZWxlbWVudCA/IGVsZW1lbnQub2Zmc2V0TGVmdCArICdweCcgOiAnMCc7XG4gIH1cblxuICBnZXRFbGVtZW50V2lkdGgoZWxlbWVudDogSFRNTEVsZW1lbnQpOiBzdHJpbmcge1xuICAgIHJldHVybiBlbGVtZW50ID8gZWxlbWVudC5vZmZzZXRXaWR0aCArICdweCcgOiAnMCc7XG4gIH1cblxuICBnZXRUb3BQb3NpdGlvbihlbGVtZW50OiBIVE1MRWxlbWVudCk6IHN0cmluZyB7XG4gICAgcmV0dXJuIGVsZW1lbnQgPyBlbGVtZW50Lm9mZnNldFRvcCArICdweCcgOiAnMCc7XG4gIH1cblxuICBnZXRFbGVtZW50SGVpZ2h0KGVsZW1lbnQ6IEhUTUxFbGVtZW50KTogc3RyaW5nIHtcbiAgICByZXR1cm4gZWxlbWVudCA/IGVsZW1lbnQub2Zmc2V0SGVpZ2h0ICsgJ3B4JyA6ICcwJztcbiAgfVxufVxuIl19