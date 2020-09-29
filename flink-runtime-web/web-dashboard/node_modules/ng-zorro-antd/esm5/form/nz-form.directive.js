/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { Directive, ElementRef, Input, Renderer2 } from '@angular/core';
import { NzUpdateHostClassService } from '../core/services/update-host-class.service';
var NzFormDirective = /** @class */ (function () {
    function NzFormDirective(elementRef, renderer, nzUpdateHostClassService) {
        this.elementRef = elementRef;
        this.renderer = renderer;
        this.nzUpdateHostClassService = nzUpdateHostClassService;
        this.nzLayout = 'horizontal';
        this.renderer.addClass(elementRef.nativeElement, 'ant-form');
    }
    /**
     * @return {?}
     */
    NzFormDirective.prototype.setClassMap = /**
     * @return {?}
     */
    function () {
        var _a;
        this.nzUpdateHostClassService.updateHostClass(this.elementRef.nativeElement, (_a = {},
            _a["ant-form-" + this.nzLayout] = this.nzLayout,
            _a));
    };
    /**
     * @return {?}
     */
    NzFormDirective.prototype.ngOnInit = /**
     * @return {?}
     */
    function () {
        this.setClassMap();
    };
    /**
     * @return {?}
     */
    NzFormDirective.prototype.ngOnChanges = /**
     * @return {?}
     */
    function () {
        this.setClassMap();
    };
    NzFormDirective.decorators = [
        { type: Directive, args: [{
                    selector: '[nz-form]',
                    providers: [NzUpdateHostClassService]
                },] }
    ];
    /** @nocollapse */
    NzFormDirective.ctorParameters = function () { return [
        { type: ElementRef },
        { type: Renderer2 },
        { type: NzUpdateHostClassService }
    ]; };
    NzFormDirective.propDecorators = {
        nzLayout: [{ type: Input }]
    };
    return NzFormDirective;
}());
export { NzFormDirective };
if (false) {
    /** @type {?} */
    NzFormDirective.prototype.nzLayout;
    /**
     * @type {?}
     * @private
     */
    NzFormDirective.prototype.elementRef;
    /**
     * @type {?}
     * @private
     */
    NzFormDirective.prototype.renderer;
    /**
     * @type {?}
     * @private
     */
    NzFormDirective.prototype.nzUpdateHostClassService;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotZm9ybS5kaXJlY3RpdmUuanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsiZm9ybS9uei1mb3JtLmRpcmVjdGl2ZS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7O0FBQUEsT0FBTyxFQUFFLFNBQVMsRUFBRSxVQUFVLEVBQUUsS0FBSyxFQUFxQixTQUFTLEVBQUUsTUFBTSxlQUFlLENBQUM7QUFDM0YsT0FBTyxFQUFFLHdCQUF3QixFQUFFLE1BQU0sNENBQTRDLENBQUM7QUFFdEY7SUFhRSx5QkFDVSxVQUFzQixFQUN0QixRQUFtQixFQUNuQix3QkFBa0Q7UUFGbEQsZUFBVSxHQUFWLFVBQVUsQ0FBWTtRQUN0QixhQUFRLEdBQVIsUUFBUSxDQUFXO1FBQ25CLDZCQUF3QixHQUF4Qix3QkFBd0IsQ0FBMEI7UUFYbkQsYUFBUSxHQUFHLFlBQVksQ0FBQztRQWEvQixJQUFJLENBQUMsUUFBUSxDQUFDLFFBQVEsQ0FBQyxVQUFVLENBQUMsYUFBYSxFQUFFLFVBQVUsQ0FBQyxDQUFDO0lBQy9ELENBQUM7Ozs7SUFaRCxxQ0FBVzs7O0lBQVg7O1FBQ0UsSUFBSSxDQUFDLHdCQUF3QixDQUFDLGVBQWUsQ0FBQyxJQUFJLENBQUMsVUFBVSxDQUFDLGFBQWE7WUFDekUsR0FBQyxjQUFZLElBQUksQ0FBQyxRQUFVLElBQUcsSUFBSSxDQUFDLFFBQVE7Z0JBQzVDLENBQUM7SUFDTCxDQUFDOzs7O0lBVUQsa0NBQVE7OztJQUFSO1FBQ0UsSUFBSSxDQUFDLFdBQVcsRUFBRSxDQUFDO0lBQ3JCLENBQUM7Ozs7SUFFRCxxQ0FBVzs7O0lBQVg7UUFDRSxJQUFJLENBQUMsV0FBVyxFQUFFLENBQUM7SUFDckIsQ0FBQzs7Z0JBM0JGLFNBQVMsU0FBQztvQkFDVCxRQUFRLEVBQUUsV0FBVztvQkFDckIsU0FBUyxFQUFFLENBQUMsd0JBQXdCLENBQUM7aUJBQ3RDOzs7O2dCQU5tQixVQUFVO2dCQUE0QixTQUFTO2dCQUMxRCx3QkFBd0I7OzsyQkFPOUIsS0FBSzs7SUF1QlIsc0JBQUM7Q0FBQSxBQTVCRCxJQTRCQztTQXhCWSxlQUFlOzs7SUFDMUIsbUNBQWlDOzs7OztJQVMvQixxQ0FBOEI7Ozs7O0lBQzlCLG1DQUEyQjs7Ozs7SUFDM0IsbURBQTBEIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHsgRGlyZWN0aXZlLCBFbGVtZW50UmVmLCBJbnB1dCwgT25DaGFuZ2VzLCBPbkluaXQsIFJlbmRlcmVyMiB9IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuaW1wb3J0IHsgTnpVcGRhdGVIb3N0Q2xhc3NTZXJ2aWNlIH0gZnJvbSAnLi4vY29yZS9zZXJ2aWNlcy91cGRhdGUtaG9zdC1jbGFzcy5zZXJ2aWNlJztcblxuQERpcmVjdGl2ZSh7XG4gIHNlbGVjdG9yOiAnW256LWZvcm1dJyxcbiAgcHJvdmlkZXJzOiBbTnpVcGRhdGVIb3N0Q2xhc3NTZXJ2aWNlXVxufSlcbmV4cG9ydCBjbGFzcyBOekZvcm1EaXJlY3RpdmUgaW1wbGVtZW50cyBPbkluaXQsIE9uQ2hhbmdlcyB7XG4gIEBJbnB1dCgpIG56TGF5b3V0ID0gJ2hvcml6b250YWwnO1xuXG4gIHNldENsYXNzTWFwKCk6IHZvaWQge1xuICAgIHRoaXMubnpVcGRhdGVIb3N0Q2xhc3NTZXJ2aWNlLnVwZGF0ZUhvc3RDbGFzcyh0aGlzLmVsZW1lbnRSZWYubmF0aXZlRWxlbWVudCwge1xuICAgICAgW2BhbnQtZm9ybS0ke3RoaXMubnpMYXlvdXR9YF06IHRoaXMubnpMYXlvdXRcbiAgICB9KTtcbiAgfVxuXG4gIGNvbnN0cnVjdG9yKFxuICAgIHByaXZhdGUgZWxlbWVudFJlZjogRWxlbWVudFJlZixcbiAgICBwcml2YXRlIHJlbmRlcmVyOiBSZW5kZXJlcjIsXG4gICAgcHJpdmF0ZSBuelVwZGF0ZUhvc3RDbGFzc1NlcnZpY2U6IE56VXBkYXRlSG9zdENsYXNzU2VydmljZVxuICApIHtcbiAgICB0aGlzLnJlbmRlcmVyLmFkZENsYXNzKGVsZW1lbnRSZWYubmF0aXZlRWxlbWVudCwgJ2FudC1mb3JtJyk7XG4gIH1cblxuICBuZ09uSW5pdCgpOiB2b2lkIHtcbiAgICB0aGlzLnNldENsYXNzTWFwKCk7XG4gIH1cblxuICBuZ09uQ2hhbmdlcygpOiB2b2lkIHtcbiAgICB0aGlzLnNldENsYXNzTWFwKCk7XG4gIH1cbn1cbiJdfQ==