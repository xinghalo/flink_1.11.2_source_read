/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { ChangeDetectionStrategy, Component, ElementRef, Input, ViewEncapsulation } from '@angular/core';
import { NzUpdateHostClassService } from '../core/services/update-host-class.service';
var NzButtonGroupComponent = /** @class */ (function () {
    function NzButtonGroupComponent(nzUpdateHostClassService, elementRef) {
        this.nzUpdateHostClassService = nzUpdateHostClassService;
        this.elementRef = elementRef;
        this.prefixCls = 'ant-btn-group';
    }
    Object.defineProperty(NzButtonGroupComponent.prototype, "nzSize", {
        get: /**
         * @return {?}
         */
        function () {
            return this._size;
        },
        set: /**
         * @param {?} value
         * @return {?}
         */
        function (value) {
            this._size = value;
            this.setClassMap();
        },
        enumerable: true,
        configurable: true
    });
    /**
     * @return {?}
     */
    NzButtonGroupComponent.prototype.setClassMap = /**
     * @return {?}
     */
    function () {
        var _a;
        /** @type {?} */
        var classMap = (_a = {},
            _a[this.prefixCls] = true,
            _a[this.prefixCls + "-lg"] = this.nzSize === 'large',
            _a[this.prefixCls + "-sm"] = this.nzSize === 'small',
            _a);
        this.nzUpdateHostClassService.updateHostClass(this.elementRef.nativeElement, classMap);
    };
    /**
     * @return {?}
     */
    NzButtonGroupComponent.prototype.ngOnInit = /**
     * @return {?}
     */
    function () {
        this.setClassMap();
    };
    NzButtonGroupComponent.decorators = [
        { type: Component, args: [{
                    selector: 'nz-button-group',
                    changeDetection: ChangeDetectionStrategy.OnPush,
                    encapsulation: ViewEncapsulation.None,
                    preserveWhitespaces: false,
                    providers: [NzUpdateHostClassService],
                    template: "<ng-content></ng-content>\n"
                }] }
    ];
    /** @nocollapse */
    NzButtonGroupComponent.ctorParameters = function () { return [
        { type: NzUpdateHostClassService },
        { type: ElementRef }
    ]; };
    NzButtonGroupComponent.propDecorators = {
        nzSize: [{ type: Input }]
    };
    return NzButtonGroupComponent;
}());
export { NzButtonGroupComponent };
if (false) {
    /**
     * @type {?}
     * @private
     */
    NzButtonGroupComponent.prototype._size;
    /**
     * @type {?}
     * @private
     */
    NzButtonGroupComponent.prototype.prefixCls;
    /**
     * @type {?}
     * @private
     */
    NzButtonGroupComponent.prototype.nzUpdateHostClassService;
    /**
     * @type {?}
     * @private
     */
    NzButtonGroupComponent.prototype.elementRef;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotYnV0dG9uLWdyb3VwLmNvbXBvbmVudC5qcyIsInNvdXJjZVJvb3QiOiJuZzovL25nLXpvcnJvLWFudGQvIiwic291cmNlcyI6WyJidXR0b24vbnotYnV0dG9uLWdyb3VwLmNvbXBvbmVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7O0FBQUEsT0FBTyxFQUFFLHVCQUF1QixFQUFFLFNBQVMsRUFBRSxVQUFVLEVBQUUsS0FBSyxFQUFVLGlCQUFpQixFQUFFLE1BQU0sZUFBZSxDQUFDO0FBQ2pILE9BQU8sRUFBRSx3QkFBd0IsRUFBRSxNQUFNLDRDQUE0QyxDQUFDO0FBR3RGO0lBbUJFLGdDQUFvQix3QkFBa0QsRUFBVSxVQUFzQjtRQUFsRiw2QkFBd0IsR0FBeEIsd0JBQXdCLENBQTBCO1FBQVUsZUFBVSxHQUFWLFVBQVUsQ0FBWTtRQUc5RixjQUFTLEdBQUcsZUFBZSxDQUFDO0lBSHFFLENBQUM7SUFWMUcsc0JBQ0ksMENBQU07Ozs7UUFEVjtZQUVFLE9BQU8sSUFBSSxDQUFDLEtBQUssQ0FBQztRQUNwQixDQUFDOzs7OztRQUVELFVBQVcsS0FBb0I7WUFDN0IsSUFBSSxDQUFDLEtBQUssR0FBRyxLQUFLLENBQUM7WUFDbkIsSUFBSSxDQUFDLFdBQVcsRUFBRSxDQUFDO1FBQ3JCLENBQUM7OztPQUxBOzs7O0lBWUQsNENBQVc7OztJQUFYOzs7WUFDUSxRQUFRO1lBQ1osR0FBQyxJQUFJLENBQUMsU0FBUyxJQUFHLElBQUk7WUFDdEIsR0FBSSxJQUFJLENBQUMsU0FBUyxRQUFLLElBQUcsSUFBSSxDQUFDLE1BQU0sS0FBSyxPQUFPO1lBQ2pELEdBQUksSUFBSSxDQUFDLFNBQVMsUUFBSyxJQUFHLElBQUksQ0FBQyxNQUFNLEtBQUssT0FBTztlQUNsRDtRQUNELElBQUksQ0FBQyx3QkFBd0IsQ0FBQyxlQUFlLENBQUMsSUFBSSxDQUFDLFVBQVUsQ0FBQyxhQUFhLEVBQUUsUUFBUSxDQUFDLENBQUM7SUFDekYsQ0FBQzs7OztJQUVELHlDQUFROzs7SUFBUjtRQUNFLElBQUksQ0FBQyxXQUFXLEVBQUUsQ0FBQztJQUNyQixDQUFDOztnQkFuQ0YsU0FBUyxTQUFDO29CQUNULFFBQVEsRUFBRSxpQkFBaUI7b0JBQzNCLGVBQWUsRUFBRSx1QkFBdUIsQ0FBQyxNQUFNO29CQUMvQyxhQUFhLEVBQUUsaUJBQWlCLENBQUMsSUFBSTtvQkFDckMsbUJBQW1CLEVBQUUsS0FBSztvQkFDMUIsU0FBUyxFQUFFLENBQUMsd0JBQXdCLENBQUM7b0JBQ3JDLHVDQUErQztpQkFDaEQ7Ozs7Z0JBVlEsd0JBQXdCO2dCQURZLFVBQVU7Ozt5QkFhcEQsS0FBSzs7SUEyQlIsNkJBQUM7Q0FBQSxBQXBDRCxJQW9DQztTQTVCWSxzQkFBc0I7Ozs7OztJQWFqQyx1Q0FBNkI7Ozs7O0lBQzdCLDJDQUFvQzs7Ozs7SUFIeEIsMERBQTBEOzs7OztJQUFFLDRDQUE4QiIsInNvdXJjZXNDb250ZW50IjpbImltcG9ydCB7IENoYW5nZURldGVjdGlvblN0cmF0ZWd5LCBDb21wb25lbnQsIEVsZW1lbnRSZWYsIElucHV0LCBPbkluaXQsIFZpZXdFbmNhcHN1bGF0aW9uIH0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5pbXBvcnQgeyBOelVwZGF0ZUhvc3RDbGFzc1NlcnZpY2UgfSBmcm9tICcuLi9jb3JlL3NlcnZpY2VzL3VwZGF0ZS1ob3N0LWNsYXNzLnNlcnZpY2UnO1xuaW1wb3J0IHsgTnpTaXplTERTVHlwZSB9IGZyb20gJy4uL2NvcmUvdHlwZXMvc2l6ZSc7XG5cbkBDb21wb25lbnQoe1xuICBzZWxlY3RvcjogJ256LWJ1dHRvbi1ncm91cCcsXG4gIGNoYW5nZURldGVjdGlvbjogQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3kuT25QdXNoLFxuICBlbmNhcHN1bGF0aW9uOiBWaWV3RW5jYXBzdWxhdGlvbi5Ob25lLFxuICBwcmVzZXJ2ZVdoaXRlc3BhY2VzOiBmYWxzZSxcbiAgcHJvdmlkZXJzOiBbTnpVcGRhdGVIb3N0Q2xhc3NTZXJ2aWNlXSxcbiAgdGVtcGxhdGVVcmw6ICcuL256LWJ1dHRvbi1ncm91cC5jb21wb25lbnQuaHRtbCdcbn0pXG5leHBvcnQgY2xhc3MgTnpCdXR0b25Hcm91cENvbXBvbmVudCBpbXBsZW1lbnRzIE9uSW5pdCB7XG4gIEBJbnB1dCgpXG4gIGdldCBuelNpemUoKTogTnpTaXplTERTVHlwZSB7XG4gICAgcmV0dXJuIHRoaXMuX3NpemU7XG4gIH1cblxuICBzZXQgbnpTaXplKHZhbHVlOiBOelNpemVMRFNUeXBlKSB7XG4gICAgdGhpcy5fc2l6ZSA9IHZhbHVlO1xuICAgIHRoaXMuc2V0Q2xhc3NNYXAoKTtcbiAgfVxuXG4gIGNvbnN0cnVjdG9yKHByaXZhdGUgbnpVcGRhdGVIb3N0Q2xhc3NTZXJ2aWNlOiBOelVwZGF0ZUhvc3RDbGFzc1NlcnZpY2UsIHByaXZhdGUgZWxlbWVudFJlZjogRWxlbWVudFJlZikge31cblxuICBwcml2YXRlIF9zaXplOiBOelNpemVMRFNUeXBlO1xuICBwcml2YXRlIHByZWZpeENscyA9ICdhbnQtYnRuLWdyb3VwJztcblxuICBzZXRDbGFzc01hcCgpOiB2b2lkIHtcbiAgICBjb25zdCBjbGFzc01hcCA9IHtcbiAgICAgIFt0aGlzLnByZWZpeENsc106IHRydWUsXG4gICAgICBbYCR7dGhpcy5wcmVmaXhDbHN9LWxnYF06IHRoaXMubnpTaXplID09PSAnbGFyZ2UnLFxuICAgICAgW2Ake3RoaXMucHJlZml4Q2xzfS1zbWBdOiB0aGlzLm56U2l6ZSA9PT0gJ3NtYWxsJ1xuICAgIH07XG4gICAgdGhpcy5uelVwZGF0ZUhvc3RDbGFzc1NlcnZpY2UudXBkYXRlSG9zdENsYXNzKHRoaXMuZWxlbWVudFJlZi5uYXRpdmVFbGVtZW50LCBjbGFzc01hcCk7XG4gIH1cblxuICBuZ09uSW5pdCgpOiB2b2lkIHtcbiAgICB0aGlzLnNldENsYXNzTWFwKCk7XG4gIH1cbn1cbiJdfQ==