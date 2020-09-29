/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { ChangeDetectionStrategy, Component, ElementRef, Input, ViewEncapsulation } from '@angular/core';
import { NzUpdateHostClassService } from '../core/services/update-host-class.service';
export class NzButtonGroupComponent {
    /**
     * @param {?} nzUpdateHostClassService
     * @param {?} elementRef
     */
    constructor(nzUpdateHostClassService, elementRef) {
        this.nzUpdateHostClassService = nzUpdateHostClassService;
        this.elementRef = elementRef;
        this.prefixCls = 'ant-btn-group';
    }
    /**
     * @return {?}
     */
    get nzSize() {
        return this._size;
    }
    /**
     * @param {?} value
     * @return {?}
     */
    set nzSize(value) {
        this._size = value;
        this.setClassMap();
    }
    /**
     * @return {?}
     */
    setClassMap() {
        /** @type {?} */
        const classMap = {
            [this.prefixCls]: true,
            [`${this.prefixCls}-lg`]: this.nzSize === 'large',
            [`${this.prefixCls}-sm`]: this.nzSize === 'small'
        };
        this.nzUpdateHostClassService.updateHostClass(this.elementRef.nativeElement, classMap);
    }
    /**
     * @return {?}
     */
    ngOnInit() {
        this.setClassMap();
    }
}
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
NzButtonGroupComponent.ctorParameters = () => [
    { type: NzUpdateHostClassService },
    { type: ElementRef }
];
NzButtonGroupComponent.propDecorators = {
    nzSize: [{ type: Input }]
};
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
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotYnV0dG9uLWdyb3VwLmNvbXBvbmVudC5qcyIsInNvdXJjZVJvb3QiOiJuZzovL25nLXpvcnJvLWFudGQvIiwic291cmNlcyI6WyJidXR0b24vbnotYnV0dG9uLWdyb3VwLmNvbXBvbmVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7O0FBQUEsT0FBTyxFQUFFLHVCQUF1QixFQUFFLFNBQVMsRUFBRSxVQUFVLEVBQUUsS0FBSyxFQUFVLGlCQUFpQixFQUFFLE1BQU0sZUFBZSxDQUFDO0FBQ2pILE9BQU8sRUFBRSx3QkFBd0IsRUFBRSxNQUFNLDRDQUE0QyxDQUFDO0FBV3RGLE1BQU0sT0FBTyxzQkFBc0I7Ozs7O0lBV2pDLFlBQW9CLHdCQUFrRCxFQUFVLFVBQXNCO1FBQWxGLDZCQUF3QixHQUF4Qix3QkFBd0IsQ0FBMEI7UUFBVSxlQUFVLEdBQVYsVUFBVSxDQUFZO1FBRzlGLGNBQVMsR0FBRyxlQUFlLENBQUM7SUFIcUUsQ0FBQzs7OztJQVYxRyxJQUNJLE1BQU07UUFDUixPQUFPLElBQUksQ0FBQyxLQUFLLENBQUM7SUFDcEIsQ0FBQzs7Ozs7SUFFRCxJQUFJLE1BQU0sQ0FBQyxLQUFvQjtRQUM3QixJQUFJLENBQUMsS0FBSyxHQUFHLEtBQUssQ0FBQztRQUNuQixJQUFJLENBQUMsV0FBVyxFQUFFLENBQUM7SUFDckIsQ0FBQzs7OztJQU9ELFdBQVc7O2NBQ0gsUUFBUSxHQUFHO1lBQ2YsQ0FBQyxJQUFJLENBQUMsU0FBUyxDQUFDLEVBQUUsSUFBSTtZQUN0QixDQUFDLEdBQUcsSUFBSSxDQUFDLFNBQVMsS0FBSyxDQUFDLEVBQUUsSUFBSSxDQUFDLE1BQU0sS0FBSyxPQUFPO1lBQ2pELENBQUMsR0FBRyxJQUFJLENBQUMsU0FBUyxLQUFLLENBQUMsRUFBRSxJQUFJLENBQUMsTUFBTSxLQUFLLE9BQU87U0FDbEQ7UUFDRCxJQUFJLENBQUMsd0JBQXdCLENBQUMsZUFBZSxDQUFDLElBQUksQ0FBQyxVQUFVLENBQUMsYUFBYSxFQUFFLFFBQVEsQ0FBQyxDQUFDO0lBQ3pGLENBQUM7Ozs7SUFFRCxRQUFRO1FBQ04sSUFBSSxDQUFDLFdBQVcsRUFBRSxDQUFDO0lBQ3JCLENBQUM7OztZQW5DRixTQUFTLFNBQUM7Z0JBQ1QsUUFBUSxFQUFFLGlCQUFpQjtnQkFDM0IsZUFBZSxFQUFFLHVCQUF1QixDQUFDLE1BQU07Z0JBQy9DLGFBQWEsRUFBRSxpQkFBaUIsQ0FBQyxJQUFJO2dCQUNyQyxtQkFBbUIsRUFBRSxLQUFLO2dCQUMxQixTQUFTLEVBQUUsQ0FBQyx3QkFBd0IsQ0FBQztnQkFDckMsdUNBQStDO2FBQ2hEOzs7O1lBVlEsd0JBQXdCO1lBRFksVUFBVTs7O3FCQWFwRCxLQUFLOzs7Ozs7O0lBWU4sdUNBQTZCOzs7OztJQUM3QiwyQ0FBb0M7Ozs7O0lBSHhCLDBEQUEwRDs7Ozs7SUFBRSw0Q0FBOEIiLCJzb3VyY2VzQ29udGVudCI6WyJpbXBvcnQgeyBDaGFuZ2VEZXRlY3Rpb25TdHJhdGVneSwgQ29tcG9uZW50LCBFbGVtZW50UmVmLCBJbnB1dCwgT25Jbml0LCBWaWV3RW5jYXBzdWxhdGlvbiB9IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuaW1wb3J0IHsgTnpVcGRhdGVIb3N0Q2xhc3NTZXJ2aWNlIH0gZnJvbSAnLi4vY29yZS9zZXJ2aWNlcy91cGRhdGUtaG9zdC1jbGFzcy5zZXJ2aWNlJztcbmltcG9ydCB7IE56U2l6ZUxEU1R5cGUgfSBmcm9tICcuLi9jb3JlL3R5cGVzL3NpemUnO1xuXG5AQ29tcG9uZW50KHtcbiAgc2VsZWN0b3I6ICduei1idXR0b24tZ3JvdXAnLFxuICBjaGFuZ2VEZXRlY3Rpb246IENoYW5nZURldGVjdGlvblN0cmF0ZWd5Lk9uUHVzaCxcbiAgZW5jYXBzdWxhdGlvbjogVmlld0VuY2Fwc3VsYXRpb24uTm9uZSxcbiAgcHJlc2VydmVXaGl0ZXNwYWNlczogZmFsc2UsXG4gIHByb3ZpZGVyczogW056VXBkYXRlSG9zdENsYXNzU2VydmljZV0sXG4gIHRlbXBsYXRlVXJsOiAnLi9uei1idXR0b24tZ3JvdXAuY29tcG9uZW50Lmh0bWwnXG59KVxuZXhwb3J0IGNsYXNzIE56QnV0dG9uR3JvdXBDb21wb25lbnQgaW1wbGVtZW50cyBPbkluaXQge1xuICBASW5wdXQoKVxuICBnZXQgbnpTaXplKCk6IE56U2l6ZUxEU1R5cGUge1xuICAgIHJldHVybiB0aGlzLl9zaXplO1xuICB9XG5cbiAgc2V0IG56U2l6ZSh2YWx1ZTogTnpTaXplTERTVHlwZSkge1xuICAgIHRoaXMuX3NpemUgPSB2YWx1ZTtcbiAgICB0aGlzLnNldENsYXNzTWFwKCk7XG4gIH1cblxuICBjb25zdHJ1Y3Rvcihwcml2YXRlIG56VXBkYXRlSG9zdENsYXNzU2VydmljZTogTnpVcGRhdGVIb3N0Q2xhc3NTZXJ2aWNlLCBwcml2YXRlIGVsZW1lbnRSZWY6IEVsZW1lbnRSZWYpIHt9XG5cbiAgcHJpdmF0ZSBfc2l6ZTogTnpTaXplTERTVHlwZTtcbiAgcHJpdmF0ZSBwcmVmaXhDbHMgPSAnYW50LWJ0bi1ncm91cCc7XG5cbiAgc2V0Q2xhc3NNYXAoKTogdm9pZCB7XG4gICAgY29uc3QgY2xhc3NNYXAgPSB7XG4gICAgICBbdGhpcy5wcmVmaXhDbHNdOiB0cnVlLFxuICAgICAgW2Ake3RoaXMucHJlZml4Q2xzfS1sZ2BdOiB0aGlzLm56U2l6ZSA9PT0gJ2xhcmdlJyxcbiAgICAgIFtgJHt0aGlzLnByZWZpeENsc30tc21gXTogdGhpcy5uelNpemUgPT09ICdzbWFsbCdcbiAgICB9O1xuICAgIHRoaXMubnpVcGRhdGVIb3N0Q2xhc3NTZXJ2aWNlLnVwZGF0ZUhvc3RDbGFzcyh0aGlzLmVsZW1lbnRSZWYubmF0aXZlRWxlbWVudCwgY2xhc3NNYXApO1xuICB9XG5cbiAgbmdPbkluaXQoKTogdm9pZCB7XG4gICAgdGhpcy5zZXRDbGFzc01hcCgpO1xuICB9XG59XG4iXX0=