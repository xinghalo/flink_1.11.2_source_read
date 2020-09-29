/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { ChangeDetectionStrategy, Component, ElementRef, Renderer2, ViewEncapsulation } from '@angular/core';
var NzLayoutComponent = /** @class */ (function () {
    function NzLayoutComponent(elementRef, renderer) {
        this.elementRef = elementRef;
        this.renderer = renderer;
        renderer.addClass(elementRef.nativeElement, 'ant-layout');
    }
    /**
     * @return {?}
     */
    NzLayoutComponent.prototype.destroySider = /**
     * @return {?}
     */
    function () {
        this.renderer.removeClass(this.elementRef.nativeElement, 'ant-layout-has-sider');
    };
    /**
     * @return {?}
     */
    NzLayoutComponent.prototype.initSider = /**
     * @return {?}
     */
    function () {
        this.renderer.addClass(this.elementRef.nativeElement, 'ant-layout-has-sider');
    };
    NzLayoutComponent.decorators = [
        { type: Component, args: [{
                    selector: 'nz-layout',
                    encapsulation: ViewEncapsulation.None,
                    changeDetection: ChangeDetectionStrategy.OnPush,
                    preserveWhitespaces: false,
                    template: "<ng-content></ng-content>"
                }] }
    ];
    /** @nocollapse */
    NzLayoutComponent.ctorParameters = function () { return [
        { type: ElementRef },
        { type: Renderer2 }
    ]; };
    return NzLayoutComponent;
}());
export { NzLayoutComponent };
if (false) {
    /**
     * @type {?}
     * @private
     */
    NzLayoutComponent.prototype.elementRef;
    /**
     * @type {?}
     * @private
     */
    NzLayoutComponent.prototype.renderer;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotbGF5b3V0LmNvbXBvbmVudC5qcyIsInNvdXJjZVJvb3QiOiJuZzovL25nLXpvcnJvLWFudGQvIiwic291cmNlcyI6WyJsYXlvdXQvbnotbGF5b3V0LmNvbXBvbmVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7O0FBQUEsT0FBTyxFQUFFLHVCQUF1QixFQUFFLFNBQVMsRUFBRSxVQUFVLEVBQUUsU0FBUyxFQUFFLGlCQUFpQixFQUFFLE1BQU0sZUFBZSxDQUFDO0FBRTdHO0lBZ0JFLDJCQUFvQixVQUFzQixFQUFVLFFBQW1CO1FBQW5ELGVBQVUsR0FBVixVQUFVLENBQVk7UUFBVSxhQUFRLEdBQVIsUUFBUSxDQUFXO1FBQ3JFLFFBQVEsQ0FBQyxRQUFRLENBQUMsVUFBVSxDQUFDLGFBQWEsRUFBRSxZQUFZLENBQUMsQ0FBQztJQUM1RCxDQUFDOzs7O0lBVkQsd0NBQVk7OztJQUFaO1FBQ0UsSUFBSSxDQUFDLFFBQVEsQ0FBQyxXQUFXLENBQUMsSUFBSSxDQUFDLFVBQVUsQ0FBQyxhQUFhLEVBQUUsc0JBQXNCLENBQUMsQ0FBQztJQUNuRixDQUFDOzs7O0lBRUQscUNBQVM7OztJQUFUO1FBQ0UsSUFBSSxDQUFDLFFBQVEsQ0FBQyxRQUFRLENBQUMsSUFBSSxDQUFDLFVBQVUsQ0FBQyxhQUFhLEVBQUUsc0JBQXNCLENBQUMsQ0FBQztJQUNoRixDQUFDOztnQkFkRixTQUFTLFNBQUM7b0JBQ1QsUUFBUSxFQUFFLFdBQVc7b0JBQ3JCLGFBQWEsRUFBRSxpQkFBaUIsQ0FBQyxJQUFJO29CQUNyQyxlQUFlLEVBQUUsdUJBQXVCLENBQUMsTUFBTTtvQkFDL0MsbUJBQW1CLEVBQUUsS0FBSztvQkFDMUIscUNBQXlDO2lCQUMxQzs7OztnQkFSNEMsVUFBVTtnQkFBRSxTQUFTOztJQXFCbEUsd0JBQUM7Q0FBQSxBQW5CRCxJQW1CQztTQVpZLGlCQUFpQjs7Ozs7O0lBU2hCLHVDQUE4Qjs7Ozs7SUFBRSxxQ0FBMkIiLCJzb3VyY2VzQ29udGVudCI6WyJpbXBvcnQgeyBDaGFuZ2VEZXRlY3Rpb25TdHJhdGVneSwgQ29tcG9uZW50LCBFbGVtZW50UmVmLCBSZW5kZXJlcjIsIFZpZXdFbmNhcHN1bGF0aW9uIH0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5cbkBDb21wb25lbnQoe1xuICBzZWxlY3RvcjogJ256LWxheW91dCcsXG4gIGVuY2Fwc3VsYXRpb246IFZpZXdFbmNhcHN1bGF0aW9uLk5vbmUsXG4gIGNoYW5nZURldGVjdGlvbjogQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3kuT25QdXNoLFxuICBwcmVzZXJ2ZVdoaXRlc3BhY2VzOiBmYWxzZSxcbiAgdGVtcGxhdGVVcmw6ICcuL256LWxheW91dC5jb21wb25lbnQuaHRtbCdcbn0pXG5leHBvcnQgY2xhc3MgTnpMYXlvdXRDb21wb25lbnQge1xuICBkZXN0cm95U2lkZXIoKTogdm9pZCB7XG4gICAgdGhpcy5yZW5kZXJlci5yZW1vdmVDbGFzcyh0aGlzLmVsZW1lbnRSZWYubmF0aXZlRWxlbWVudCwgJ2FudC1sYXlvdXQtaGFzLXNpZGVyJyk7XG4gIH1cblxuICBpbml0U2lkZXIoKTogdm9pZCB7XG4gICAgdGhpcy5yZW5kZXJlci5hZGRDbGFzcyh0aGlzLmVsZW1lbnRSZWYubmF0aXZlRWxlbWVudCwgJ2FudC1sYXlvdXQtaGFzLXNpZGVyJyk7XG4gIH1cblxuICBjb25zdHJ1Y3Rvcihwcml2YXRlIGVsZW1lbnRSZWY6IEVsZW1lbnRSZWYsIHByaXZhdGUgcmVuZGVyZXI6IFJlbmRlcmVyMikge1xuICAgIHJlbmRlcmVyLmFkZENsYXNzKGVsZW1lbnRSZWYubmF0aXZlRWxlbWVudCwgJ2FudC1sYXlvdXQnKTtcbiAgfVxufVxuIl19