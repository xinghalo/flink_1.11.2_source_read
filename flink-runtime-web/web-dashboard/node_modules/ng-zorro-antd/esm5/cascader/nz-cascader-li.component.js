/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { ChangeDetectionStrategy, ChangeDetectorRef, Component, ElementRef, Input, Renderer2, ViewEncapsulation } from '@angular/core';
import { DomSanitizer } from '@angular/platform-browser';
var NzCascaderOptionComponent = /** @class */ (function () {
    function NzCascaderOptionComponent(sanitizer, cdr, elementRef, renderer) {
        this.sanitizer = sanitizer;
        this.cdr = cdr;
        this.activated = false;
        this.nzLabelProperty = 'label';
        renderer.addClass(elementRef.nativeElement, 'ant-cascader-menu-item');
    }
    /**
     * @return {?}
     */
    NzCascaderOptionComponent.prototype.getOptionLabel = /**
     * @return {?}
     */
    function () {
        return this.option ? this.option[this.nzLabelProperty] : '';
    };
    /**
     * @param {?} str
     * @return {?}
     */
    NzCascaderOptionComponent.prototype.renderHighlightString = /**
     * @param {?} str
     * @return {?}
     */
    function (str) {
        /** @type {?} */
        var replaceStr = str.replace(new RegExp(this.highlightText, 'g'), "<span class=\"ant-cascader-menu-item-keyword\">" + this.highlightText + "</span>");
        return this.sanitizer.bypassSecurityTrustHtml(replaceStr);
    };
    /**
     * @return {?}
     */
    NzCascaderOptionComponent.prototype.markForCheck = /**
     * @return {?}
     */
    function () {
        this.cdr.markForCheck();
    };
    NzCascaderOptionComponent.decorators = [
        { type: Component, args: [{
                    changeDetection: ChangeDetectionStrategy.OnPush,
                    encapsulation: ViewEncapsulation.None,
                    selector: '[nz-cascader-option]',
                    template: "<ng-container *ngIf=\"highlightText\">\n  <span [innerHTML]=\"renderHighlightString(getOptionLabel())\"></span>\n</ng-container>\n<ng-container *ngIf=\"!highlightText\">{{ getOptionLabel() }}</ng-container>\n<span\n  *ngIf=\"!option.isLeaf || option.children && option.children.length || option.loading\"\n  class=\"ant-cascader-menu-item-expand-icon\">\n  <i nz-icon [type]=\"option.loading ? 'loading' : 'right'\"></i>\n</span>\n",
                    host: {
                        '[attr.title]': 'option.title || getOptionLabel()',
                        '[class.ant-cascader-menu-item-active]': 'activated',
                        '[class.ant-cascader-menu-item-expand]': '!option.isLeaf',
                        '[class.ant-cascader-menu-item-disabled]': 'option.disabled'
                    }
                }] }
    ];
    /** @nocollapse */
    NzCascaderOptionComponent.ctorParameters = function () { return [
        { type: DomSanitizer },
        { type: ChangeDetectorRef },
        { type: ElementRef },
        { type: Renderer2 }
    ]; };
    NzCascaderOptionComponent.propDecorators = {
        option: [{ type: Input }],
        activated: [{ type: Input }],
        highlightText: [{ type: Input }],
        nzLabelProperty: [{ type: Input }]
    };
    return NzCascaderOptionComponent;
}());
export { NzCascaderOptionComponent };
if (false) {
    /** @type {?} */
    NzCascaderOptionComponent.prototype.option;
    /** @type {?} */
    NzCascaderOptionComponent.prototype.activated;
    /** @type {?} */
    NzCascaderOptionComponent.prototype.highlightText;
    /** @type {?} */
    NzCascaderOptionComponent.prototype.nzLabelProperty;
    /**
     * @type {?}
     * @private
     */
    NzCascaderOptionComponent.prototype.sanitizer;
    /**
     * @type {?}
     * @private
     */
    NzCascaderOptionComponent.prototype.cdr;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotY2FzY2FkZXItbGkuY29tcG9uZW50LmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbImNhc2NhZGVyL256LWNhc2NhZGVyLWxpLmNvbXBvbmVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7O0FBQUEsT0FBTyxFQUNMLHVCQUF1QixFQUN2QixpQkFBaUIsRUFDakIsU0FBUyxFQUNULFVBQVUsRUFDVixLQUFLLEVBQ0wsU0FBUyxFQUNULGlCQUFpQixFQUNsQixNQUFNLGVBQWUsQ0FBQztBQUN2QixPQUFPLEVBQUUsWUFBWSxFQUFZLE1BQU0sMkJBQTJCLENBQUM7QUFHbkU7SUFrQkUsbUNBQ1UsU0FBdUIsRUFDdkIsR0FBc0IsRUFDOUIsVUFBc0IsRUFDdEIsUUFBbUI7UUFIWCxjQUFTLEdBQVQsU0FBUyxDQUFjO1FBQ3ZCLFFBQUcsR0FBSCxHQUFHLENBQW1CO1FBTnZCLGNBQVMsR0FBRyxLQUFLLENBQUM7UUFFbEIsb0JBQWUsR0FBRyxPQUFPLENBQUM7UUFRakMsUUFBUSxDQUFDLFFBQVEsQ0FBQyxVQUFVLENBQUMsYUFBYSxFQUFFLHdCQUF3QixDQUFDLENBQUM7SUFDeEUsQ0FBQzs7OztJQUVELGtEQUFjOzs7SUFBZDtRQUNFLE9BQU8sSUFBSSxDQUFDLE1BQU0sQ0FBQyxDQUFDLENBQUMsSUFBSSxDQUFDLE1BQU0sQ0FBQyxJQUFJLENBQUMsZUFBZSxDQUFDLENBQUMsQ0FBQyxDQUFDLEVBQUUsQ0FBQztJQUM5RCxDQUFDOzs7OztJQUVELHlEQUFxQjs7OztJQUFyQixVQUFzQixHQUFXOztZQUN6QixVQUFVLEdBQUcsR0FBRyxDQUFDLE9BQU8sQ0FDNUIsSUFBSSxNQUFNLENBQUMsSUFBSSxDQUFDLGFBQWEsRUFBRSxHQUFHLENBQUMsRUFDbkMsb0RBQWdELElBQUksQ0FBQyxhQUFhLFlBQVMsQ0FDNUU7UUFFRCxPQUFPLElBQUksQ0FBQyxTQUFTLENBQUMsdUJBQXVCLENBQUMsVUFBVSxDQUFDLENBQUM7SUFDNUQsQ0FBQzs7OztJQUVELGdEQUFZOzs7SUFBWjtRQUNFLElBQUksQ0FBQyxHQUFHLENBQUMsWUFBWSxFQUFFLENBQUM7SUFDMUIsQ0FBQzs7Z0JBMUNGLFNBQVMsU0FBQztvQkFDVCxlQUFlLEVBQUUsdUJBQXVCLENBQUMsTUFBTTtvQkFDL0MsYUFBYSxFQUFFLGlCQUFpQixDQUFDLElBQUk7b0JBQ3JDLFFBQVEsRUFBRSxzQkFBc0I7b0JBQ2hDLDJiQUE4QztvQkFDOUMsSUFBSSxFQUFFO3dCQUNKLGNBQWMsRUFBRSxrQ0FBa0M7d0JBQ2xELHVDQUF1QyxFQUFFLFdBQVc7d0JBQ3BELHVDQUF1QyxFQUFFLGdCQUFnQjt3QkFDekQseUNBQXlDLEVBQUUsaUJBQWlCO3FCQUM3RDtpQkFDRjs7OztnQkFkUSxZQUFZO2dCQVBuQixpQkFBaUI7Z0JBRWpCLFVBQVU7Z0JBRVYsU0FBUzs7O3lCQW1CUixLQUFLOzRCQUNMLEtBQUs7Z0NBQ0wsS0FBSztrQ0FDTCxLQUFLOztJQTJCUixnQ0FBQztDQUFBLEFBM0NELElBMkNDO1NBL0JZLHlCQUF5Qjs7O0lBQ3BDLDJDQUFnQzs7SUFDaEMsOENBQTJCOztJQUMzQixrREFBK0I7O0lBQy9CLG9EQUFtQzs7Ozs7SUFHakMsOENBQStCOzs7OztJQUMvQix3Q0FBOEIiLCJzb3VyY2VzQ29udGVudCI6WyJpbXBvcnQge1xuICBDaGFuZ2VEZXRlY3Rpb25TdHJhdGVneSxcbiAgQ2hhbmdlRGV0ZWN0b3JSZWYsXG4gIENvbXBvbmVudCxcbiAgRWxlbWVudFJlZixcbiAgSW5wdXQsXG4gIFJlbmRlcmVyMixcbiAgVmlld0VuY2Fwc3VsYXRpb25cbn0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5pbXBvcnQgeyBEb21TYW5pdGl6ZXIsIFNhZmVIdG1sIH0gZnJvbSAnQGFuZ3VsYXIvcGxhdGZvcm0tYnJvd3Nlcic7XG5pbXBvcnQgeyBDYXNjYWRlck9wdGlvbiB9IGZyb20gJy4vbnotY2FzY2FkZXItZGVmaW5pdGlvbnMnO1xuXG5AQ29tcG9uZW50KHtcbiAgY2hhbmdlRGV0ZWN0aW9uOiBDaGFuZ2VEZXRlY3Rpb25TdHJhdGVneS5PblB1c2gsXG4gIGVuY2Fwc3VsYXRpb246IFZpZXdFbmNhcHN1bGF0aW9uLk5vbmUsXG4gIHNlbGVjdG9yOiAnW256LWNhc2NhZGVyLW9wdGlvbl0nLFxuICB0ZW1wbGF0ZVVybDogJy4vbnotY2FzY2FkZXItbGkuY29tcG9uZW50Lmh0bWwnLFxuICBob3N0OiB7XG4gICAgJ1thdHRyLnRpdGxlXSc6ICdvcHRpb24udGl0bGUgfHwgZ2V0T3B0aW9uTGFiZWwoKScsXG4gICAgJ1tjbGFzcy5hbnQtY2FzY2FkZXItbWVudS1pdGVtLWFjdGl2ZV0nOiAnYWN0aXZhdGVkJyxcbiAgICAnW2NsYXNzLmFudC1jYXNjYWRlci1tZW51LWl0ZW0tZXhwYW5kXSc6ICchb3B0aW9uLmlzTGVhZicsXG4gICAgJ1tjbGFzcy5hbnQtY2FzY2FkZXItbWVudS1pdGVtLWRpc2FibGVkXSc6ICdvcHRpb24uZGlzYWJsZWQnXG4gIH1cbn0pXG5leHBvcnQgY2xhc3MgTnpDYXNjYWRlck9wdGlvbkNvbXBvbmVudCB7XG4gIEBJbnB1dCgpIG9wdGlvbjogQ2FzY2FkZXJPcHRpb247XG4gIEBJbnB1dCgpIGFjdGl2YXRlZCA9IGZhbHNlO1xuICBASW5wdXQoKSBoaWdobGlnaHRUZXh0OiBzdHJpbmc7XG4gIEBJbnB1dCgpIG56TGFiZWxQcm9wZXJ0eSA9ICdsYWJlbCc7XG5cbiAgY29uc3RydWN0b3IoXG4gICAgcHJpdmF0ZSBzYW5pdGl6ZXI6IERvbVNhbml0aXplcixcbiAgICBwcml2YXRlIGNkcjogQ2hhbmdlRGV0ZWN0b3JSZWYsXG4gICAgZWxlbWVudFJlZjogRWxlbWVudFJlZixcbiAgICByZW5kZXJlcjogUmVuZGVyZXIyXG4gICkge1xuICAgIHJlbmRlcmVyLmFkZENsYXNzKGVsZW1lbnRSZWYubmF0aXZlRWxlbWVudCwgJ2FudC1jYXNjYWRlci1tZW51LWl0ZW0nKTtcbiAgfVxuXG4gIGdldE9wdGlvbkxhYmVsKCk6IHN0cmluZyB7XG4gICAgcmV0dXJuIHRoaXMub3B0aW9uID8gdGhpcy5vcHRpb25bdGhpcy5uekxhYmVsUHJvcGVydHldIDogJyc7XG4gIH1cblxuICByZW5kZXJIaWdobGlnaHRTdHJpbmcoc3RyOiBzdHJpbmcpOiBTYWZlSHRtbCB7XG4gICAgY29uc3QgcmVwbGFjZVN0ciA9IHN0ci5yZXBsYWNlKFxuICAgICAgbmV3IFJlZ0V4cCh0aGlzLmhpZ2hsaWdodFRleHQsICdnJyksXG4gICAgICBgPHNwYW4gY2xhc3M9XCJhbnQtY2FzY2FkZXItbWVudS1pdGVtLWtleXdvcmRcIj4ke3RoaXMuaGlnaGxpZ2h0VGV4dH08L3NwYW4+YFxuICAgICk7XG5cbiAgICByZXR1cm4gdGhpcy5zYW5pdGl6ZXIuYnlwYXNzU2VjdXJpdHlUcnVzdEh0bWwocmVwbGFjZVN0cik7XG4gIH1cblxuICBtYXJrRm9yQ2hlY2soKTogdm9pZCB7XG4gICAgdGhpcy5jZHIubWFya0ZvckNoZWNrKCk7XG4gIH1cbn1cbiJdfQ==