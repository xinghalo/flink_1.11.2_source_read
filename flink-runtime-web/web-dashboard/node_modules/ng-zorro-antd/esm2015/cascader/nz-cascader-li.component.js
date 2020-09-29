/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { ChangeDetectionStrategy, ChangeDetectorRef, Component, ElementRef, Input, Renderer2, ViewEncapsulation } from '@angular/core';
import { DomSanitizer } from '@angular/platform-browser';
export class NzCascaderOptionComponent {
    /**
     * @param {?} sanitizer
     * @param {?} cdr
     * @param {?} elementRef
     * @param {?} renderer
     */
    constructor(sanitizer, cdr, elementRef, renderer) {
        this.sanitizer = sanitizer;
        this.cdr = cdr;
        this.activated = false;
        this.nzLabelProperty = 'label';
        renderer.addClass(elementRef.nativeElement, 'ant-cascader-menu-item');
    }
    /**
     * @return {?}
     */
    getOptionLabel() {
        return this.option ? this.option[this.nzLabelProperty] : '';
    }
    /**
     * @param {?} str
     * @return {?}
     */
    renderHighlightString(str) {
        /** @type {?} */
        const replaceStr = str.replace(new RegExp(this.highlightText, 'g'), `<span class="ant-cascader-menu-item-keyword">${this.highlightText}</span>`);
        return this.sanitizer.bypassSecurityTrustHtml(replaceStr);
    }
    /**
     * @return {?}
     */
    markForCheck() {
        this.cdr.markForCheck();
    }
}
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
NzCascaderOptionComponent.ctorParameters = () => [
    { type: DomSanitizer },
    { type: ChangeDetectorRef },
    { type: ElementRef },
    { type: Renderer2 }
];
NzCascaderOptionComponent.propDecorators = {
    option: [{ type: Input }],
    activated: [{ type: Input }],
    highlightText: [{ type: Input }],
    nzLabelProperty: [{ type: Input }]
};
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
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotY2FzY2FkZXItbGkuY29tcG9uZW50LmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbImNhc2NhZGVyL256LWNhc2NhZGVyLWxpLmNvbXBvbmVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7O0FBQUEsT0FBTyxFQUNMLHVCQUF1QixFQUN2QixpQkFBaUIsRUFDakIsU0FBUyxFQUNULFVBQVUsRUFDVixLQUFLLEVBQ0wsU0FBUyxFQUNULGlCQUFpQixFQUNsQixNQUFNLGVBQWUsQ0FBQztBQUN2QixPQUFPLEVBQUUsWUFBWSxFQUFZLE1BQU0sMkJBQTJCLENBQUM7QUFlbkUsTUFBTSxPQUFPLHlCQUF5Qjs7Ozs7OztJQU1wQyxZQUNVLFNBQXVCLEVBQ3ZCLEdBQXNCLEVBQzlCLFVBQXNCLEVBQ3RCLFFBQW1CO1FBSFgsY0FBUyxHQUFULFNBQVMsQ0FBYztRQUN2QixRQUFHLEdBQUgsR0FBRyxDQUFtQjtRQU52QixjQUFTLEdBQUcsS0FBSyxDQUFDO1FBRWxCLG9CQUFlLEdBQUcsT0FBTyxDQUFDO1FBUWpDLFFBQVEsQ0FBQyxRQUFRLENBQUMsVUFBVSxDQUFDLGFBQWEsRUFBRSx3QkFBd0IsQ0FBQyxDQUFDO0lBQ3hFLENBQUM7Ozs7SUFFRCxjQUFjO1FBQ1osT0FBTyxJQUFJLENBQUMsTUFBTSxDQUFDLENBQUMsQ0FBQyxJQUFJLENBQUMsTUFBTSxDQUFDLElBQUksQ0FBQyxlQUFlLENBQUMsQ0FBQyxDQUFDLENBQUMsRUFBRSxDQUFDO0lBQzlELENBQUM7Ozs7O0lBRUQscUJBQXFCLENBQUMsR0FBVzs7Y0FDekIsVUFBVSxHQUFHLEdBQUcsQ0FBQyxPQUFPLENBQzVCLElBQUksTUFBTSxDQUFDLElBQUksQ0FBQyxhQUFhLEVBQUUsR0FBRyxDQUFDLEVBQ25DLGdEQUFnRCxJQUFJLENBQUMsYUFBYSxTQUFTLENBQzVFO1FBRUQsT0FBTyxJQUFJLENBQUMsU0FBUyxDQUFDLHVCQUF1QixDQUFDLFVBQVUsQ0FBQyxDQUFDO0lBQzVELENBQUM7Ozs7SUFFRCxZQUFZO1FBQ1YsSUFBSSxDQUFDLEdBQUcsQ0FBQyxZQUFZLEVBQUUsQ0FBQztJQUMxQixDQUFDOzs7WUExQ0YsU0FBUyxTQUFDO2dCQUNULGVBQWUsRUFBRSx1QkFBdUIsQ0FBQyxNQUFNO2dCQUMvQyxhQUFhLEVBQUUsaUJBQWlCLENBQUMsSUFBSTtnQkFDckMsUUFBUSxFQUFFLHNCQUFzQjtnQkFDaEMsMmJBQThDO2dCQUM5QyxJQUFJLEVBQUU7b0JBQ0osY0FBYyxFQUFFLGtDQUFrQztvQkFDbEQsdUNBQXVDLEVBQUUsV0FBVztvQkFDcEQsdUNBQXVDLEVBQUUsZ0JBQWdCO29CQUN6RCx5Q0FBeUMsRUFBRSxpQkFBaUI7aUJBQzdEO2FBQ0Y7Ozs7WUFkUSxZQUFZO1lBUG5CLGlCQUFpQjtZQUVqQixVQUFVO1lBRVYsU0FBUzs7O3FCQW1CUixLQUFLO3dCQUNMLEtBQUs7NEJBQ0wsS0FBSzs4QkFDTCxLQUFLOzs7O0lBSE4sMkNBQWdDOztJQUNoQyw4Q0FBMkI7O0lBQzNCLGtEQUErQjs7SUFDL0Isb0RBQW1DOzs7OztJQUdqQyw4Q0FBK0I7Ozs7O0lBQy9CLHdDQUE4QiIsInNvdXJjZXNDb250ZW50IjpbImltcG9ydCB7XG4gIENoYW5nZURldGVjdGlvblN0cmF0ZWd5LFxuICBDaGFuZ2VEZXRlY3RvclJlZixcbiAgQ29tcG9uZW50LFxuICBFbGVtZW50UmVmLFxuICBJbnB1dCxcbiAgUmVuZGVyZXIyLFxuICBWaWV3RW5jYXBzdWxhdGlvblxufSBmcm9tICdAYW5ndWxhci9jb3JlJztcbmltcG9ydCB7IERvbVNhbml0aXplciwgU2FmZUh0bWwgfSBmcm9tICdAYW5ndWxhci9wbGF0Zm9ybS1icm93c2VyJztcbmltcG9ydCB7IENhc2NhZGVyT3B0aW9uIH0gZnJvbSAnLi9uei1jYXNjYWRlci1kZWZpbml0aW9ucyc7XG5cbkBDb21wb25lbnQoe1xuICBjaGFuZ2VEZXRlY3Rpb246IENoYW5nZURldGVjdGlvblN0cmF0ZWd5Lk9uUHVzaCxcbiAgZW5jYXBzdWxhdGlvbjogVmlld0VuY2Fwc3VsYXRpb24uTm9uZSxcbiAgc2VsZWN0b3I6ICdbbnotY2FzY2FkZXItb3B0aW9uXScsXG4gIHRlbXBsYXRlVXJsOiAnLi9uei1jYXNjYWRlci1saS5jb21wb25lbnQuaHRtbCcsXG4gIGhvc3Q6IHtcbiAgICAnW2F0dHIudGl0bGVdJzogJ29wdGlvbi50aXRsZSB8fCBnZXRPcHRpb25MYWJlbCgpJyxcbiAgICAnW2NsYXNzLmFudC1jYXNjYWRlci1tZW51LWl0ZW0tYWN0aXZlXSc6ICdhY3RpdmF0ZWQnLFxuICAgICdbY2xhc3MuYW50LWNhc2NhZGVyLW1lbnUtaXRlbS1leHBhbmRdJzogJyFvcHRpb24uaXNMZWFmJyxcbiAgICAnW2NsYXNzLmFudC1jYXNjYWRlci1tZW51LWl0ZW0tZGlzYWJsZWRdJzogJ29wdGlvbi5kaXNhYmxlZCdcbiAgfVxufSlcbmV4cG9ydCBjbGFzcyBOekNhc2NhZGVyT3B0aW9uQ29tcG9uZW50IHtcbiAgQElucHV0KCkgb3B0aW9uOiBDYXNjYWRlck9wdGlvbjtcbiAgQElucHV0KCkgYWN0aXZhdGVkID0gZmFsc2U7XG4gIEBJbnB1dCgpIGhpZ2hsaWdodFRleHQ6IHN0cmluZztcbiAgQElucHV0KCkgbnpMYWJlbFByb3BlcnR5ID0gJ2xhYmVsJztcblxuICBjb25zdHJ1Y3RvcihcbiAgICBwcml2YXRlIHNhbml0aXplcjogRG9tU2FuaXRpemVyLFxuICAgIHByaXZhdGUgY2RyOiBDaGFuZ2VEZXRlY3RvclJlZixcbiAgICBlbGVtZW50UmVmOiBFbGVtZW50UmVmLFxuICAgIHJlbmRlcmVyOiBSZW5kZXJlcjJcbiAgKSB7XG4gICAgcmVuZGVyZXIuYWRkQ2xhc3MoZWxlbWVudFJlZi5uYXRpdmVFbGVtZW50LCAnYW50LWNhc2NhZGVyLW1lbnUtaXRlbScpO1xuICB9XG5cbiAgZ2V0T3B0aW9uTGFiZWwoKTogc3RyaW5nIHtcbiAgICByZXR1cm4gdGhpcy5vcHRpb24gPyB0aGlzLm9wdGlvblt0aGlzLm56TGFiZWxQcm9wZXJ0eV0gOiAnJztcbiAgfVxuXG4gIHJlbmRlckhpZ2hsaWdodFN0cmluZyhzdHI6IHN0cmluZyk6IFNhZmVIdG1sIHtcbiAgICBjb25zdCByZXBsYWNlU3RyID0gc3RyLnJlcGxhY2UoXG4gICAgICBuZXcgUmVnRXhwKHRoaXMuaGlnaGxpZ2h0VGV4dCwgJ2cnKSxcbiAgICAgIGA8c3BhbiBjbGFzcz1cImFudC1jYXNjYWRlci1tZW51LWl0ZW0ta2V5d29yZFwiPiR7dGhpcy5oaWdobGlnaHRUZXh0fTwvc3Bhbj5gXG4gICAgKTtcblxuICAgIHJldHVybiB0aGlzLnNhbml0aXplci5ieXBhc3NTZWN1cml0eVRydXN0SHRtbChyZXBsYWNlU3RyKTtcbiAgfVxuXG4gIG1hcmtGb3JDaGVjaygpOiB2b2lkIHtcbiAgICB0aGlzLmNkci5tYXJrRm9yQ2hlY2soKTtcbiAgfVxufVxuIl19