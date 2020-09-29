/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { ChangeDetectionStrategy, Component, ContentChild, ElementRef, EventEmitter, Input, Output, TemplateRef, ViewEncapsulation } from '@angular/core';
import { NzPageHeaderFooterDirective } from './nz-page-header-cells';
var NzPageHeaderComponent = /** @class */ (function () {
    function NzPageHeaderComponent() {
        this.isTemplateRefBackIcon = false;
        this.isStringBackIcon = false;
        this.nzBackIcon = null;
        this.nzBack = new EventEmitter();
    }
    /**
     * @return {?}
     */
    NzPageHeaderComponent.prototype.ngOnInit = /**
     * @return {?}
     */
    function () { };
    /**
     * @param {?} changes
     * @return {?}
     */
    NzPageHeaderComponent.prototype.ngOnChanges = /**
     * @param {?} changes
     * @return {?}
     */
    function (changes) {
        if (changes.hasOwnProperty('nzBackIcon')) {
            this.isTemplateRefBackIcon = changes.nzBackIcon.currentValue instanceof TemplateRef;
            this.isStringBackIcon = typeof changes.nzBackIcon.currentValue === 'string';
        }
    };
    /**
     * @return {?}
     */
    NzPageHeaderComponent.prototype.onBack = /**
     * @return {?}
     */
    function () {
        this.nzBack.emit();
    };
    NzPageHeaderComponent.decorators = [
        { type: Component, args: [{
                    selector: 'nz-page-header',
                    template: "<ng-content select=\"nz-breadcrumb[nz-page-header-breadcrumb]\"></ng-content>\n\n<div *ngIf=\"nzBackIcon !== null\" (click)=\"onBack()\" class=\"ant-page-header-back-icon\">\n  <i *ngIf=\"isStringBackIcon\" nz-icon [type]=\"nzBackIcon ? nzBackIcon : 'arrow-left'\" theme=\"outline\"></i>\n  <ng-container *ngIf=\"isTemplateRefBackIcon\" [ngTemplateOutlet]=\"nzBackIcon\"></ng-container>\n  <nz-divider nzType=\"vertical\"></nz-divider>\n</div>\n\n<div class=\"ant-page-header-title-view\">\n  <span class=\"ant-page-header-title-view-title\" *ngIf=\"nzTitle\">\n    <ng-container *nzStringTemplateOutlet=\"nzTitle\">{{ nzTitle }}</ng-container>\n  </span>\n  <ng-content *ngIf=\"!nzTitle\" select=\"nz-page-header-title, [nz-page-header-title]\"></ng-content>\n  <span class=\"ant-page-header-title-view-sub-title\" *ngIf=\"nzSubtitle\">\n    <ng-container *nzStringTemplateOutlet=\"nzSubtitle\">{{ nzSubtitle }}</ng-container>\n  </span>\n  <ng-content *ngIf=\"!nzSubtitle\" select=\"nz-page-header-subtitle, [nz-page-header-subtitle]\"></ng-content>\n  <ng-content select=\"nz-page-header-tags, [nz-page-header-tags]\"></ng-content>\n  <ng-content select=\"nz-page-header-extra, [nz-page-header-extra]\"></ng-content>\n</div>\n\n<ng-content select=\"nz-page-header-content, [nz-page-header-content]\"></ng-content>\n<ng-content select=\"nz-page-header-footer, [nz-page-header-footer]\"></ng-content>\n",
                    changeDetection: ChangeDetectionStrategy.OnPush,
                    encapsulation: ViewEncapsulation.None,
                    host: {
                        class: 'ant-page-header',
                        '[class.ant-page-header-has-footer]': 'nzPageHeaderFooter'
                    },
                    styles: ["nz-page-header,nz-page-header-content,nz-page-header-footer{display:block}"]
                }] }
    ];
    /** @nocollapse */
    NzPageHeaderComponent.ctorParameters = function () { return []; };
    NzPageHeaderComponent.propDecorators = {
        nzBackIcon: [{ type: Input }],
        nzTitle: [{ type: Input }],
        nzSubtitle: [{ type: Input }],
        nzBack: [{ type: Output }],
        nzPageHeaderFooter: [{ type: ContentChild, args: [NzPageHeaderFooterDirective,] }]
    };
    return NzPageHeaderComponent;
}());
export { NzPageHeaderComponent };
if (false) {
    /** @type {?} */
    NzPageHeaderComponent.prototype.isTemplateRefBackIcon;
    /** @type {?} */
    NzPageHeaderComponent.prototype.isStringBackIcon;
    /** @type {?} */
    NzPageHeaderComponent.prototype.nzBackIcon;
    /** @type {?} */
    NzPageHeaderComponent.prototype.nzTitle;
    /** @type {?} */
    NzPageHeaderComponent.prototype.nzSubtitle;
    /** @type {?} */
    NzPageHeaderComponent.prototype.nzBack;
    /** @type {?} */
    NzPageHeaderComponent.prototype.nzPageHeaderFooter;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotcGFnZS1oZWFkZXIuY29tcG9uZW50LmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbInBhZ2UtaGVhZGVyL256LXBhZ2UtaGVhZGVyLmNvbXBvbmVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7O0FBQUEsT0FBTyxFQUNMLHVCQUF1QixFQUN2QixTQUFTLEVBQ1QsWUFBWSxFQUNaLFVBQVUsRUFDVixZQUFZLEVBQ1osS0FBSyxFQUdMLE1BQU0sRUFFTixXQUFXLEVBQ1gsaUJBQWlCLEVBQ2xCLE1BQU0sZUFBZSxDQUFDO0FBQ3ZCLE9BQU8sRUFBRSwyQkFBMkIsRUFBRSxNQUFNLHdCQUF3QixDQUFDO0FBRXJFO0lBc0JFO1FBVkEsMEJBQXFCLEdBQUcsS0FBSyxDQUFDO1FBQzlCLHFCQUFnQixHQUFHLEtBQUssQ0FBQztRQUVoQixlQUFVLEdBQXNDLElBQUksQ0FBQztRQUczQyxXQUFNLEdBQUcsSUFBSSxZQUFZLEVBQVEsQ0FBQztJQUl0QyxDQUFDOzs7O0lBRWhCLHdDQUFROzs7SUFBUixjQUFrQixDQUFDOzs7OztJQUVuQiwyQ0FBVzs7OztJQUFYLFVBQVksT0FBc0I7UUFDaEMsSUFBSSxPQUFPLENBQUMsY0FBYyxDQUFDLFlBQVksQ0FBQyxFQUFFO1lBQ3hDLElBQUksQ0FBQyxxQkFBcUIsR0FBRyxPQUFPLENBQUMsVUFBVSxDQUFDLFlBQVksWUFBWSxXQUFXLENBQUM7WUFDcEYsSUFBSSxDQUFDLGdCQUFnQixHQUFHLE9BQU8sT0FBTyxDQUFDLFVBQVUsQ0FBQyxZQUFZLEtBQUssUUFBUSxDQUFDO1NBQzdFO0lBQ0gsQ0FBQzs7OztJQUVELHNDQUFNOzs7SUFBTjtRQUNFLElBQUksQ0FBQyxNQUFNLENBQUMsSUFBSSxFQUFFLENBQUM7SUFDckIsQ0FBQzs7Z0JBbkNGLFNBQVMsU0FBQztvQkFDVCxRQUFRLEVBQUUsZ0JBQWdCO29CQUMxQix1NENBQThDO29CQUU5QyxlQUFlLEVBQUUsdUJBQXVCLENBQUMsTUFBTTtvQkFDL0MsYUFBYSxFQUFFLGlCQUFpQixDQUFDLElBQUk7b0JBQ3JDLElBQUksRUFBRTt3QkFDSixLQUFLLEVBQUUsaUJBQWlCO3dCQUN4QixvQ0FBb0MsRUFBRSxvQkFBb0I7cUJBQzNEOztpQkFDRjs7Ozs7NkJBS0UsS0FBSzswQkFDTCxLQUFLOzZCQUNMLEtBQUs7eUJBQ0wsTUFBTTtxQ0FFTixZQUFZLFNBQUMsMkJBQTJCOztJQWdCM0MsNEJBQUM7Q0FBQSxBQXBDRCxJQW9DQztTQXpCWSxxQkFBcUI7OztJQUNoQyxzREFBOEI7O0lBQzlCLGlEQUF5Qjs7SUFFekIsMkNBQThEOztJQUM5RCx3Q0FBNkM7O0lBQzdDLDJDQUFnRDs7SUFDaEQsdUNBQXFEOztJQUVyRCxtREFBdUciLCJzb3VyY2VzQ29udGVudCI6WyJpbXBvcnQge1xuICBDaGFuZ2VEZXRlY3Rpb25TdHJhdGVneSxcbiAgQ29tcG9uZW50LFxuICBDb250ZW50Q2hpbGQsXG4gIEVsZW1lbnRSZWYsXG4gIEV2ZW50RW1pdHRlcixcbiAgSW5wdXQsXG4gIE9uQ2hhbmdlcyxcbiAgT25Jbml0LFxuICBPdXRwdXQsXG4gIFNpbXBsZUNoYW5nZXMsXG4gIFRlbXBsYXRlUmVmLFxuICBWaWV3RW5jYXBzdWxhdGlvblxufSBmcm9tICdAYW5ndWxhci9jb3JlJztcbmltcG9ydCB7IE56UGFnZUhlYWRlckZvb3RlckRpcmVjdGl2ZSB9IGZyb20gJy4vbnotcGFnZS1oZWFkZXItY2VsbHMnO1xuXG5AQ29tcG9uZW50KHtcbiAgc2VsZWN0b3I6ICduei1wYWdlLWhlYWRlcicsXG4gIHRlbXBsYXRlVXJsOiAnLi9uei1wYWdlLWhlYWRlci5jb21wb25lbnQuaHRtbCcsXG4gIHN0eWxlVXJsczogWycuL256LXBhZ2UtaGVhZGVyLmNvbXBvbmVudC5sZXNzJ10sXG4gIGNoYW5nZURldGVjdGlvbjogQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3kuT25QdXNoLFxuICBlbmNhcHN1bGF0aW9uOiBWaWV3RW5jYXBzdWxhdGlvbi5Ob25lLFxuICBob3N0OiB7XG4gICAgY2xhc3M6ICdhbnQtcGFnZS1oZWFkZXInLFxuICAgICdbY2xhc3MuYW50LXBhZ2UtaGVhZGVyLWhhcy1mb290ZXJdJzogJ256UGFnZUhlYWRlckZvb3RlcidcbiAgfVxufSlcbmV4cG9ydCBjbGFzcyBOelBhZ2VIZWFkZXJDb21wb25lbnQgaW1wbGVtZW50cyBPbkluaXQsIE9uQ2hhbmdlcyB7XG4gIGlzVGVtcGxhdGVSZWZCYWNrSWNvbiA9IGZhbHNlO1xuICBpc1N0cmluZ0JhY2tJY29uID0gZmFsc2U7XG5cbiAgQElucHV0KCkgbnpCYWNrSWNvbjogc3RyaW5nIHwgVGVtcGxhdGVSZWY8dm9pZD4gfCBudWxsID0gbnVsbDtcbiAgQElucHV0KCkgbnpUaXRsZTogc3RyaW5nIHwgVGVtcGxhdGVSZWY8dm9pZD47XG4gIEBJbnB1dCgpIG56U3VidGl0bGU6IHN0cmluZyB8IFRlbXBsYXRlUmVmPHZvaWQ+O1xuICBAT3V0cHV0KCkgcmVhZG9ubHkgbnpCYWNrID0gbmV3IEV2ZW50RW1pdHRlcjx2b2lkPigpO1xuXG4gIEBDb250ZW50Q2hpbGQoTnpQYWdlSGVhZGVyRm9vdGVyRGlyZWN0aXZlKSBuelBhZ2VIZWFkZXJGb290ZXI6IEVsZW1lbnRSZWY8TnpQYWdlSGVhZGVyRm9vdGVyRGlyZWN0aXZlPjtcblxuICBjb25zdHJ1Y3RvcigpIHt9XG5cbiAgbmdPbkluaXQoKTogdm9pZCB7fVxuXG4gIG5nT25DaGFuZ2VzKGNoYW5nZXM6IFNpbXBsZUNoYW5nZXMpOiB2b2lkIHtcbiAgICBpZiAoY2hhbmdlcy5oYXNPd25Qcm9wZXJ0eSgnbnpCYWNrSWNvbicpKSB7XG4gICAgICB0aGlzLmlzVGVtcGxhdGVSZWZCYWNrSWNvbiA9IGNoYW5nZXMubnpCYWNrSWNvbi5jdXJyZW50VmFsdWUgaW5zdGFuY2VvZiBUZW1wbGF0ZVJlZjtcbiAgICAgIHRoaXMuaXNTdHJpbmdCYWNrSWNvbiA9IHR5cGVvZiBjaGFuZ2VzLm56QmFja0ljb24uY3VycmVudFZhbHVlID09PSAnc3RyaW5nJztcbiAgICB9XG4gIH1cblxuICBvbkJhY2soKTogdm9pZCB7XG4gICAgdGhpcy5uekJhY2suZW1pdCgpO1xuICB9XG59XG4iXX0=