/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { ChangeDetectionStrategy, ChangeDetectorRef, Component, ContentChild, ElementRef, Input, Renderer2, TemplateRef, ViewEncapsulation } from '@angular/core';
import { NzAnchorComponent } from './nz-anchor.component';
export class NzAnchorLinkComponent {
    /**
     * @param {?} elementRef
     * @param {?} anchorComp
     * @param {?} cdr
     * @param {?} renderer
     */
    constructor(elementRef, anchorComp, cdr, renderer) {
        this.elementRef = elementRef;
        this.anchorComp = anchorComp;
        this.cdr = cdr;
        this.nzHref = '#';
        this.titleStr = '';
        this.active = false;
        renderer.addClass(elementRef.nativeElement, 'ant-anchor-link');
    }
    /**
     * @param {?} value
     * @return {?}
     */
    set nzTitle(value) {
        if (value instanceof TemplateRef) {
            this.titleStr = null;
            this.titleTpl = value;
        }
        else {
            this.titleStr = value;
        }
    }
    /**
     * @return {?}
     */
    ngOnInit() {
        this.anchorComp.registerLink(this);
    }
    /**
     * @param {?} e
     * @return {?}
     */
    goToClick(e) {
        e.preventDefault();
        e.stopPropagation();
        this.anchorComp.handleScrollTo(this);
    }
    /**
     * @return {?}
     */
    markForCheck() {
        this.cdr.markForCheck();
    }
    /**
     * @return {?}
     */
    ngOnDestroy() {
        this.anchorComp.unregisterLink(this);
    }
}
NzAnchorLinkComponent.decorators = [
    { type: Component, args: [{
                selector: 'nz-link',
                preserveWhitespaces: false,
                template: "<a (click)=\"goToClick($event)\" href=\"{{nzHref}}\" class=\"ant-anchor-link-title\" title=\"{{titleStr}}\">\n  <span *ngIf=\"titleStr; else (titleTpl || nzTemplate)\">{{ titleStr }}</span>\n</a>\n<ng-content></ng-content>",
                host: {
                    '[class.ant-anchor-link-active]': 'active'
                },
                encapsulation: ViewEncapsulation.None,
                changeDetection: ChangeDetectionStrategy.OnPush,
                styles: [`
      nz-link {
        display: block;
      }
    `]
            }] }
];
/** @nocollapse */
NzAnchorLinkComponent.ctorParameters = () => [
    { type: ElementRef },
    { type: NzAnchorComponent },
    { type: ChangeDetectorRef },
    { type: Renderer2 }
];
NzAnchorLinkComponent.propDecorators = {
    nzHref: [{ type: Input }],
    nzTitle: [{ type: Input }],
    nzTemplate: [{ type: ContentChild, args: ['nzTemplate',] }]
};
if (false) {
    /** @type {?} */
    NzAnchorLinkComponent.prototype.nzHref;
    /** @type {?} */
    NzAnchorLinkComponent.prototype.titleStr;
    /** @type {?} */
    NzAnchorLinkComponent.prototype.titleTpl;
    /** @type {?} */
    NzAnchorLinkComponent.prototype.active;
    /** @type {?} */
    NzAnchorLinkComponent.prototype.nzTemplate;
    /** @type {?} */
    NzAnchorLinkComponent.prototype.elementRef;
    /**
     * @type {?}
     * @private
     */
    NzAnchorLinkComponent.prototype.anchorComp;
    /**
     * @type {?}
     * @private
     */
    NzAnchorLinkComponent.prototype.cdr;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotYW5jaG9yLWxpbmsuY29tcG9uZW50LmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbImFuY2hvci9uei1hbmNob3ItbGluay5jb21wb25lbnQudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7OztBQUFBLE9BQU8sRUFDTCx1QkFBdUIsRUFDdkIsaUJBQWlCLEVBQ2pCLFNBQVMsRUFDVCxZQUFZLEVBQ1osVUFBVSxFQUNWLEtBQUssRUFHTCxTQUFTLEVBQ1QsV0FBVyxFQUNYLGlCQUFpQixFQUNsQixNQUFNLGVBQWUsQ0FBQztBQUV2QixPQUFPLEVBQUUsaUJBQWlCLEVBQUUsTUFBTSx1QkFBdUIsQ0FBQztBQW1CMUQsTUFBTSxPQUFPLHFCQUFxQjs7Ozs7OztJQW1CaEMsWUFDUyxVQUFzQixFQUNyQixVQUE2QixFQUM3QixHQUFzQixFQUM5QixRQUFtQjtRQUhaLGVBQVUsR0FBVixVQUFVLENBQVk7UUFDckIsZUFBVSxHQUFWLFVBQVUsQ0FBbUI7UUFDN0IsUUFBRyxHQUFILEdBQUcsQ0FBbUI7UUFyQnZCLFdBQU0sR0FBRyxHQUFHLENBQUM7UUFFdEIsYUFBUSxHQUFrQixFQUFFLENBQUM7UUFFN0IsV0FBTSxHQUFZLEtBQUssQ0FBQztRQW9CdEIsUUFBUSxDQUFDLFFBQVEsQ0FBQyxVQUFVLENBQUMsYUFBYSxFQUFFLGlCQUFpQixDQUFDLENBQUM7SUFDakUsQ0FBQzs7Ozs7SUFuQkQsSUFDSSxPQUFPLENBQUMsS0FBaUM7UUFDM0MsSUFBSSxLQUFLLFlBQVksV0FBVyxFQUFFO1lBQ2hDLElBQUksQ0FBQyxRQUFRLEdBQUcsSUFBSSxDQUFDO1lBQ3JCLElBQUksQ0FBQyxRQUFRLEdBQUcsS0FBSyxDQUFDO1NBQ3ZCO2FBQU07WUFDTCxJQUFJLENBQUMsUUFBUSxHQUFHLEtBQUssQ0FBQztTQUN2QjtJQUNILENBQUM7Ozs7SUFhRCxRQUFRO1FBQ04sSUFBSSxDQUFDLFVBQVUsQ0FBQyxZQUFZLENBQUMsSUFBSSxDQUFDLENBQUM7SUFDckMsQ0FBQzs7Ozs7SUFFRCxTQUFTLENBQUMsQ0FBUTtRQUNoQixDQUFDLENBQUMsY0FBYyxFQUFFLENBQUM7UUFDbkIsQ0FBQyxDQUFDLGVBQWUsRUFBRSxDQUFDO1FBQ3BCLElBQUksQ0FBQyxVQUFVLENBQUMsY0FBYyxDQUFDLElBQUksQ0FBQyxDQUFDO0lBQ3ZDLENBQUM7Ozs7SUFFRCxZQUFZO1FBQ1YsSUFBSSxDQUFDLEdBQUcsQ0FBQyxZQUFZLEVBQUUsQ0FBQztJQUMxQixDQUFDOzs7O0lBRUQsV0FBVztRQUNULElBQUksQ0FBQyxVQUFVLENBQUMsY0FBYyxDQUFDLElBQUksQ0FBQyxDQUFDO0lBQ3ZDLENBQUM7OztZQTdERixTQUFTLFNBQUM7Z0JBQ1QsUUFBUSxFQUFFLFNBQVM7Z0JBQ25CLG1CQUFtQixFQUFFLEtBQUs7Z0JBQzFCLDBPQUE4QztnQkFDOUMsSUFBSSxFQUFFO29CQUNKLGdDQUFnQyxFQUFFLFFBQVE7aUJBQzNDO2dCQVFELGFBQWEsRUFBRSxpQkFBaUIsQ0FBQyxJQUFJO2dCQUNyQyxlQUFlLEVBQUUsdUJBQXVCLENBQUMsTUFBTTt5QkFQN0M7Ozs7S0FJQzthQUlKOzs7O1lBM0JDLFVBQVU7WUFTSCxpQkFBaUI7WUFaeEIsaUJBQWlCO1lBT2pCLFNBQVM7OztxQkF5QlIsS0FBSztzQkFNTCxLQUFLO3lCQVVMLFlBQVksU0FBQyxZQUFZOzs7O0lBaEIxQix1Q0FBc0I7O0lBRXRCLHlDQUE2Qjs7SUFDN0IseUNBQTRCOztJQUM1Qix1Q0FBd0I7O0lBWXhCLDJDQUEwRDs7SUFHeEQsMkNBQTZCOzs7OztJQUM3QiwyQ0FBcUM7Ozs7O0lBQ3JDLG9DQUE4QiIsInNvdXJjZXNDb250ZW50IjpbImltcG9ydCB7XG4gIENoYW5nZURldGVjdGlvblN0cmF0ZWd5LFxuICBDaGFuZ2VEZXRlY3RvclJlZixcbiAgQ29tcG9uZW50LFxuICBDb250ZW50Q2hpbGQsXG4gIEVsZW1lbnRSZWYsXG4gIElucHV0LFxuICBPbkRlc3Ryb3ksXG4gIE9uSW5pdCxcbiAgUmVuZGVyZXIyLFxuICBUZW1wbGF0ZVJlZixcbiAgVmlld0VuY2Fwc3VsYXRpb25cbn0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5cbmltcG9ydCB7IE56QW5jaG9yQ29tcG9uZW50IH0gZnJvbSAnLi9uei1hbmNob3IuY29tcG9uZW50JztcblxuQENvbXBvbmVudCh7XG4gIHNlbGVjdG9yOiAnbnotbGluaycsXG4gIHByZXNlcnZlV2hpdGVzcGFjZXM6IGZhbHNlLFxuICB0ZW1wbGF0ZVVybDogJy4vbnotYW5jaG9yLWxpbmsuY29tcG9uZW50Lmh0bWwnLFxuICBob3N0OiB7XG4gICAgJ1tjbGFzcy5hbnQtYW5jaG9yLWxpbmstYWN0aXZlXSc6ICdhY3RpdmUnXG4gIH0sXG4gIHN0eWxlczogW1xuICAgIGBcbiAgICAgIG56LWxpbmsge1xuICAgICAgICBkaXNwbGF5OiBibG9jaztcbiAgICAgIH1cbiAgICBgXG4gIF0sXG4gIGVuY2Fwc3VsYXRpb246IFZpZXdFbmNhcHN1bGF0aW9uLk5vbmUsXG4gIGNoYW5nZURldGVjdGlvbjogQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3kuT25QdXNoXG59KVxuZXhwb3J0IGNsYXNzIE56QW5jaG9yTGlua0NvbXBvbmVudCBpbXBsZW1lbnRzIE9uSW5pdCwgT25EZXN0cm95IHtcbiAgQElucHV0KCkgbnpIcmVmID0gJyMnO1xuXG4gIHRpdGxlU3RyOiBzdHJpbmcgfCBudWxsID0gJyc7XG4gIHRpdGxlVHBsOiBUZW1wbGF0ZVJlZjx2b2lkPjtcbiAgYWN0aXZlOiBib29sZWFuID0gZmFsc2U7XG5cbiAgQElucHV0KClcbiAgc2V0IG56VGl0bGUodmFsdWU6IHN0cmluZyB8IFRlbXBsYXRlUmVmPHZvaWQ+KSB7XG4gICAgaWYgKHZhbHVlIGluc3RhbmNlb2YgVGVtcGxhdGVSZWYpIHtcbiAgICAgIHRoaXMudGl0bGVTdHIgPSBudWxsO1xuICAgICAgdGhpcy50aXRsZVRwbCA9IHZhbHVlO1xuICAgIH0gZWxzZSB7XG4gICAgICB0aGlzLnRpdGxlU3RyID0gdmFsdWU7XG4gICAgfVxuICB9XG5cbiAgQENvbnRlbnRDaGlsZCgnbnpUZW1wbGF0ZScpIG56VGVtcGxhdGU6IFRlbXBsYXRlUmVmPHZvaWQ+O1xuXG4gIGNvbnN0cnVjdG9yKFxuICAgIHB1YmxpYyBlbGVtZW50UmVmOiBFbGVtZW50UmVmLFxuICAgIHByaXZhdGUgYW5jaG9yQ29tcDogTnpBbmNob3JDb21wb25lbnQsXG4gICAgcHJpdmF0ZSBjZHI6IENoYW5nZURldGVjdG9yUmVmLFxuICAgIHJlbmRlcmVyOiBSZW5kZXJlcjJcbiAgKSB7XG4gICAgcmVuZGVyZXIuYWRkQ2xhc3MoZWxlbWVudFJlZi5uYXRpdmVFbGVtZW50LCAnYW50LWFuY2hvci1saW5rJyk7XG4gIH1cblxuICBuZ09uSW5pdCgpOiB2b2lkIHtcbiAgICB0aGlzLmFuY2hvckNvbXAucmVnaXN0ZXJMaW5rKHRoaXMpO1xuICB9XG5cbiAgZ29Ub0NsaWNrKGU6IEV2ZW50KTogdm9pZCB7XG4gICAgZS5wcmV2ZW50RGVmYXVsdCgpO1xuICAgIGUuc3RvcFByb3BhZ2F0aW9uKCk7XG4gICAgdGhpcy5hbmNob3JDb21wLmhhbmRsZVNjcm9sbFRvKHRoaXMpO1xuICB9XG5cbiAgbWFya0ZvckNoZWNrKCk6IHZvaWQge1xuICAgIHRoaXMuY2RyLm1hcmtGb3JDaGVjaygpO1xuICB9XG5cbiAgbmdPbkRlc3Ryb3koKTogdm9pZCB7XG4gICAgdGhpcy5hbmNob3JDb21wLnVucmVnaXN0ZXJMaW5rKHRoaXMpO1xuICB9XG59XG4iXX0=