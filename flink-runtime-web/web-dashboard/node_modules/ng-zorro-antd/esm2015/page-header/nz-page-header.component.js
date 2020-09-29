/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { ChangeDetectionStrategy, Component, ContentChild, ElementRef, EventEmitter, Input, Output, TemplateRef, ViewEncapsulation } from '@angular/core';
import { NzPageHeaderFooterDirective } from './nz-page-header-cells';
export class NzPageHeaderComponent {
    constructor() {
        this.isTemplateRefBackIcon = false;
        this.isStringBackIcon = false;
        this.nzBackIcon = null;
        this.nzBack = new EventEmitter();
    }
    /**
     * @return {?}
     */
    ngOnInit() { }
    /**
     * @param {?} changes
     * @return {?}
     */
    ngOnChanges(changes) {
        if (changes.hasOwnProperty('nzBackIcon')) {
            this.isTemplateRefBackIcon = changes.nzBackIcon.currentValue instanceof TemplateRef;
            this.isStringBackIcon = typeof changes.nzBackIcon.currentValue === 'string';
        }
    }
    /**
     * @return {?}
     */
    onBack() {
        this.nzBack.emit();
    }
}
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
NzPageHeaderComponent.ctorParameters = () => [];
NzPageHeaderComponent.propDecorators = {
    nzBackIcon: [{ type: Input }],
    nzTitle: [{ type: Input }],
    nzSubtitle: [{ type: Input }],
    nzBack: [{ type: Output }],
    nzPageHeaderFooter: [{ type: ContentChild, args: [NzPageHeaderFooterDirective,] }]
};
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
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotcGFnZS1oZWFkZXIuY29tcG9uZW50LmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbInBhZ2UtaGVhZGVyL256LXBhZ2UtaGVhZGVyLmNvbXBvbmVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7O0FBQUEsT0FBTyxFQUNMLHVCQUF1QixFQUN2QixTQUFTLEVBQ1QsWUFBWSxFQUNaLFVBQVUsRUFDVixZQUFZLEVBQ1osS0FBSyxFQUdMLE1BQU0sRUFFTixXQUFXLEVBQ1gsaUJBQWlCLEVBQ2xCLE1BQU0sZUFBZSxDQUFDO0FBQ3ZCLE9BQU8sRUFBRSwyQkFBMkIsRUFBRSxNQUFNLHdCQUF3QixDQUFDO0FBYXJFLE1BQU0sT0FBTyxxQkFBcUI7SUFXaEM7UUFWQSwwQkFBcUIsR0FBRyxLQUFLLENBQUM7UUFDOUIscUJBQWdCLEdBQUcsS0FBSyxDQUFDO1FBRWhCLGVBQVUsR0FBc0MsSUFBSSxDQUFDO1FBRzNDLFdBQU0sR0FBRyxJQUFJLFlBQVksRUFBUSxDQUFDO0lBSXRDLENBQUM7Ozs7SUFFaEIsUUFBUSxLQUFVLENBQUM7Ozs7O0lBRW5CLFdBQVcsQ0FBQyxPQUFzQjtRQUNoQyxJQUFJLE9BQU8sQ0FBQyxjQUFjLENBQUMsWUFBWSxDQUFDLEVBQUU7WUFDeEMsSUFBSSxDQUFDLHFCQUFxQixHQUFHLE9BQU8sQ0FBQyxVQUFVLENBQUMsWUFBWSxZQUFZLFdBQVcsQ0FBQztZQUNwRixJQUFJLENBQUMsZ0JBQWdCLEdBQUcsT0FBTyxPQUFPLENBQUMsVUFBVSxDQUFDLFlBQVksS0FBSyxRQUFRLENBQUM7U0FDN0U7SUFDSCxDQUFDOzs7O0lBRUQsTUFBTTtRQUNKLElBQUksQ0FBQyxNQUFNLENBQUMsSUFBSSxFQUFFLENBQUM7SUFDckIsQ0FBQzs7O1lBbkNGLFNBQVMsU0FBQztnQkFDVCxRQUFRLEVBQUUsZ0JBQWdCO2dCQUMxQix1NENBQThDO2dCQUU5QyxlQUFlLEVBQUUsdUJBQXVCLENBQUMsTUFBTTtnQkFDL0MsYUFBYSxFQUFFLGlCQUFpQixDQUFDLElBQUk7Z0JBQ3JDLElBQUksRUFBRTtvQkFDSixLQUFLLEVBQUUsaUJBQWlCO29CQUN4QixvQ0FBb0MsRUFBRSxvQkFBb0I7aUJBQzNEOzthQUNGOzs7Ozt5QkFLRSxLQUFLO3NCQUNMLEtBQUs7eUJBQ0wsS0FBSztxQkFDTCxNQUFNO2lDQUVOLFlBQVksU0FBQywyQkFBMkI7Ozs7SUFSekMsc0RBQThCOztJQUM5QixpREFBeUI7O0lBRXpCLDJDQUE4RDs7SUFDOUQsd0NBQTZDOztJQUM3QywyQ0FBZ0Q7O0lBQ2hELHVDQUFxRDs7SUFFckQsbURBQXVHIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHtcbiAgQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3ksXG4gIENvbXBvbmVudCxcbiAgQ29udGVudENoaWxkLFxuICBFbGVtZW50UmVmLFxuICBFdmVudEVtaXR0ZXIsXG4gIElucHV0LFxuICBPbkNoYW5nZXMsXG4gIE9uSW5pdCxcbiAgT3V0cHV0LFxuICBTaW1wbGVDaGFuZ2VzLFxuICBUZW1wbGF0ZVJlZixcbiAgVmlld0VuY2Fwc3VsYXRpb25cbn0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5pbXBvcnQgeyBOelBhZ2VIZWFkZXJGb290ZXJEaXJlY3RpdmUgfSBmcm9tICcuL256LXBhZ2UtaGVhZGVyLWNlbGxzJztcblxuQENvbXBvbmVudCh7XG4gIHNlbGVjdG9yOiAnbnotcGFnZS1oZWFkZXInLFxuICB0ZW1wbGF0ZVVybDogJy4vbnotcGFnZS1oZWFkZXIuY29tcG9uZW50Lmh0bWwnLFxuICBzdHlsZVVybHM6IFsnLi9uei1wYWdlLWhlYWRlci5jb21wb25lbnQubGVzcyddLFxuICBjaGFuZ2VEZXRlY3Rpb246IENoYW5nZURldGVjdGlvblN0cmF0ZWd5Lk9uUHVzaCxcbiAgZW5jYXBzdWxhdGlvbjogVmlld0VuY2Fwc3VsYXRpb24uTm9uZSxcbiAgaG9zdDoge1xuICAgIGNsYXNzOiAnYW50LXBhZ2UtaGVhZGVyJyxcbiAgICAnW2NsYXNzLmFudC1wYWdlLWhlYWRlci1oYXMtZm9vdGVyXSc6ICduelBhZ2VIZWFkZXJGb290ZXInXG4gIH1cbn0pXG5leHBvcnQgY2xhc3MgTnpQYWdlSGVhZGVyQ29tcG9uZW50IGltcGxlbWVudHMgT25Jbml0LCBPbkNoYW5nZXMge1xuICBpc1RlbXBsYXRlUmVmQmFja0ljb24gPSBmYWxzZTtcbiAgaXNTdHJpbmdCYWNrSWNvbiA9IGZhbHNlO1xuXG4gIEBJbnB1dCgpIG56QmFja0ljb246IHN0cmluZyB8IFRlbXBsYXRlUmVmPHZvaWQ+IHwgbnVsbCA9IG51bGw7XG4gIEBJbnB1dCgpIG56VGl0bGU6IHN0cmluZyB8IFRlbXBsYXRlUmVmPHZvaWQ+O1xuICBASW5wdXQoKSBuelN1YnRpdGxlOiBzdHJpbmcgfCBUZW1wbGF0ZVJlZjx2b2lkPjtcbiAgQE91dHB1dCgpIHJlYWRvbmx5IG56QmFjayA9IG5ldyBFdmVudEVtaXR0ZXI8dm9pZD4oKTtcblxuICBAQ29udGVudENoaWxkKE56UGFnZUhlYWRlckZvb3RlckRpcmVjdGl2ZSkgbnpQYWdlSGVhZGVyRm9vdGVyOiBFbGVtZW50UmVmPE56UGFnZUhlYWRlckZvb3RlckRpcmVjdGl2ZT47XG5cbiAgY29uc3RydWN0b3IoKSB7fVxuXG4gIG5nT25Jbml0KCk6IHZvaWQge31cblxuICBuZ09uQ2hhbmdlcyhjaGFuZ2VzOiBTaW1wbGVDaGFuZ2VzKTogdm9pZCB7XG4gICAgaWYgKGNoYW5nZXMuaGFzT3duUHJvcGVydHkoJ256QmFja0ljb24nKSkge1xuICAgICAgdGhpcy5pc1RlbXBsYXRlUmVmQmFja0ljb24gPSBjaGFuZ2VzLm56QmFja0ljb24uY3VycmVudFZhbHVlIGluc3RhbmNlb2YgVGVtcGxhdGVSZWY7XG4gICAgICB0aGlzLmlzU3RyaW5nQmFja0ljb24gPSB0eXBlb2YgY2hhbmdlcy5uekJhY2tJY29uLmN1cnJlbnRWYWx1ZSA9PT0gJ3N0cmluZyc7XG4gICAgfVxuICB9XG5cbiAgb25CYWNrKCk6IHZvaWQge1xuICAgIHRoaXMubnpCYWNrLmVtaXQoKTtcbiAgfVxufVxuIl19