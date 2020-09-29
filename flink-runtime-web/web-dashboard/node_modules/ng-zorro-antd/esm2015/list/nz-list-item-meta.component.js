/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { ChangeDetectionStrategy, Component, ElementRef, Input, Renderer2, TemplateRef, ViewEncapsulation } from '@angular/core';
export class NzListItemMetaComponent {
    /**
     * @param {?} elementRef
     * @param {?} renderer
     */
    constructor(elementRef, renderer) {
        this.elementRef = elementRef;
        this.renderer = renderer;
        this.avatarStr = '';
        this.renderer.addClass(elementRef.nativeElement, 'ant-list-item-meta');
    }
    /**
     * @param {?} value
     * @return {?}
     */
    set nzAvatar(value) {
        if (value instanceof TemplateRef) {
            this.avatarStr = '';
            this.avatarTpl = value;
        }
        else {
            this.avatarStr = value;
        }
    }
}
NzListItemMetaComponent.decorators = [
    { type: Component, args: [{
                selector: 'nz-list-item-meta',
                template: "<div *ngIf=\"avatarStr || avatarTpl\" class=\"ant-list-item-meta-avatar\">\n  <ng-container *ngIf=\"avatarStr; else avatarTpl\">\n    <nz-avatar [nzSrc]=\"avatarStr\"></nz-avatar>\n  </ng-container>\n</div>\n<div *ngIf=\"nzTitle || nzDescription\" class=\"ant-list-item-meta-content\">\n  <h4 *ngIf=\"nzTitle\" class=\"ant-list-item-meta-title\">\n    <ng-container *nzStringTemplateOutlet=\"nzTitle\">{{ nzTitle }}</ng-container>\n  </h4>\n  <div *ngIf=\"nzDescription\" class=\"ant-list-item-meta-description\">\n    <ng-container *nzStringTemplateOutlet=\"nzDescription\">{{ nzDescription }}</ng-container>\n  </div>\n</div>",
                preserveWhitespaces: false,
                changeDetection: ChangeDetectionStrategy.OnPush,
                encapsulation: ViewEncapsulation.None
            }] }
];
/** @nocollapse */
NzListItemMetaComponent.ctorParameters = () => [
    { type: ElementRef },
    { type: Renderer2 }
];
NzListItemMetaComponent.propDecorators = {
    nzAvatar: [{ type: Input }],
    nzTitle: [{ type: Input }],
    nzDescription: [{ type: Input }]
};
if (false) {
    /** @type {?} */
    NzListItemMetaComponent.prototype.avatarStr;
    /** @type {?} */
    NzListItemMetaComponent.prototype.avatarTpl;
    /** @type {?} */
    NzListItemMetaComponent.prototype.nzTitle;
    /** @type {?} */
    NzListItemMetaComponent.prototype.nzDescription;
    /** @type {?} */
    NzListItemMetaComponent.prototype.elementRef;
    /**
     * @type {?}
     * @private
     */
    NzListItemMetaComponent.prototype.renderer;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotbGlzdC1pdGVtLW1ldGEuY29tcG9uZW50LmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbImxpc3QvbnotbGlzdC1pdGVtLW1ldGEuY29tcG9uZW50LnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7QUFBQSxPQUFPLEVBQ0wsdUJBQXVCLEVBQ3ZCLFNBQVMsRUFDVCxVQUFVLEVBQ1YsS0FBSyxFQUNMLFNBQVMsRUFDVCxXQUFXLEVBQ1gsaUJBQWlCLEVBQ2xCLE1BQU0sZUFBZSxDQUFDO0FBU3ZCLE1BQU0sT0FBTyx1QkFBdUI7Ozs7O0lBa0JsQyxZQUFtQixVQUFzQixFQUFVLFFBQW1CO1FBQW5ELGVBQVUsR0FBVixVQUFVLENBQVk7UUFBVSxhQUFRLEdBQVIsUUFBUSxDQUFXO1FBakJ0RSxjQUFTLEdBQUcsRUFBRSxDQUFDO1FBa0JiLElBQUksQ0FBQyxRQUFRLENBQUMsUUFBUSxDQUFDLFVBQVUsQ0FBQyxhQUFhLEVBQUUsb0JBQW9CLENBQUMsQ0FBQztJQUN6RSxDQUFDOzs7OztJQWhCRCxJQUNJLFFBQVEsQ0FBQyxLQUFpQztRQUM1QyxJQUFJLEtBQUssWUFBWSxXQUFXLEVBQUU7WUFDaEMsSUFBSSxDQUFDLFNBQVMsR0FBRyxFQUFFLENBQUM7WUFDcEIsSUFBSSxDQUFDLFNBQVMsR0FBRyxLQUFLLENBQUM7U0FDeEI7YUFBTTtZQUNMLElBQUksQ0FBQyxTQUFTLEdBQUcsS0FBSyxDQUFDO1NBQ3hCO0lBQ0gsQ0FBQzs7O1lBbkJGLFNBQVMsU0FBQztnQkFDVCxRQUFRLEVBQUUsbUJBQW1CO2dCQUM3QiwrbkJBQWlEO2dCQUNqRCxtQkFBbUIsRUFBRSxLQUFLO2dCQUMxQixlQUFlLEVBQUUsdUJBQXVCLENBQUMsTUFBTTtnQkFDL0MsYUFBYSxFQUFFLGlCQUFpQixDQUFDLElBQUk7YUFDdEM7Ozs7WUFiQyxVQUFVO1lBRVYsU0FBUzs7O3VCQWdCUixLQUFLO3NCQVVMLEtBQUs7NEJBRUwsS0FBSzs7OztJQWZOLDRDQUFlOztJQUNmLDRDQUE2Qjs7SUFZN0IsMENBQTZDOztJQUU3QyxnREFBbUQ7O0lBRXZDLDZDQUE2Qjs7Ozs7SUFBRSwyQ0FBMkIiLCJzb3VyY2VzQ29udGVudCI6WyJpbXBvcnQge1xuICBDaGFuZ2VEZXRlY3Rpb25TdHJhdGVneSxcbiAgQ29tcG9uZW50LFxuICBFbGVtZW50UmVmLFxuICBJbnB1dCxcbiAgUmVuZGVyZXIyLFxuICBUZW1wbGF0ZVJlZixcbiAgVmlld0VuY2Fwc3VsYXRpb25cbn0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5cbkBDb21wb25lbnQoe1xuICBzZWxlY3RvcjogJ256LWxpc3QtaXRlbS1tZXRhJyxcbiAgdGVtcGxhdGVVcmw6ICcuL256LWxpc3QtaXRlbS1tZXRhLmNvbXBvbmVudC5odG1sJyxcbiAgcHJlc2VydmVXaGl0ZXNwYWNlczogZmFsc2UsXG4gIGNoYW5nZURldGVjdGlvbjogQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3kuT25QdXNoLFxuICBlbmNhcHN1bGF0aW9uOiBWaWV3RW5jYXBzdWxhdGlvbi5Ob25lXG59KVxuZXhwb3J0IGNsYXNzIE56TGlzdEl0ZW1NZXRhQ29tcG9uZW50IHtcbiAgYXZhdGFyU3RyID0gJyc7XG4gIGF2YXRhclRwbDogVGVtcGxhdGVSZWY8dm9pZD47XG5cbiAgQElucHV0KClcbiAgc2V0IG56QXZhdGFyKHZhbHVlOiBzdHJpbmcgfCBUZW1wbGF0ZVJlZjx2b2lkPikge1xuICAgIGlmICh2YWx1ZSBpbnN0YW5jZW9mIFRlbXBsYXRlUmVmKSB7XG4gICAgICB0aGlzLmF2YXRhclN0ciA9ICcnO1xuICAgICAgdGhpcy5hdmF0YXJUcGwgPSB2YWx1ZTtcbiAgICB9IGVsc2Uge1xuICAgICAgdGhpcy5hdmF0YXJTdHIgPSB2YWx1ZTtcbiAgICB9XG4gIH1cblxuICBASW5wdXQoKSBuelRpdGxlOiBzdHJpbmcgfCBUZW1wbGF0ZVJlZjx2b2lkPjtcblxuICBASW5wdXQoKSBuekRlc2NyaXB0aW9uOiBzdHJpbmcgfCBUZW1wbGF0ZVJlZjx2b2lkPjtcblxuICBjb25zdHJ1Y3RvcihwdWJsaWMgZWxlbWVudFJlZjogRWxlbWVudFJlZiwgcHJpdmF0ZSByZW5kZXJlcjogUmVuZGVyZXIyKSB7XG4gICAgdGhpcy5yZW5kZXJlci5hZGRDbGFzcyhlbGVtZW50UmVmLm5hdGl2ZUVsZW1lbnQsICdhbnQtbGlzdC1pdGVtLW1ldGEnKTtcbiAgfVxufVxuIl19