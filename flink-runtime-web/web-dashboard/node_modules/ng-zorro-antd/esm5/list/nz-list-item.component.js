/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { ChangeDetectionStrategy, Component, ContentChildren, ElementRef, Input, QueryList, Renderer2, TemplateRef, ViewEncapsulation } from '@angular/core';
import { NzListItemMetaComponent } from './nz-list-item-meta.component';
var NzListItemComponent = /** @class */ (function () {
    function NzListItemComponent(elementRef, renderer) {
        this.elementRef = elementRef;
        this.renderer = renderer;
        this.nzActions = [];
        this.renderer.addClass(this.elementRef.nativeElement, 'ant-list-item');
    }
    NzListItemComponent.decorators = [
        { type: Component, args: [{
                    selector: 'nz-list-item',
                    template: "<ng-template #contentTpl>\n  <div *ngIf=\"nzContent\" class=\"ant-list-item-content\" [ngClass]=\"{'ant-list-item-content-single': metas.length < 1}\">\n    <ng-container *nzStringTemplateOutlet=\"nzContent\">{{ nzContent }}</ng-container>\n  </div>\n</ng-template>\n<ng-template #actionsTpl>\n  <ul *ngIf=\"nzActions?.length > 0\" class=\"ant-list-item-action\">\n    <li *ngFor=\"let i of nzActions; let last=last;\">\n      <ng-template [ngTemplateOutlet]=\"i\"></ng-template>\n      <em *ngIf=\"!last\" class=\"ant-list-item-action-split\"></em>\n    </li>\n  </ul>\n</ng-template>\n<ng-template #mainTpl>\n  <ng-content></ng-content>\n  <ng-template [ngTemplateOutlet]=\"contentTpl\"></ng-template>\n  <ng-template [ngTemplateOutlet]=\"actionsTpl\"></ng-template>\n</ng-template>\n<div *ngIf=\"nzExtra; else mainTpl\" class=\"ant-list-item-extra-wrap\">\n  <div class=\"ant-list-item-main\">\n    <ng-template [ngTemplateOutlet]=\"mainTpl\"></ng-template>\n  </div>\n  <div class=\"ant-list-item-extra\">\n    <ng-template [ngTemplateOutlet]=\"nzExtra\"></ng-template>\n  </div>\n</div>",
                    preserveWhitespaces: false,
                    encapsulation: ViewEncapsulation.None,
                    changeDetection: ChangeDetectionStrategy.OnPush
                }] }
    ];
    /** @nocollapse */
    NzListItemComponent.ctorParameters = function () { return [
        { type: ElementRef },
        { type: Renderer2 }
    ]; };
    NzListItemComponent.propDecorators = {
        metas: [{ type: ContentChildren, args: [NzListItemMetaComponent,] }],
        nzActions: [{ type: Input }],
        nzContent: [{ type: Input }],
        nzExtra: [{ type: Input }]
    };
    return NzListItemComponent;
}());
export { NzListItemComponent };
if (false) {
    /** @type {?} */
    NzListItemComponent.prototype.metas;
    /** @type {?} */
    NzListItemComponent.prototype.nzActions;
    /** @type {?} */
    NzListItemComponent.prototype.nzContent;
    /** @type {?} */
    NzListItemComponent.prototype.nzExtra;
    /** @type {?} */
    NzListItemComponent.prototype.elementRef;
    /**
     * @type {?}
     * @private
     */
    NzListItemComponent.prototype.renderer;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotbGlzdC1pdGVtLmNvbXBvbmVudC5qcyIsInNvdXJjZVJvb3QiOiJuZzovL25nLXpvcnJvLWFudGQvIiwic291cmNlcyI6WyJsaXN0L256LWxpc3QtaXRlbS5jb21wb25lbnQudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7OztBQUFBLE9BQU8sRUFDTCx1QkFBdUIsRUFDdkIsU0FBUyxFQUNULGVBQWUsRUFDZixVQUFVLEVBQ1YsS0FBSyxFQUNMLFNBQVMsRUFDVCxTQUFTLEVBQ1QsV0FBVyxFQUNYLGlCQUFpQixFQUNsQixNQUFNLGVBQWUsQ0FBQztBQUV2QixPQUFPLEVBQUUsdUJBQXVCLEVBQUUsTUFBTSwrQkFBK0IsQ0FBQztBQUV4RTtJQWFFLDZCQUFtQixVQUFzQixFQUFVLFFBQW1CO1FBQW5ELGVBQVUsR0FBVixVQUFVLENBQVk7UUFBVSxhQUFRLEdBQVIsUUFBUSxDQUFXO1FBSjdELGNBQVMsR0FBNkIsRUFBRSxDQUFDO1FBS2hELElBQUksQ0FBQyxRQUFRLENBQUMsUUFBUSxDQUFDLElBQUksQ0FBQyxVQUFVLENBQUMsYUFBYSxFQUFFLGVBQWUsQ0FBQyxDQUFDO0lBQ3pFLENBQUM7O2dCQWZGLFNBQVMsU0FBQztvQkFDVCxRQUFRLEVBQUUsY0FBYztvQkFDeEIsK2tDQUE0QztvQkFDNUMsbUJBQW1CLEVBQUUsS0FBSztvQkFDMUIsYUFBYSxFQUFFLGlCQUFpQixDQUFDLElBQUk7b0JBQ3JDLGVBQWUsRUFBRSx1QkFBdUIsQ0FBQyxNQUFNO2lCQUNoRDs7OztnQkFoQkMsVUFBVTtnQkFHVixTQUFTOzs7d0JBZVIsZUFBZSxTQUFDLHVCQUF1Qjs0QkFDdkMsS0FBSzs0QkFDTCxLQUFLOzBCQUNMLEtBQUs7O0lBS1IsMEJBQUM7Q0FBQSxBQWhCRCxJQWdCQztTQVRZLG1CQUFtQjs7O0lBQzlCLG9DQUFxRjs7SUFDckYsd0NBQWtEOztJQUNsRCx3Q0FBK0M7O0lBQy9DLHNDQUFvQzs7SUFFeEIseUNBQTZCOzs7OztJQUFFLHVDQUEyQiIsInNvdXJjZXNDb250ZW50IjpbImltcG9ydCB7XG4gIENoYW5nZURldGVjdGlvblN0cmF0ZWd5LFxuICBDb21wb25lbnQsXG4gIENvbnRlbnRDaGlsZHJlbixcbiAgRWxlbWVudFJlZixcbiAgSW5wdXQsXG4gIFF1ZXJ5TGlzdCxcbiAgUmVuZGVyZXIyLFxuICBUZW1wbGF0ZVJlZixcbiAgVmlld0VuY2Fwc3VsYXRpb25cbn0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5cbmltcG9ydCB7IE56TGlzdEl0ZW1NZXRhQ29tcG9uZW50IH0gZnJvbSAnLi9uei1saXN0LWl0ZW0tbWV0YS5jb21wb25lbnQnO1xuXG5AQ29tcG9uZW50KHtcbiAgc2VsZWN0b3I6ICduei1saXN0LWl0ZW0nLFxuICB0ZW1wbGF0ZVVybDogJy4vbnotbGlzdC1pdGVtLmNvbXBvbmVudC5odG1sJyxcbiAgcHJlc2VydmVXaGl0ZXNwYWNlczogZmFsc2UsXG4gIGVuY2Fwc3VsYXRpb246IFZpZXdFbmNhcHN1bGF0aW9uLk5vbmUsXG4gIGNoYW5nZURldGVjdGlvbjogQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3kuT25QdXNoXG59KVxuZXhwb3J0IGNsYXNzIE56TGlzdEl0ZW1Db21wb25lbnQge1xuICBAQ29udGVudENoaWxkcmVuKE56TGlzdEl0ZW1NZXRhQ29tcG9uZW50KSBtZXRhcyE6IFF1ZXJ5TGlzdDxOekxpc3RJdGVtTWV0YUNvbXBvbmVudD47XG4gIEBJbnB1dCgpIG56QWN0aW9uczogQXJyYXk8VGVtcGxhdGVSZWY8dm9pZD4+ID0gW107XG4gIEBJbnB1dCgpIG56Q29udGVudDogc3RyaW5nIHwgVGVtcGxhdGVSZWY8dm9pZD47XG4gIEBJbnB1dCgpIG56RXh0cmE6IFRlbXBsYXRlUmVmPHZvaWQ+O1xuXG4gIGNvbnN0cnVjdG9yKHB1YmxpYyBlbGVtZW50UmVmOiBFbGVtZW50UmVmLCBwcml2YXRlIHJlbmRlcmVyOiBSZW5kZXJlcjIpIHtcbiAgICB0aGlzLnJlbmRlcmVyLmFkZENsYXNzKHRoaXMuZWxlbWVudFJlZi5uYXRpdmVFbGVtZW50LCAnYW50LWxpc3QtaXRlbScpO1xuICB9XG59XG4iXX0=