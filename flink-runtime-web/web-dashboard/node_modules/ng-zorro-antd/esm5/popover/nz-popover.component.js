/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import * as tslib_1 from "tslib";
import { ChangeDetectionStrategy, ChangeDetectorRef, Component, ContentChild, Host, Input, Optional, TemplateRef, ViewEncapsulation } from '@angular/core';
import { zoomBigMotion } from '../core/animation/zoom';
import { NzNoAnimationDirective } from '../core/no-animation/nz-no-animation.directive';
import { isNotNil } from '../core/util';
import { NzToolTipComponent } from '../tooltip/nz-tooltip.component';
var NzPopoverComponent = /** @class */ (function (_super) {
    tslib_1.__extends(NzPopoverComponent, _super);
    function NzPopoverComponent(cdr, noAnimation) {
        var _this = _super.call(this, cdr, noAnimation) || this;
        _this.noAnimation = noAnimation;
        _this._prefix = 'ant-popover-placement';
        return _this;
    }
    /**
     * @protected
     * @return {?}
     */
    NzPopoverComponent.prototype.isContentEmpty = /**
     * @protected
     * @return {?}
     */
    function () {
        /** @type {?} */
        var isTitleEmpty = this.nzTitle instanceof TemplateRef ? false : this.nzTitle === '' || !isNotNil(this.nzTitle);
        /** @type {?} */
        var isContentEmpty = this.nzContent instanceof TemplateRef ? false : this.nzContent === '' || !isNotNil(this.nzContent);
        return isTitleEmpty && isContentEmpty;
    };
    NzPopoverComponent.decorators = [
        { type: Component, args: [{
                    selector: 'nz-popover',
                    animations: [zoomBigMotion],
                    template: "<ng-content></ng-content>\n<ng-template\n  #overlay=\"cdkConnectedOverlay\"\n  cdkConnectedOverlay\n  nzConnectedOverlay\n  [cdkConnectedOverlayOrigin]=\"overlayOrigin\"\n  [cdkConnectedOverlayHasBackdrop]=\"_hasBackdrop\"\n  (backdropClick)=\"hide()\"\n  (detach)=\"hide()\"\n  (positionChange)=\"onPositionChange($event)\"\n  [cdkConnectedOverlayPositions]=\"_positions\"\n  [cdkConnectedOverlayOpen]=\"visible$ | async\">\n  <div class=\"ant-popover\"\n    [ngClass]=\"_classMap\"\n    [ngStyle]=\"nzOverlayStyle\"\n    [nzNoAnimation]=\"noAnimation?.nzNoAnimation\"\n    [@zoomBigMotion]=\"'active'\"\n    (@zoomBigMotion.done)=\"_afterVisibilityAnimation($event)\">\n    <div class=\"ant-popover-content\">\n      <div class=\"ant-popover-arrow\"></div>\n      <div class=\"ant-popover-inner\" role=\"tooltip\">\n        <div>\n          <div class=\"ant-popover-title\" *ngIf=\"nzTitle\">\n            <ng-container *nzStringTemplateOutlet=\"nzTitle\">{{ nzTitle }}</ng-container>\n          </div>\n          <div class=\"ant-popover-inner-content\">\n            <ng-container *nzStringTemplateOutlet=\"nzContent\">{{ nzContent }}</ng-container>\n          </div>\n        </div>\n      </div>\n    </div>\n  </div>\n</ng-template>",
                    changeDetection: ChangeDetectionStrategy.OnPush,
                    encapsulation: ViewEncapsulation.None,
                    preserveWhitespaces: false,
                    styles: ["\n      .ant-popover {\n        position: relative;\n      }\n    "]
                }] }
    ];
    /** @nocollapse */
    NzPopoverComponent.ctorParameters = function () { return [
        { type: ChangeDetectorRef },
        { type: NzNoAnimationDirective, decorators: [{ type: Host }, { type: Optional }] }
    ]; };
    NzPopoverComponent.propDecorators = {
        nzTitle: [{ type: Input }, { type: ContentChild, args: ['neverUsedTemplate',] }],
        nzContent: [{ type: Input }, { type: ContentChild, args: ['nzTemplate',] }]
    };
    return NzPopoverComponent;
}(NzToolTipComponent));
export { NzPopoverComponent };
if (false) {
    /** @type {?} */
    NzPopoverComponent.prototype._prefix;
    /**
     * Used to remove NzToolTipComponent \@ContentChild('nzTemplate')
     * @type {?}
     */
    NzPopoverComponent.prototype.nzTitle;
    /** @type {?} */
    NzPopoverComponent.prototype.nzContent;
    /** @type {?} */
    NzPopoverComponent.prototype.noAnimation;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotcG9wb3Zlci5jb21wb25lbnQuanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsicG9wb3Zlci9uei1wb3BvdmVyLmNvbXBvbmVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7OztBQUFBLE9BQU8sRUFDTCx1QkFBdUIsRUFDdkIsaUJBQWlCLEVBQ2pCLFNBQVMsRUFDVCxZQUFZLEVBQ1osSUFBSSxFQUNKLEtBQUssRUFDTCxRQUFRLEVBQ1IsV0FBVyxFQUNYLGlCQUFpQixFQUNsQixNQUFNLGVBQWUsQ0FBQztBQUV2QixPQUFPLEVBQUUsYUFBYSxFQUFFLE1BQU0sd0JBQXdCLENBQUM7QUFDdkQsT0FBTyxFQUFFLHNCQUFzQixFQUFFLE1BQU0sZ0RBQWdELENBQUM7QUFDeEYsT0FBTyxFQUFFLFFBQVEsRUFBRSxNQUFNLGNBQWMsQ0FBQztBQUN4QyxPQUFPLEVBQUUsa0JBQWtCLEVBQUUsTUFBTSxpQ0FBaUMsQ0FBQztBQUVyRTtJQWV3Qyw4Q0FBa0I7SUFPeEQsNEJBQVksR0FBc0IsRUFBNkIsV0FBb0M7UUFBbkcsWUFDRSxrQkFBTSxHQUFHLEVBQUUsV0FBVyxDQUFDLFNBQ3hCO1FBRjhELGlCQUFXLEdBQVgsV0FBVyxDQUF5QjtRQU5uRyxhQUFPLEdBQUcsdUJBQXVCLENBQUM7O0lBUWxDLENBQUM7Ozs7O0lBRVMsMkNBQWM7Ozs7SUFBeEI7O1lBQ1EsWUFBWSxHQUFHLElBQUksQ0FBQyxPQUFPLFlBQVksV0FBVyxDQUFDLENBQUMsQ0FBQyxLQUFLLENBQUMsQ0FBQyxDQUFDLElBQUksQ0FBQyxPQUFPLEtBQUssRUFBRSxJQUFJLENBQUMsUUFBUSxDQUFDLElBQUksQ0FBQyxPQUFPLENBQUM7O1lBQzNHLGNBQWMsR0FDbEIsSUFBSSxDQUFDLFNBQVMsWUFBWSxXQUFXLENBQUMsQ0FBQyxDQUFDLEtBQUssQ0FBQyxDQUFDLENBQUMsSUFBSSxDQUFDLFNBQVMsS0FBSyxFQUFFLElBQUksQ0FBQyxRQUFRLENBQUMsSUFBSSxDQUFDLFNBQVMsQ0FBQztRQUNwRyxPQUFPLFlBQVksSUFBSSxjQUFjLENBQUM7SUFDeEMsQ0FBQzs7Z0JBL0JGLFNBQVMsU0FBQztvQkFDVCxRQUFRLEVBQUUsWUFBWTtvQkFDdEIsVUFBVSxFQUFFLENBQUMsYUFBYSxDQUFDO29CQUMzQixtdUNBQTBDO29CQUMxQyxlQUFlLEVBQUUsdUJBQXVCLENBQUMsTUFBTTtvQkFDL0MsYUFBYSxFQUFFLGlCQUFpQixDQUFDLElBQUk7b0JBQ3JDLG1CQUFtQixFQUFFLEtBQUs7NkJBRXhCLG9FQUlDO2lCQUVKOzs7O2dCQTdCQyxpQkFBaUI7Z0JBV1Ysc0JBQXNCLHVCQTBCUSxJQUFJLFlBQUksUUFBUTs7OzBCQUhwRCxLQUFLLFlBQUksWUFBWSxTQUFDLG1CQUFtQjs0QkFDekMsS0FBSyxZQUFJLFlBQVksU0FBQyxZQUFZOztJQVlyQyx5QkFBQztDQUFBLEFBaENELENBZXdDLGtCQUFrQixHQWlCekQ7U0FqQlksa0JBQWtCOzs7SUFDN0IscUNBQWtDOzs7OztJQUdsQyxxQ0FBZ0Y7O0lBQ2hGLHVDQUEyRTs7SUFFdkMseUNBQStEIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHtcbiAgQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3ksXG4gIENoYW5nZURldGVjdG9yUmVmLFxuICBDb21wb25lbnQsXG4gIENvbnRlbnRDaGlsZCxcbiAgSG9zdCxcbiAgSW5wdXQsXG4gIE9wdGlvbmFsLFxuICBUZW1wbGF0ZVJlZixcbiAgVmlld0VuY2Fwc3VsYXRpb25cbn0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5cbmltcG9ydCB7IHpvb21CaWdNb3Rpb24gfSBmcm9tICcuLi9jb3JlL2FuaW1hdGlvbi96b29tJztcbmltcG9ydCB7IE56Tm9BbmltYXRpb25EaXJlY3RpdmUgfSBmcm9tICcuLi9jb3JlL25vLWFuaW1hdGlvbi9uei1uby1hbmltYXRpb24uZGlyZWN0aXZlJztcbmltcG9ydCB7IGlzTm90TmlsIH0gZnJvbSAnLi4vY29yZS91dGlsJztcbmltcG9ydCB7IE56VG9vbFRpcENvbXBvbmVudCB9IGZyb20gJy4uL3Rvb2x0aXAvbnotdG9vbHRpcC5jb21wb25lbnQnO1xuXG5AQ29tcG9uZW50KHtcbiAgc2VsZWN0b3I6ICduei1wb3BvdmVyJyxcbiAgYW5pbWF0aW9uczogW3pvb21CaWdNb3Rpb25dLFxuICB0ZW1wbGF0ZVVybDogJy4vbnotcG9wb3Zlci5jb21wb25lbnQuaHRtbCcsXG4gIGNoYW5nZURldGVjdGlvbjogQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3kuT25QdXNoLFxuICBlbmNhcHN1bGF0aW9uOiBWaWV3RW5jYXBzdWxhdGlvbi5Ob25lLFxuICBwcmVzZXJ2ZVdoaXRlc3BhY2VzOiBmYWxzZSxcbiAgc3R5bGVzOiBbXG4gICAgYFxuICAgICAgLmFudC1wb3BvdmVyIHtcbiAgICAgICAgcG9zaXRpb246IHJlbGF0aXZlO1xuICAgICAgfVxuICAgIGBcbiAgXVxufSlcbmV4cG9ydCBjbGFzcyBOelBvcG92ZXJDb21wb25lbnQgZXh0ZW5kcyBOelRvb2xUaXBDb21wb25lbnQge1xuICBfcHJlZml4ID0gJ2FudC1wb3BvdmVyLXBsYWNlbWVudCc7XG5cbiAgLyoqIFVzZWQgdG8gcmVtb3ZlIE56VG9vbFRpcENvbXBvbmVudCBAQ29udGVudENoaWxkKCduelRlbXBsYXRlJykgKi9cbiAgQElucHV0KCkgQENvbnRlbnRDaGlsZCgnbmV2ZXJVc2VkVGVtcGxhdGUnKSBuelRpdGxlOiBzdHJpbmcgfCBUZW1wbGF0ZVJlZjx2b2lkPjtcbiAgQElucHV0KCkgQENvbnRlbnRDaGlsZCgnbnpUZW1wbGF0ZScpIG56Q29udGVudDogc3RyaW5nIHwgVGVtcGxhdGVSZWY8dm9pZD47XG5cbiAgY29uc3RydWN0b3IoY2RyOiBDaGFuZ2VEZXRlY3RvclJlZiwgQEhvc3QoKSBAT3B0aW9uYWwoKSBwdWJsaWMgbm9BbmltYXRpb24/OiBOek5vQW5pbWF0aW9uRGlyZWN0aXZlKSB7XG4gICAgc3VwZXIoY2RyLCBub0FuaW1hdGlvbik7XG4gIH1cblxuICBwcm90ZWN0ZWQgaXNDb250ZW50RW1wdHkoKTogYm9vbGVhbiB7XG4gICAgY29uc3QgaXNUaXRsZUVtcHR5ID0gdGhpcy5uelRpdGxlIGluc3RhbmNlb2YgVGVtcGxhdGVSZWYgPyBmYWxzZSA6IHRoaXMubnpUaXRsZSA9PT0gJycgfHwgIWlzTm90TmlsKHRoaXMubnpUaXRsZSk7XG4gICAgY29uc3QgaXNDb250ZW50RW1wdHkgPVxuICAgICAgdGhpcy5uekNvbnRlbnQgaW5zdGFuY2VvZiBUZW1wbGF0ZVJlZiA/IGZhbHNlIDogdGhpcy5uekNvbnRlbnQgPT09ICcnIHx8ICFpc05vdE5pbCh0aGlzLm56Q29udGVudCk7XG4gICAgcmV0dXJuIGlzVGl0bGVFbXB0eSAmJiBpc0NvbnRlbnRFbXB0eTtcbiAgfVxufVxuIl19