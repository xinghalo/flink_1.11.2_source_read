/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { ChangeDetectionStrategy, ChangeDetectorRef, Component, ContentChild, Host, Input, Optional, TemplateRef, ViewEncapsulation } from '@angular/core';
import { zoomBigMotion } from '../core/animation/zoom';
import { NzNoAnimationDirective } from '../core/no-animation/nz-no-animation.directive';
import { isNotNil } from '../core/util';
import { NzToolTipComponent } from '../tooltip/nz-tooltip.component';
export class NzPopoverComponent extends NzToolTipComponent {
    /**
     * @param {?} cdr
     * @param {?=} noAnimation
     */
    constructor(cdr, noAnimation) {
        super(cdr, noAnimation);
        this.noAnimation = noAnimation;
        this._prefix = 'ant-popover-placement';
    }
    /**
     * @protected
     * @return {?}
     */
    isContentEmpty() {
        /** @type {?} */
        const isTitleEmpty = this.nzTitle instanceof TemplateRef ? false : this.nzTitle === '' || !isNotNil(this.nzTitle);
        /** @type {?} */
        const isContentEmpty = this.nzContent instanceof TemplateRef ? false : this.nzContent === '' || !isNotNil(this.nzContent);
        return isTitleEmpty && isContentEmpty;
    }
}
NzPopoverComponent.decorators = [
    { type: Component, args: [{
                selector: 'nz-popover',
                animations: [zoomBigMotion],
                template: "<ng-content></ng-content>\n<ng-template\n  #overlay=\"cdkConnectedOverlay\"\n  cdkConnectedOverlay\n  nzConnectedOverlay\n  [cdkConnectedOverlayOrigin]=\"overlayOrigin\"\n  [cdkConnectedOverlayHasBackdrop]=\"_hasBackdrop\"\n  (backdropClick)=\"hide()\"\n  (detach)=\"hide()\"\n  (positionChange)=\"onPositionChange($event)\"\n  [cdkConnectedOverlayPositions]=\"_positions\"\n  [cdkConnectedOverlayOpen]=\"visible$ | async\">\n  <div class=\"ant-popover\"\n    [ngClass]=\"_classMap\"\n    [ngStyle]=\"nzOverlayStyle\"\n    [nzNoAnimation]=\"noAnimation?.nzNoAnimation\"\n    [@zoomBigMotion]=\"'active'\"\n    (@zoomBigMotion.done)=\"_afterVisibilityAnimation($event)\">\n    <div class=\"ant-popover-content\">\n      <div class=\"ant-popover-arrow\"></div>\n      <div class=\"ant-popover-inner\" role=\"tooltip\">\n        <div>\n          <div class=\"ant-popover-title\" *ngIf=\"nzTitle\">\n            <ng-container *nzStringTemplateOutlet=\"nzTitle\">{{ nzTitle }}</ng-container>\n          </div>\n          <div class=\"ant-popover-inner-content\">\n            <ng-container *nzStringTemplateOutlet=\"nzContent\">{{ nzContent }}</ng-container>\n          </div>\n        </div>\n      </div>\n    </div>\n  </div>\n</ng-template>",
                changeDetection: ChangeDetectionStrategy.OnPush,
                encapsulation: ViewEncapsulation.None,
                preserveWhitespaces: false,
                styles: [`
      .ant-popover {
        position: relative;
      }
    `]
            }] }
];
/** @nocollapse */
NzPopoverComponent.ctorParameters = () => [
    { type: ChangeDetectorRef },
    { type: NzNoAnimationDirective, decorators: [{ type: Host }, { type: Optional }] }
];
NzPopoverComponent.propDecorators = {
    nzTitle: [{ type: Input }, { type: ContentChild, args: ['neverUsedTemplate',] }],
    nzContent: [{ type: Input }, { type: ContentChild, args: ['nzTemplate',] }]
};
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
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotcG9wb3Zlci5jb21wb25lbnQuanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsicG9wb3Zlci9uei1wb3BvdmVyLmNvbXBvbmVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7O0FBQUEsT0FBTyxFQUNMLHVCQUF1QixFQUN2QixpQkFBaUIsRUFDakIsU0FBUyxFQUNULFlBQVksRUFDWixJQUFJLEVBQ0osS0FBSyxFQUNMLFFBQVEsRUFDUixXQUFXLEVBQ1gsaUJBQWlCLEVBQ2xCLE1BQU0sZUFBZSxDQUFDO0FBRXZCLE9BQU8sRUFBRSxhQUFhLEVBQUUsTUFBTSx3QkFBd0IsQ0FBQztBQUN2RCxPQUFPLEVBQUUsc0JBQXNCLEVBQUUsTUFBTSxnREFBZ0QsQ0FBQztBQUN4RixPQUFPLEVBQUUsUUFBUSxFQUFFLE1BQU0sY0FBYyxDQUFDO0FBQ3hDLE9BQU8sRUFBRSxrQkFBa0IsRUFBRSxNQUFNLGlDQUFpQyxDQUFDO0FBaUJyRSxNQUFNLE9BQU8sa0JBQW1CLFNBQVEsa0JBQWtCOzs7OztJQU94RCxZQUFZLEdBQXNCLEVBQTZCLFdBQW9DO1FBQ2pHLEtBQUssQ0FBQyxHQUFHLEVBQUUsV0FBVyxDQUFDLENBQUM7UUFEcUMsZ0JBQVcsR0FBWCxXQUFXLENBQXlCO1FBTm5HLFlBQU8sR0FBRyx1QkFBdUIsQ0FBQztJQVFsQyxDQUFDOzs7OztJQUVTLGNBQWM7O2NBQ2hCLFlBQVksR0FBRyxJQUFJLENBQUMsT0FBTyxZQUFZLFdBQVcsQ0FBQyxDQUFDLENBQUMsS0FBSyxDQUFDLENBQUMsQ0FBQyxJQUFJLENBQUMsT0FBTyxLQUFLLEVBQUUsSUFBSSxDQUFDLFFBQVEsQ0FBQyxJQUFJLENBQUMsT0FBTyxDQUFDOztjQUMzRyxjQUFjLEdBQ2xCLElBQUksQ0FBQyxTQUFTLFlBQVksV0FBVyxDQUFDLENBQUMsQ0FBQyxLQUFLLENBQUMsQ0FBQyxDQUFDLElBQUksQ0FBQyxTQUFTLEtBQUssRUFBRSxJQUFJLENBQUMsUUFBUSxDQUFDLElBQUksQ0FBQyxTQUFTLENBQUM7UUFDcEcsT0FBTyxZQUFZLElBQUksY0FBYyxDQUFDO0lBQ3hDLENBQUM7OztZQS9CRixTQUFTLFNBQUM7Z0JBQ1QsUUFBUSxFQUFFLFlBQVk7Z0JBQ3RCLFVBQVUsRUFBRSxDQUFDLGFBQWEsQ0FBQztnQkFDM0IsbXVDQUEwQztnQkFDMUMsZUFBZSxFQUFFLHVCQUF1QixDQUFDLE1BQU07Z0JBQy9DLGFBQWEsRUFBRSxpQkFBaUIsQ0FBQyxJQUFJO2dCQUNyQyxtQkFBbUIsRUFBRSxLQUFLO3lCQUV4Qjs7OztLQUlDO2FBRUo7Ozs7WUE3QkMsaUJBQWlCO1lBV1Ysc0JBQXNCLHVCQTBCUSxJQUFJLFlBQUksUUFBUTs7O3NCQUhwRCxLQUFLLFlBQUksWUFBWSxTQUFDLG1CQUFtQjt3QkFDekMsS0FBSyxZQUFJLFlBQVksU0FBQyxZQUFZOzs7O0lBSm5DLHFDQUFrQzs7Ozs7SUFHbEMscUNBQWdGOztJQUNoRix1Q0FBMkU7O0lBRXZDLHlDQUErRCIsInNvdXJjZXNDb250ZW50IjpbImltcG9ydCB7XG4gIENoYW5nZURldGVjdGlvblN0cmF0ZWd5LFxuICBDaGFuZ2VEZXRlY3RvclJlZixcbiAgQ29tcG9uZW50LFxuICBDb250ZW50Q2hpbGQsXG4gIEhvc3QsXG4gIElucHV0LFxuICBPcHRpb25hbCxcbiAgVGVtcGxhdGVSZWYsXG4gIFZpZXdFbmNhcHN1bGF0aW9uXG59IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuXG5pbXBvcnQgeyB6b29tQmlnTW90aW9uIH0gZnJvbSAnLi4vY29yZS9hbmltYXRpb24vem9vbSc7XG5pbXBvcnQgeyBOek5vQW5pbWF0aW9uRGlyZWN0aXZlIH0gZnJvbSAnLi4vY29yZS9uby1hbmltYXRpb24vbnotbm8tYW5pbWF0aW9uLmRpcmVjdGl2ZSc7XG5pbXBvcnQgeyBpc05vdE5pbCB9IGZyb20gJy4uL2NvcmUvdXRpbCc7XG5pbXBvcnQgeyBOelRvb2xUaXBDb21wb25lbnQgfSBmcm9tICcuLi90b29sdGlwL256LXRvb2x0aXAuY29tcG9uZW50JztcblxuQENvbXBvbmVudCh7XG4gIHNlbGVjdG9yOiAnbnotcG9wb3ZlcicsXG4gIGFuaW1hdGlvbnM6IFt6b29tQmlnTW90aW9uXSxcbiAgdGVtcGxhdGVVcmw6ICcuL256LXBvcG92ZXIuY29tcG9uZW50Lmh0bWwnLFxuICBjaGFuZ2VEZXRlY3Rpb246IENoYW5nZURldGVjdGlvblN0cmF0ZWd5Lk9uUHVzaCxcbiAgZW5jYXBzdWxhdGlvbjogVmlld0VuY2Fwc3VsYXRpb24uTm9uZSxcbiAgcHJlc2VydmVXaGl0ZXNwYWNlczogZmFsc2UsXG4gIHN0eWxlczogW1xuICAgIGBcbiAgICAgIC5hbnQtcG9wb3ZlciB7XG4gICAgICAgIHBvc2l0aW9uOiByZWxhdGl2ZTtcbiAgICAgIH1cbiAgICBgXG4gIF1cbn0pXG5leHBvcnQgY2xhc3MgTnpQb3BvdmVyQ29tcG9uZW50IGV4dGVuZHMgTnpUb29sVGlwQ29tcG9uZW50IHtcbiAgX3ByZWZpeCA9ICdhbnQtcG9wb3Zlci1wbGFjZW1lbnQnO1xuXG4gIC8qKiBVc2VkIHRvIHJlbW92ZSBOelRvb2xUaXBDb21wb25lbnQgQENvbnRlbnRDaGlsZCgnbnpUZW1wbGF0ZScpICovXG4gIEBJbnB1dCgpIEBDb250ZW50Q2hpbGQoJ25ldmVyVXNlZFRlbXBsYXRlJykgbnpUaXRsZTogc3RyaW5nIHwgVGVtcGxhdGVSZWY8dm9pZD47XG4gIEBJbnB1dCgpIEBDb250ZW50Q2hpbGQoJ256VGVtcGxhdGUnKSBuekNvbnRlbnQ6IHN0cmluZyB8IFRlbXBsYXRlUmVmPHZvaWQ+O1xuXG4gIGNvbnN0cnVjdG9yKGNkcjogQ2hhbmdlRGV0ZWN0b3JSZWYsIEBIb3N0KCkgQE9wdGlvbmFsKCkgcHVibGljIG5vQW5pbWF0aW9uPzogTnpOb0FuaW1hdGlvbkRpcmVjdGl2ZSkge1xuICAgIHN1cGVyKGNkciwgbm9BbmltYXRpb24pO1xuICB9XG5cbiAgcHJvdGVjdGVkIGlzQ29udGVudEVtcHR5KCk6IGJvb2xlYW4ge1xuICAgIGNvbnN0IGlzVGl0bGVFbXB0eSA9IHRoaXMubnpUaXRsZSBpbnN0YW5jZW9mIFRlbXBsYXRlUmVmID8gZmFsc2UgOiB0aGlzLm56VGl0bGUgPT09ICcnIHx8ICFpc05vdE5pbCh0aGlzLm56VGl0bGUpO1xuICAgIGNvbnN0IGlzQ29udGVudEVtcHR5ID1cbiAgICAgIHRoaXMubnpDb250ZW50IGluc3RhbmNlb2YgVGVtcGxhdGVSZWYgPyBmYWxzZSA6IHRoaXMubnpDb250ZW50ID09PSAnJyB8fCAhaXNOb3ROaWwodGhpcy5uekNvbnRlbnQpO1xuICAgIHJldHVybiBpc1RpdGxlRW1wdHkgJiYgaXNDb250ZW50RW1wdHk7XG4gIH1cbn1cbiJdfQ==