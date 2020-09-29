/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import * as tslib_1 from "tslib";
import { ChangeDetectionStrategy, ChangeDetectorRef, Component, EventEmitter, Host, Input, Optional, Output, ViewEncapsulation } from '@angular/core';
import { zoomBigMotion } from '../core/animation/zoom';
import { NzNoAnimationDirective } from '../core/no-animation/nz-no-animation.directive';
import { InputBoolean } from '../core/util/convert';
import { NzToolTipComponent } from '../tooltip/nz-tooltip.component';
var NzPopconfirmComponent = /** @class */ (function (_super) {
    tslib_1.__extends(NzPopconfirmComponent, _super);
    function NzPopconfirmComponent(cdr, noAnimation) {
        var _this = _super.call(this, cdr, noAnimation) || this;
        _this.noAnimation = noAnimation;
        _this._prefix = 'ant-popover-placement';
        _this._trigger = 'click';
        _this._hasBackdrop = true;
        _this.nzOkType = 'primary';
        _this.nzCondition = false;
        _this.nzOnCancel = new EventEmitter();
        _this.nzOnConfirm = new EventEmitter();
        return _this;
    }
    /**
     * @return {?}
     */
    NzPopconfirmComponent.prototype.show = /**
     * @return {?}
     */
    function () {
        if (!this.nzCondition) {
            this.nzVisible = true;
        }
        else {
            this.onConfirm();
        }
    };
    /**
     * @return {?}
     */
    NzPopconfirmComponent.prototype.onCancel = /**
     * @return {?}
     */
    function () {
        this.nzOnCancel.emit();
        this.nzVisible = false;
    };
    /**
     * @return {?}
     */
    NzPopconfirmComponent.prototype.onConfirm = /**
     * @return {?}
     */
    function () {
        this.nzOnConfirm.emit();
        this.nzVisible = false;
    };
    NzPopconfirmComponent.decorators = [
        { type: Component, args: [{
                    changeDetection: ChangeDetectionStrategy.OnPush,
                    encapsulation: ViewEncapsulation.None,
                    selector: 'nz-popconfirm',
                    preserveWhitespaces: false,
                    animations: [zoomBigMotion],
                    template: "<ng-content></ng-content>\n<ng-template\n  #overlay=\"cdkConnectedOverlay\"\n  cdkConnectedOverlay\n  nzConnectedOverlay\n  [cdkConnectedOverlayOrigin]=\"overlayOrigin\"\n  [cdkConnectedOverlayHasBackdrop]=\"_hasBackdrop\"\n  (backdropClick)=\"hide()\"\n  (detach)=\"hide()\"\n  (positionChange)=\"onPositionChange($event)\"\n  [cdkConnectedOverlayPositions]=\"_positions\"\n  [cdkConnectedOverlayOpen]=\"visible$ | async\">\n  <div class=\"ant-popover\"\n    [ngClass]=\"_classMap\"\n    [ngStyle]=\"nzOverlayStyle\"\n    [nzNoAnimation]=\"noAnimation?.nzNoAnimation\"\n    [@zoomBigMotion]=\"'active'\"\n    (@zoomBigMotion.done)=\"_afterVisibilityAnimation($event)\">\n    <div class=\"ant-popover-content\">\n      <div class=\"ant-popover-arrow\"></div>\n      <div class=\"ant-popover-inner\">\n        <div>\n          <div class=\"ant-popover-inner-content\">\n            <div class=\"ant-popover-message\">\n              <ng-container *nzStringTemplateOutlet=\"nzTitle\">\n                <ng-container *nzStringTemplateOutlet=\"nzIcon\">\n                  <i nz-icon [nzType]=\"nzIcon || 'exclamation-circle'\" nzTheme=\"fill\"></i>\n                </ng-container>\n                <div class=\"ant-popover-message-title\">{{ nzTitle }}</div>\n              </ng-container>\n            </div>\n            <div class=\"ant-popover-buttons\">\n              <button nz-button [nzSize]=\"'small'\" (click)=\"onCancel()\">\n                <ng-container *ngIf=\"nzCancelText\">{{ nzCancelText }}</ng-container>\n                <ng-container *ngIf=\"!nzCancelText\">{{ 'Modal.cancelText' | nzI18n }}</ng-container>\n              </button>\n              <button nz-button [nzSize]=\"'small'\" [nzType]=\"nzOkType\" (click)=\"onConfirm()\">\n                <ng-container *ngIf=\"nzOkText\">{{ nzOkText }}</ng-container>\n                <ng-container *ngIf=\"!nzOkText\">{{ 'Modal.okText' | nzI18n }}</ng-container>\n              </button>\n            </div>\n          </div>\n        </div>\n      </div>\n    </div>\n  </div>\n</ng-template>",
                    styles: ["\n      .ant-popover {\n        position: relative;\n      }\n    "]
                }] }
    ];
    /** @nocollapse */
    NzPopconfirmComponent.ctorParameters = function () { return [
        { type: ChangeDetectorRef },
        { type: NzNoAnimationDirective, decorators: [{ type: Host }, { type: Optional }] }
    ]; };
    NzPopconfirmComponent.propDecorators = {
        nzOkText: [{ type: Input }],
        nzOkType: [{ type: Input }],
        nzCancelText: [{ type: Input }],
        nzCondition: [{ type: Input }],
        nzIcon: [{ type: Input }],
        nzOnCancel: [{ type: Output }],
        nzOnConfirm: [{ type: Output }]
    };
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Object)
    ], NzPopconfirmComponent.prototype, "nzCondition", void 0);
    return NzPopconfirmComponent;
}(NzToolTipComponent));
export { NzPopconfirmComponent };
if (false) {
    /** @type {?} */
    NzPopconfirmComponent.prototype._prefix;
    /** @type {?} */
    NzPopconfirmComponent.prototype._trigger;
    /** @type {?} */
    NzPopconfirmComponent.prototype._hasBackdrop;
    /** @type {?} */
    NzPopconfirmComponent.prototype.nzOkText;
    /** @type {?} */
    NzPopconfirmComponent.prototype.nzOkType;
    /** @type {?} */
    NzPopconfirmComponent.prototype.nzCancelText;
    /** @type {?} */
    NzPopconfirmComponent.prototype.nzCondition;
    /** @type {?} */
    NzPopconfirmComponent.prototype.nzIcon;
    /** @type {?} */
    NzPopconfirmComponent.prototype.nzOnCancel;
    /** @type {?} */
    NzPopconfirmComponent.prototype.nzOnConfirm;
    /** @type {?} */
    NzPopconfirmComponent.prototype.noAnimation;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotcG9wY29uZmlybS5jb21wb25lbnQuanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsicG9wY29uZmlybS9uei1wb3Bjb25maXJtLmNvbXBvbmVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7OztBQUFBLE9BQU8sRUFDTCx1QkFBdUIsRUFDdkIsaUJBQWlCLEVBQ2pCLFNBQVMsRUFDVCxZQUFZLEVBQ1osSUFBSSxFQUNKLEtBQUssRUFDTCxRQUFRLEVBQ1IsTUFBTSxFQUVOLGlCQUFpQixFQUNsQixNQUFNLGVBQWUsQ0FBQztBQUN2QixPQUFPLEVBQUUsYUFBYSxFQUFFLE1BQU0sd0JBQXdCLENBQUM7QUFDdkQsT0FBTyxFQUFFLHNCQUFzQixFQUFFLE1BQU0sZ0RBQWdELENBQUM7QUFDeEYsT0FBTyxFQUFFLFlBQVksRUFBRSxNQUFNLHNCQUFzQixDQUFDO0FBQ3BELE9BQU8sRUFBRSxrQkFBa0IsRUFBRSxNQUFNLGlDQUFpQyxDQUFDO0FBRXJFO0lBZTJDLGlEQUFrQjtJQWMzRCwrQkFBWSxHQUFzQixFQUE2QixXQUFvQztRQUFuRyxZQUNFLGtCQUFNLEdBQUcsRUFBRSxXQUFXLENBQUMsU0FDeEI7UUFGOEQsaUJBQVcsR0FBWCxXQUFXLENBQXlCO1FBYm5HLGFBQU8sR0FBRyx1QkFBdUIsQ0FBQztRQUNsQyxjQUFRLEdBQUcsT0FBTyxDQUFDO1FBQ25CLGtCQUFZLEdBQUcsSUFBSSxDQUFDO1FBR1gsY0FBUSxHQUFXLFNBQVMsQ0FBQztRQUViLGlCQUFXLEdBQUcsS0FBSyxDQUFDO1FBRzFCLGdCQUFVLEdBQXVCLElBQUksWUFBWSxFQUFFLENBQUM7UUFDcEQsaUJBQVcsR0FBdUIsSUFBSSxZQUFZLEVBQUUsQ0FBQzs7SUFJeEUsQ0FBQzs7OztJQUVELG9DQUFJOzs7SUFBSjtRQUNFLElBQUksQ0FBQyxJQUFJLENBQUMsV0FBVyxFQUFFO1lBQ3JCLElBQUksQ0FBQyxTQUFTLEdBQUcsSUFBSSxDQUFDO1NBQ3ZCO2FBQU07WUFDTCxJQUFJLENBQUMsU0FBUyxFQUFFLENBQUM7U0FDbEI7SUFDSCxDQUFDOzs7O0lBRUQsd0NBQVE7OztJQUFSO1FBQ0UsSUFBSSxDQUFDLFVBQVUsQ0FBQyxJQUFJLEVBQUUsQ0FBQztRQUN2QixJQUFJLENBQUMsU0FBUyxHQUFHLEtBQUssQ0FBQztJQUN6QixDQUFDOzs7O0lBRUQseUNBQVM7OztJQUFUO1FBQ0UsSUFBSSxDQUFDLFdBQVcsQ0FBQyxJQUFJLEVBQUUsQ0FBQztRQUN4QixJQUFJLENBQUMsU0FBUyxHQUFHLEtBQUssQ0FBQztJQUN6QixDQUFDOztnQkFqREYsU0FBUyxTQUFDO29CQUNULGVBQWUsRUFBRSx1QkFBdUIsQ0FBQyxNQUFNO29CQUMvQyxhQUFhLEVBQUUsaUJBQWlCLENBQUMsSUFBSTtvQkFDckMsUUFBUSxFQUFFLGVBQWU7b0JBQ3pCLG1CQUFtQixFQUFFLEtBQUs7b0JBQzFCLFVBQVUsRUFBRSxDQUFDLGFBQWEsQ0FBQztvQkFDM0IsdWhFQUE2Qzs2QkFFM0Msb0VBSUM7aUJBRUo7Ozs7Z0JBN0JDLGlCQUFpQjtnQkFXVixzQkFBc0IsdUJBaUNRLElBQUksWUFBSSxRQUFROzs7MkJBVHBELEtBQUs7MkJBQ0wsS0FBSzsrQkFDTCxLQUFLOzhCQUNMLEtBQUs7eUJBQ0wsS0FBSzs2QkFFTCxNQUFNOzhCQUNOLE1BQU07O0lBSmtCO1FBQWYsWUFBWSxFQUFFOzs4REFBcUI7SUEyQi9DLDRCQUFDO0NBQUEsQUFsREQsQ0FlMkMsa0JBQWtCLEdBbUM1RDtTQW5DWSxxQkFBcUI7OztJQUNoQyx3Q0FBa0M7O0lBQ2xDLHlDQUFtQjs7SUFDbkIsNkNBQW9COztJQUVwQix5Q0FBMEI7O0lBQzFCLHlDQUFzQzs7SUFDdEMsNkNBQThCOztJQUM5Qiw0Q0FBNkM7O0lBQzdDLHVDQUE0Qzs7SUFFNUMsMkNBQXVFOztJQUN2RSw0Q0FBd0U7O0lBRXBDLDRDQUErRCIsInNvdXJjZXNDb250ZW50IjpbImltcG9ydCB7XG4gIENoYW5nZURldGVjdGlvblN0cmF0ZWd5LFxuICBDaGFuZ2VEZXRlY3RvclJlZixcbiAgQ29tcG9uZW50LFxuICBFdmVudEVtaXR0ZXIsXG4gIEhvc3QsXG4gIElucHV0LFxuICBPcHRpb25hbCxcbiAgT3V0cHV0LFxuICBUZW1wbGF0ZVJlZixcbiAgVmlld0VuY2Fwc3VsYXRpb25cbn0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5pbXBvcnQgeyB6b29tQmlnTW90aW9uIH0gZnJvbSAnLi4vY29yZS9hbmltYXRpb24vem9vbSc7XG5pbXBvcnQgeyBOek5vQW5pbWF0aW9uRGlyZWN0aXZlIH0gZnJvbSAnLi4vY29yZS9uby1hbmltYXRpb24vbnotbm8tYW5pbWF0aW9uLmRpcmVjdGl2ZSc7XG5pbXBvcnQgeyBJbnB1dEJvb2xlYW4gfSBmcm9tICcuLi9jb3JlL3V0aWwvY29udmVydCc7XG5pbXBvcnQgeyBOelRvb2xUaXBDb21wb25lbnQgfSBmcm9tICcuLi90b29sdGlwL256LXRvb2x0aXAuY29tcG9uZW50JztcblxuQENvbXBvbmVudCh7XG4gIGNoYW5nZURldGVjdGlvbjogQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3kuT25QdXNoLFxuICBlbmNhcHN1bGF0aW9uOiBWaWV3RW5jYXBzdWxhdGlvbi5Ob25lLFxuICBzZWxlY3RvcjogJ256LXBvcGNvbmZpcm0nLFxuICBwcmVzZXJ2ZVdoaXRlc3BhY2VzOiBmYWxzZSxcbiAgYW5pbWF0aW9uczogW3pvb21CaWdNb3Rpb25dLFxuICB0ZW1wbGF0ZVVybDogJy4vbnotcG9wY29uZmlybS5jb21wb25lbnQuaHRtbCcsXG4gIHN0eWxlczogW1xuICAgIGBcbiAgICAgIC5hbnQtcG9wb3ZlciB7XG4gICAgICAgIHBvc2l0aW9uOiByZWxhdGl2ZTtcbiAgICAgIH1cbiAgICBgXG4gIF1cbn0pXG5leHBvcnQgY2xhc3MgTnpQb3Bjb25maXJtQ29tcG9uZW50IGV4dGVuZHMgTnpUb29sVGlwQ29tcG9uZW50IHtcbiAgX3ByZWZpeCA9ICdhbnQtcG9wb3Zlci1wbGFjZW1lbnQnO1xuICBfdHJpZ2dlciA9ICdjbGljayc7XG4gIF9oYXNCYWNrZHJvcCA9IHRydWU7XG5cbiAgQElucHV0KCkgbnpPa1RleHQ6IHN0cmluZztcbiAgQElucHV0KCkgbnpPa1R5cGU6IHN0cmluZyA9ICdwcmltYXJ5JztcbiAgQElucHV0KCkgbnpDYW5jZWxUZXh0OiBzdHJpbmc7XG4gIEBJbnB1dCgpIEBJbnB1dEJvb2xlYW4oKSBuekNvbmRpdGlvbiA9IGZhbHNlO1xuICBASW5wdXQoKSBuekljb246IHN0cmluZyB8IFRlbXBsYXRlUmVmPHZvaWQ+O1xuXG4gIEBPdXRwdXQoKSByZWFkb25seSBuek9uQ2FuY2VsOiBFdmVudEVtaXR0ZXI8dm9pZD4gPSBuZXcgRXZlbnRFbWl0dGVyKCk7XG4gIEBPdXRwdXQoKSByZWFkb25seSBuek9uQ29uZmlybTogRXZlbnRFbWl0dGVyPHZvaWQ+ID0gbmV3IEV2ZW50RW1pdHRlcigpO1xuXG4gIGNvbnN0cnVjdG9yKGNkcjogQ2hhbmdlRGV0ZWN0b3JSZWYsIEBIb3N0KCkgQE9wdGlvbmFsKCkgcHVibGljIG5vQW5pbWF0aW9uPzogTnpOb0FuaW1hdGlvbkRpcmVjdGl2ZSkge1xuICAgIHN1cGVyKGNkciwgbm9BbmltYXRpb24pO1xuICB9XG5cbiAgc2hvdygpOiB2b2lkIHtcbiAgICBpZiAoIXRoaXMubnpDb25kaXRpb24pIHtcbiAgICAgIHRoaXMubnpWaXNpYmxlID0gdHJ1ZTtcbiAgICB9IGVsc2Uge1xuICAgICAgdGhpcy5vbkNvbmZpcm0oKTtcbiAgICB9XG4gIH1cblxuICBvbkNhbmNlbCgpOiB2b2lkIHtcbiAgICB0aGlzLm56T25DYW5jZWwuZW1pdCgpO1xuICAgIHRoaXMubnpWaXNpYmxlID0gZmFsc2U7XG4gIH1cblxuICBvbkNvbmZpcm0oKTogdm9pZCB7XG4gICAgdGhpcy5uek9uQ29uZmlybS5lbWl0KCk7XG4gICAgdGhpcy5uelZpc2libGUgPSBmYWxzZTtcbiAgfVxufVxuIl19