/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import * as tslib_1 from "tslib";
import { ChangeDetectorRef, Component, Input, ViewEncapsulation } from '@angular/core';
import { notificationMotion } from '../core/animation/notification';
import { NzMessageComponent } from '../message/nz-message.component';
import { NzNotificationContainerComponent } from './nz-notification-container.component';
var NzNotificationComponent = /** @class */ (function (_super) {
    tslib_1.__extends(NzNotificationComponent, _super);
    function NzNotificationComponent(container, cdr) {
        var _this = _super.call(this, container, cdr) || this;
        _this.container = container;
        _this.cdr = cdr;
        return _this;
    }
    /**
     * @return {?}
     */
    NzNotificationComponent.prototype.close = /**
     * @return {?}
     */
    function () {
        this._destroy(true);
    };
    Object.defineProperty(NzNotificationComponent.prototype, "state", {
        get: /**
         * @return {?}
         */
        function () {
            if (this.nzMessage.state === 'enter') {
                if (this.container.config.nzPlacement === 'topLeft' || this.container.config.nzPlacement === 'bottomLeft') {
                    return 'enterLeft';
                }
                else {
                    return 'enterRight';
                }
            }
            else {
                return this.nzMessage.state;
            }
        },
        enumerable: true,
        configurable: true
    });
    NzNotificationComponent.decorators = [
        { type: Component, args: [{
                    encapsulation: ViewEncapsulation.None,
                    selector: 'nz-notification',
                    preserveWhitespaces: false,
                    animations: [notificationMotion],
                    template: "<div class=\"ant-notification-notice ant-notification-notice-closable\"\n  [ngStyle]=\"nzMessage.options?.nzStyle\"\n  [ngClass]=\"nzMessage.options?.nzClass\"\n  [@notificationMotion]=\"state\"\n  (mouseenter)=\"onEnter()\"\n  (mouseleave)=\"onLeave()\">\n  <div *ngIf=\"!nzMessage.template\" class=\"ant-notification-notice-content\">\n    <div class=\"ant-notification-notice-content\" [ngClass]=\"{ 'ant-notification-notice-with-icon': nzMessage.type !== 'blank' }\">\n      <div [class.ant-notification-notice-with-icon]=\"nzMessage.type !== 'blank'\">\n        <ng-container [ngSwitch]=\"nzMessage.type\">\n          <i *ngSwitchCase=\"'success'\" nz-icon type=\"check-circle\" class=\"ant-notification-notice-icon ant-notification-notice-icon-success\"></i>\n          <i *ngSwitchCase=\"'info'\" nz-icon type=\"info-circle\" class=\"ant-notification-notice-icon ant-notification-notice-icon-info\"></i>\n          <i *ngSwitchCase=\"'warning'\" nz-icon type=\"exclamation-circle\" class=\"ant-notification-notice-icon ant-notification-notice-icon-warning\"></i>\n          <i *ngSwitchCase=\"'error'\" nz-icon type=\"close-circle\" class=\"ant-notification-notice-icon ant-notification-notice-icon-error\"></i>\n        </ng-container>\n        <div class=\"ant-notification-notice-message\" [innerHTML]=\"nzMessage.title\"></div>\n        <div class=\"ant-notification-notice-description\" [innerHTML]=\"nzMessage.content\"></div>\n      </div>\n    </div>\n  </div>\n  <ng-template\n    [ngIf]=\"nzMessage.template\"\n    [ngTemplateOutlet]=\"nzMessage.template\"\n    [ngTemplateOutletContext]=\"{ $implicit: this, data: nzMessage.options?.nzData }\">\n  </ng-template>\n  <a tabindex=\"0\" class=\"ant-notification-notice-close\" (click)=\"close()\">\n    <span class=\"ant-notification-notice-close-x\">\n      <i nz-icon type=\"close\" class=\"ant-notification-close-icon\"></i>\n    </span>\n  </a>\n</div>"
                }] }
    ];
    /** @nocollapse */
    NzNotificationComponent.ctorParameters = function () { return [
        { type: NzNotificationContainerComponent },
        { type: ChangeDetectorRef }
    ]; };
    NzNotificationComponent.propDecorators = {
        nzMessage: [{ type: Input }]
    };
    return NzNotificationComponent;
}(NzMessageComponent));
export { NzNotificationComponent };
if (false) {
    /** @type {?} */
    NzNotificationComponent.prototype.nzMessage;
    /**
     * @type {?}
     * @private
     */
    NzNotificationComponent.prototype.container;
    /**
     * @type {?}
     * @protected
     */
    NzNotificationComponent.prototype.cdr;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotbm90aWZpY2F0aW9uLmNvbXBvbmVudC5qcyIsInNvdXJjZVJvb3QiOiJuZzovL25nLXpvcnJvLWFudGQvIiwic291cmNlcyI6WyJub3RpZmljYXRpb24vbnotbm90aWZpY2F0aW9uLmNvbXBvbmVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7OztBQUFBLE9BQU8sRUFBRSxpQkFBaUIsRUFBRSxTQUFTLEVBQUUsS0FBSyxFQUFFLGlCQUFpQixFQUFFLE1BQU0sZUFBZSxDQUFDO0FBRXZGLE9BQU8sRUFBRSxrQkFBa0IsRUFBRSxNQUFNLGdDQUFnQyxDQUFDO0FBQ3BFLE9BQU8sRUFBRSxrQkFBa0IsRUFBRSxNQUFNLGlDQUFpQyxDQUFDO0FBRXJFLE9BQU8sRUFBRSxnQ0FBZ0MsRUFBRSxNQUFNLHVDQUF1QyxDQUFDO0FBR3pGO0lBTzZDLG1EQUFrQjtJQUc3RCxpQ0FBb0IsU0FBMkMsRUFBWSxHQUFzQjtRQUFqRyxZQUNFLGtCQUFNLFNBQVMsRUFBRSxHQUFHLENBQUMsU0FDdEI7UUFGbUIsZUFBUyxHQUFULFNBQVMsQ0FBa0M7UUFBWSxTQUFHLEdBQUgsR0FBRyxDQUFtQjs7SUFFakcsQ0FBQzs7OztJQUVELHVDQUFLOzs7SUFBTDtRQUNFLElBQUksQ0FBQyxRQUFRLENBQUMsSUFBSSxDQUFDLENBQUM7SUFDdEIsQ0FBQztJQUVELHNCQUFJLDBDQUFLOzs7O1FBQVQ7WUFDRSxJQUFJLElBQUksQ0FBQyxTQUFTLENBQUMsS0FBSyxLQUFLLE9BQU8sRUFBRTtnQkFDcEMsSUFBSSxJQUFJLENBQUMsU0FBUyxDQUFDLE1BQU0sQ0FBQyxXQUFXLEtBQUssU0FBUyxJQUFJLElBQUksQ0FBQyxTQUFTLENBQUMsTUFBTSxDQUFDLFdBQVcsS0FBSyxZQUFZLEVBQUU7b0JBQ3pHLE9BQU8sV0FBVyxDQUFDO2lCQUNwQjtxQkFBTTtvQkFDTCxPQUFPLFlBQVksQ0FBQztpQkFDckI7YUFDRjtpQkFBTTtnQkFDTCxPQUFPLElBQUksQ0FBQyxTQUFTLENBQUMsS0FBSyxDQUFDO2FBQzdCO1FBQ0gsQ0FBQzs7O09BQUE7O2dCQTVCRixTQUFTLFNBQUM7b0JBQ1QsYUFBYSxFQUFFLGlCQUFpQixDQUFDLElBQUk7b0JBQ3JDLFFBQVEsRUFBRSxpQkFBaUI7b0JBQzNCLG1CQUFtQixFQUFFLEtBQUs7b0JBQzFCLFVBQVUsRUFBRSxDQUFDLGtCQUFrQixDQUFDO29CQUNoQyw2NERBQStDO2lCQUNoRDs7OztnQkFUUSxnQ0FBZ0M7Z0JBTGhDLGlCQUFpQjs7OzRCQWdCdkIsS0FBSzs7SUFxQlIsOEJBQUM7Q0FBQSxBQTdCRCxDQU82QyxrQkFBa0IsR0FzQjlEO1NBdEJZLHVCQUF1Qjs7O0lBQ2xDLDRDQUE2Qzs7Ozs7SUFFakMsNENBQW1EOzs7OztJQUFFLHNDQUFnQyIsInNvdXJjZXNDb250ZW50IjpbImltcG9ydCB7IENoYW5nZURldGVjdG9yUmVmLCBDb21wb25lbnQsIElucHV0LCBWaWV3RW5jYXBzdWxhdGlvbiB9IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuXG5pbXBvcnQgeyBub3RpZmljYXRpb25Nb3Rpb24gfSBmcm9tICcuLi9jb3JlL2FuaW1hdGlvbi9ub3RpZmljYXRpb24nO1xuaW1wb3J0IHsgTnpNZXNzYWdlQ29tcG9uZW50IH0gZnJvbSAnLi4vbWVzc2FnZS9uei1tZXNzYWdlLmNvbXBvbmVudCc7XG5cbmltcG9ydCB7IE56Tm90aWZpY2F0aW9uQ29udGFpbmVyQ29tcG9uZW50IH0gZnJvbSAnLi9uei1ub3RpZmljYXRpb24tY29udGFpbmVyLmNvbXBvbmVudCc7XG5pbXBvcnQgeyBOek5vdGlmaWNhdGlvbkRhdGFGaWxsZWQgfSBmcm9tICcuL256LW5vdGlmaWNhdGlvbi5kZWZpbml0aW9ucyc7XG5cbkBDb21wb25lbnQoe1xuICBlbmNhcHN1bGF0aW9uOiBWaWV3RW5jYXBzdWxhdGlvbi5Ob25lLFxuICBzZWxlY3RvcjogJ256LW5vdGlmaWNhdGlvbicsXG4gIHByZXNlcnZlV2hpdGVzcGFjZXM6IGZhbHNlLFxuICBhbmltYXRpb25zOiBbbm90aWZpY2F0aW9uTW90aW9uXSxcbiAgdGVtcGxhdGVVcmw6ICcuL256LW5vdGlmaWNhdGlvbi5jb21wb25lbnQuaHRtbCdcbn0pXG5leHBvcnQgY2xhc3MgTnpOb3RpZmljYXRpb25Db21wb25lbnQgZXh0ZW5kcyBOek1lc3NhZ2VDb21wb25lbnQge1xuICBASW5wdXQoKSBuek1lc3NhZ2U6IE56Tm90aWZpY2F0aW9uRGF0YUZpbGxlZDtcblxuICBjb25zdHJ1Y3Rvcihwcml2YXRlIGNvbnRhaW5lcjogTnpOb3RpZmljYXRpb25Db250YWluZXJDb21wb25lbnQsIHByb3RlY3RlZCBjZHI6IENoYW5nZURldGVjdG9yUmVmKSB7XG4gICAgc3VwZXIoY29udGFpbmVyLCBjZHIpO1xuICB9XG5cbiAgY2xvc2UoKTogdm9pZCB7XG4gICAgdGhpcy5fZGVzdHJveSh0cnVlKTtcbiAgfVxuXG4gIGdldCBzdGF0ZSgpOiBzdHJpbmcgfCB1bmRlZmluZWQge1xuICAgIGlmICh0aGlzLm56TWVzc2FnZS5zdGF0ZSA9PT0gJ2VudGVyJykge1xuICAgICAgaWYgKHRoaXMuY29udGFpbmVyLmNvbmZpZy5uelBsYWNlbWVudCA9PT0gJ3RvcExlZnQnIHx8IHRoaXMuY29udGFpbmVyLmNvbmZpZy5uelBsYWNlbWVudCA9PT0gJ2JvdHRvbUxlZnQnKSB7XG4gICAgICAgIHJldHVybiAnZW50ZXJMZWZ0JztcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIHJldHVybiAnZW50ZXJSaWdodCc7XG4gICAgICB9XG4gICAgfSBlbHNlIHtcbiAgICAgIHJldHVybiB0aGlzLm56TWVzc2FnZS5zdGF0ZTtcbiAgICB9XG4gIH1cbn1cbiJdfQ==