/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { ChangeDetectorRef, Component, Input, ViewEncapsulation } from '@angular/core';
import { notificationMotion } from '../core/animation/notification';
import { NzMessageComponent } from '../message/nz-message.component';
import { NzNotificationContainerComponent } from './nz-notification-container.component';
export class NzNotificationComponent extends NzMessageComponent {
    /**
     * @param {?} container
     * @param {?} cdr
     */
    constructor(container, cdr) {
        super(container, cdr);
        this.container = container;
        this.cdr = cdr;
    }
    /**
     * @return {?}
     */
    close() {
        this._destroy(true);
    }
    /**
     * @return {?}
     */
    get state() {
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
    }
}
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
NzNotificationComponent.ctorParameters = () => [
    { type: NzNotificationContainerComponent },
    { type: ChangeDetectorRef }
];
NzNotificationComponent.propDecorators = {
    nzMessage: [{ type: Input }]
};
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
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotbm90aWZpY2F0aW9uLmNvbXBvbmVudC5qcyIsInNvdXJjZVJvb3QiOiJuZzovL25nLXpvcnJvLWFudGQvIiwic291cmNlcyI6WyJub3RpZmljYXRpb24vbnotbm90aWZpY2F0aW9uLmNvbXBvbmVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7O0FBQUEsT0FBTyxFQUFFLGlCQUFpQixFQUFFLFNBQVMsRUFBRSxLQUFLLEVBQUUsaUJBQWlCLEVBQUUsTUFBTSxlQUFlLENBQUM7QUFFdkYsT0FBTyxFQUFFLGtCQUFrQixFQUFFLE1BQU0sZ0NBQWdDLENBQUM7QUFDcEUsT0FBTyxFQUFFLGtCQUFrQixFQUFFLE1BQU0saUNBQWlDLENBQUM7QUFFckUsT0FBTyxFQUFFLGdDQUFnQyxFQUFFLE1BQU0sdUNBQXVDLENBQUM7QUFVekYsTUFBTSxPQUFPLHVCQUF3QixTQUFRLGtCQUFrQjs7Ozs7SUFHN0QsWUFBb0IsU0FBMkMsRUFBWSxHQUFzQjtRQUMvRixLQUFLLENBQUMsU0FBUyxFQUFFLEdBQUcsQ0FBQyxDQUFDO1FBREosY0FBUyxHQUFULFNBQVMsQ0FBa0M7UUFBWSxRQUFHLEdBQUgsR0FBRyxDQUFtQjtJQUVqRyxDQUFDOzs7O0lBRUQsS0FBSztRQUNILElBQUksQ0FBQyxRQUFRLENBQUMsSUFBSSxDQUFDLENBQUM7SUFDdEIsQ0FBQzs7OztJQUVELElBQUksS0FBSztRQUNQLElBQUksSUFBSSxDQUFDLFNBQVMsQ0FBQyxLQUFLLEtBQUssT0FBTyxFQUFFO1lBQ3BDLElBQUksSUFBSSxDQUFDLFNBQVMsQ0FBQyxNQUFNLENBQUMsV0FBVyxLQUFLLFNBQVMsSUFBSSxJQUFJLENBQUMsU0FBUyxDQUFDLE1BQU0sQ0FBQyxXQUFXLEtBQUssWUFBWSxFQUFFO2dCQUN6RyxPQUFPLFdBQVcsQ0FBQzthQUNwQjtpQkFBTTtnQkFDTCxPQUFPLFlBQVksQ0FBQzthQUNyQjtTQUNGO2FBQU07WUFDTCxPQUFPLElBQUksQ0FBQyxTQUFTLENBQUMsS0FBSyxDQUFDO1NBQzdCO0lBQ0gsQ0FBQzs7O1lBNUJGLFNBQVMsU0FBQztnQkFDVCxhQUFhLEVBQUUsaUJBQWlCLENBQUMsSUFBSTtnQkFDckMsUUFBUSxFQUFFLGlCQUFpQjtnQkFDM0IsbUJBQW1CLEVBQUUsS0FBSztnQkFDMUIsVUFBVSxFQUFFLENBQUMsa0JBQWtCLENBQUM7Z0JBQ2hDLDY0REFBK0M7YUFDaEQ7Ozs7WUFUUSxnQ0FBZ0M7WUFMaEMsaUJBQWlCOzs7d0JBZ0J2QixLQUFLOzs7O0lBQU4sNENBQTZDOzs7OztJQUVqQyw0Q0FBbUQ7Ozs7O0lBQUUsc0NBQWdDIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHsgQ2hhbmdlRGV0ZWN0b3JSZWYsIENvbXBvbmVudCwgSW5wdXQsIFZpZXdFbmNhcHN1bGF0aW9uIH0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5cbmltcG9ydCB7IG5vdGlmaWNhdGlvbk1vdGlvbiB9IGZyb20gJy4uL2NvcmUvYW5pbWF0aW9uL25vdGlmaWNhdGlvbic7XG5pbXBvcnQgeyBOek1lc3NhZ2VDb21wb25lbnQgfSBmcm9tICcuLi9tZXNzYWdlL256LW1lc3NhZ2UuY29tcG9uZW50JztcblxuaW1wb3J0IHsgTnpOb3RpZmljYXRpb25Db250YWluZXJDb21wb25lbnQgfSBmcm9tICcuL256LW5vdGlmaWNhdGlvbi1jb250YWluZXIuY29tcG9uZW50JztcbmltcG9ydCB7IE56Tm90aWZpY2F0aW9uRGF0YUZpbGxlZCB9IGZyb20gJy4vbnotbm90aWZpY2F0aW9uLmRlZmluaXRpb25zJztcblxuQENvbXBvbmVudCh7XG4gIGVuY2Fwc3VsYXRpb246IFZpZXdFbmNhcHN1bGF0aW9uLk5vbmUsXG4gIHNlbGVjdG9yOiAnbnotbm90aWZpY2F0aW9uJyxcbiAgcHJlc2VydmVXaGl0ZXNwYWNlczogZmFsc2UsXG4gIGFuaW1hdGlvbnM6IFtub3RpZmljYXRpb25Nb3Rpb25dLFxuICB0ZW1wbGF0ZVVybDogJy4vbnotbm90aWZpY2F0aW9uLmNvbXBvbmVudC5odG1sJ1xufSlcbmV4cG9ydCBjbGFzcyBOek5vdGlmaWNhdGlvbkNvbXBvbmVudCBleHRlbmRzIE56TWVzc2FnZUNvbXBvbmVudCB7XG4gIEBJbnB1dCgpIG56TWVzc2FnZTogTnpOb3RpZmljYXRpb25EYXRhRmlsbGVkO1xuXG4gIGNvbnN0cnVjdG9yKHByaXZhdGUgY29udGFpbmVyOiBOek5vdGlmaWNhdGlvbkNvbnRhaW5lckNvbXBvbmVudCwgcHJvdGVjdGVkIGNkcjogQ2hhbmdlRGV0ZWN0b3JSZWYpIHtcbiAgICBzdXBlcihjb250YWluZXIsIGNkcik7XG4gIH1cblxuICBjbG9zZSgpOiB2b2lkIHtcbiAgICB0aGlzLl9kZXN0cm95KHRydWUpO1xuICB9XG5cbiAgZ2V0IHN0YXRlKCk6IHN0cmluZyB8IHVuZGVmaW5lZCB7XG4gICAgaWYgKHRoaXMubnpNZXNzYWdlLnN0YXRlID09PSAnZW50ZXInKSB7XG4gICAgICBpZiAodGhpcy5jb250YWluZXIuY29uZmlnLm56UGxhY2VtZW50ID09PSAndG9wTGVmdCcgfHwgdGhpcy5jb250YWluZXIuY29uZmlnLm56UGxhY2VtZW50ID09PSAnYm90dG9tTGVmdCcpIHtcbiAgICAgICAgcmV0dXJuICdlbnRlckxlZnQnO1xuICAgICAgfSBlbHNlIHtcbiAgICAgICAgcmV0dXJuICdlbnRlclJpZ2h0JztcbiAgICAgIH1cbiAgICB9IGVsc2Uge1xuICAgICAgcmV0dXJuIHRoaXMubnpNZXNzYWdlLnN0YXRlO1xuICAgIH1cbiAgfVxufVxuIl19