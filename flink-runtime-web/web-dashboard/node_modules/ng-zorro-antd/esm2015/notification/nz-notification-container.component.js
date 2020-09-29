/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { ChangeDetectionStrategy, ChangeDetectorRef, Component, Inject, Optional, ViewEncapsulation } from '@angular/core';
import { Subject } from 'rxjs';
import { toCssPixel } from '../core/util';
import { NzMessageContainerComponent } from '../message/nz-message-container.component';
import { NZ_NOTIFICATION_CONFIG, NZ_NOTIFICATION_DEFAULT_CONFIG } from './nz-notification-config';
export class NzNotificationContainerComponent extends NzMessageContainerComponent {
    /**
     * @param {?} cdr
     * @param {?} defaultConfig
     * @param {?} config
     */
    constructor(cdr, defaultConfig, config) {
        super(cdr, defaultConfig, config);
        /**
         * @override
         */
        this.messages = [];
    }
    /**
     * @override
     * @param {?} config
     * @return {?}
     */
    setConfig(config) {
        /** @type {?} */
        const newConfig = (this.config = Object.assign({}, this.config, config));
        /** @type {?} */
        const placement = this.config.nzPlacement;
        this.top = placement === 'topLeft' || placement === 'topRight' ? toCssPixel(newConfig.nzTop) : null;
        this.bottom = placement === 'bottomLeft' || placement === 'bottomRight' ? toCssPixel(newConfig.nzBottom) : null;
        this.cdr.markForCheck();
    }
    /**
     * Create a new notification.
     * If there's a notification whose `nzKey` is same with `nzKey` in `NzNotificationDataFilled`,
     * replace its content instead of create a new one.
     * @override
     * @param {?} notification
     * @return {?}
     */
    createMessage(notification) {
        notification.options = this._mergeMessageOptions(notification.options);
        notification.onClose = new Subject();
        /** @type {?} */
        const key = notification.options.nzKey;
        /** @type {?} */
        const notificationWithSameKey = this.messages.find((/**
         * @param {?} msg
         * @return {?}
         */
        msg => msg.options.nzKey === ((/** @type {?} */ (notification.options))).nzKey));
        if (key && notificationWithSameKey) {
            this.replaceNotification(notificationWithSameKey, notification);
        }
        else {
            if (this.messages.length >= this.config.nzMaxStack) {
                this.messages.splice(0, 1);
            }
            this.messages.push((/** @type {?} */ (notification)));
        }
        this.cdr.detectChanges();
    }
    /**
     * @private
     * @param {?} old
     * @param {?} _new
     * @return {?}
     */
    replaceNotification(old, _new) {
        old.title = _new.title;
        old.content = _new.content;
        old.template = _new.template;
        old.type = _new.type;
    }
}
NzNotificationContainerComponent.decorators = [
    { type: Component, args: [{
                changeDetection: ChangeDetectionStrategy.OnPush,
                encapsulation: ViewEncapsulation.None,
                selector: 'nz-notification-container',
                preserveWhitespaces: false,
                template: "<div\n  class=\"ant-notification ant-notification-{{config.nzPlacement}}\"\n  [style.top]=\"(config.nzPlacement === 'topLeft' || config.nzPlacement === 'topRight') ? top : null\"\n  [style.bottom]=\"(config.nzPlacement === 'bottomLeft' || config.nzPlacement === 'bottomRight') ? bottom : null\"\n  [style.right]=\"(config.nzPlacement === 'bottomRight' || config.nzPlacement === 'topRight') ? '0px' : null\"\n  [style.left]=\"(config.nzPlacement === 'topLeft' || config.nzPlacement === 'bottomLeft') ? '0px' : null\">\n  <nz-notification\n    *ngFor=\"let message of messages; let i = index\"\n    [nzMessage]=\"message\"\n    [nzIndex]=\"i\">\n  </nz-notification>\n</div>"
            }] }
];
/** @nocollapse */
NzNotificationContainerComponent.ctorParameters = () => [
    { type: ChangeDetectorRef },
    { type: undefined, decorators: [{ type: Optional }, { type: Inject, args: [NZ_NOTIFICATION_DEFAULT_CONFIG,] }] },
    { type: undefined, decorators: [{ type: Optional }, { type: Inject, args: [NZ_NOTIFICATION_CONFIG,] }] }
];
if (false) {
    /** @type {?} */
    NzNotificationContainerComponent.prototype.config;
    /** @type {?} */
    NzNotificationContainerComponent.prototype.bottom;
    /**
     * @override
     * @type {?}
     */
    NzNotificationContainerComponent.prototype.messages;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotbm90aWZpY2F0aW9uLWNvbnRhaW5lci5jb21wb25lbnQuanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsibm90aWZpY2F0aW9uL256LW5vdGlmaWNhdGlvbi1jb250YWluZXIuY29tcG9uZW50LnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7QUFBQSxPQUFPLEVBQ0wsdUJBQXVCLEVBQ3ZCLGlCQUFpQixFQUNqQixTQUFTLEVBQ1QsTUFBTSxFQUNOLFFBQVEsRUFDUixpQkFBaUIsRUFDbEIsTUFBTSxlQUFlLENBQUM7QUFDdkIsT0FBTyxFQUFFLE9BQU8sRUFBRSxNQUFNLE1BQU0sQ0FBQztBQUUvQixPQUFPLEVBQUUsVUFBVSxFQUFFLE1BQU0sY0FBYyxDQUFDO0FBQzFDLE9BQU8sRUFBRSwyQkFBMkIsRUFBRSxNQUFNLDJDQUEyQyxDQUFDO0FBRXhGLE9BQU8sRUFBd0Isc0JBQXNCLEVBQUUsOEJBQThCLEVBQUUsTUFBTSwwQkFBMEIsQ0FBQztBQVV4SCxNQUFNLE9BQU8sZ0NBQWlDLFNBQVEsMkJBQTJCOzs7Ozs7SUFTL0UsWUFDRSxHQUFzQixFQUM4QixhQUFtQyxFQUMzQyxNQUE0QjtRQUV4RSxLQUFLLENBQUMsR0FBRyxFQUFFLGFBQWEsRUFBRSxNQUFNLENBQUMsQ0FBQzs7OztRQVBwQyxhQUFRLEdBQThDLEVBQUUsQ0FBQztJQVF6RCxDQUFDOzs7Ozs7SUFLRCxTQUFTLENBQUMsTUFBNEI7O2NBQzlCLFNBQVMsR0FBRyxDQUFDLElBQUksQ0FBQyxNQUFNLHFCQUFRLElBQUksQ0FBQyxNQUFNLEVBQUssTUFBTSxDQUFFLENBQUM7O2NBQ3pELFNBQVMsR0FBRyxJQUFJLENBQUMsTUFBTSxDQUFDLFdBQVc7UUFFekMsSUFBSSxDQUFDLEdBQUcsR0FBRyxTQUFTLEtBQUssU0FBUyxJQUFJLFNBQVMsS0FBSyxVQUFVLENBQUMsQ0FBQyxDQUFDLFVBQVUsQ0FBQyxTQUFTLENBQUMsS0FBSyxDQUFDLENBQUMsQ0FBQyxDQUFDLElBQUksQ0FBQztRQUNwRyxJQUFJLENBQUMsTUFBTSxHQUFHLFNBQVMsS0FBSyxZQUFZLElBQUksU0FBUyxLQUFLLGFBQWEsQ0FBQyxDQUFDLENBQUMsVUFBVSxDQUFDLFNBQVMsQ0FBQyxRQUFRLENBQUMsQ0FBQyxDQUFDLENBQUMsSUFBSSxDQUFDO1FBRWhILElBQUksQ0FBQyxHQUFHLENBQUMsWUFBWSxFQUFFLENBQUM7SUFDMUIsQ0FBQzs7Ozs7Ozs7O0lBU0QsYUFBYSxDQUFDLFlBQXNDO1FBQ2xELFlBQVksQ0FBQyxPQUFPLEdBQUcsSUFBSSxDQUFDLG9CQUFvQixDQUFDLFlBQVksQ0FBQyxPQUFPLENBQUMsQ0FBQztRQUN2RSxZQUFZLENBQUMsT0FBTyxHQUFHLElBQUksT0FBTyxFQUFXLENBQUM7O2NBQ3hDLEdBQUcsR0FBRyxZQUFZLENBQUMsT0FBTyxDQUFDLEtBQUs7O2NBQ2hDLHVCQUF1QixHQUFHLElBQUksQ0FBQyxRQUFRLENBQUMsSUFBSTs7OztRQUNoRCxHQUFHLENBQUMsRUFBRSxDQUFDLEdBQUcsQ0FBQyxPQUFPLENBQUMsS0FBSyxLQUFLLENBQUMsbUJBQUEsWUFBWSxDQUFDLE9BQU8sRUFBdUMsQ0FBQyxDQUFDLEtBQUssRUFDakc7UUFFRCxJQUFJLEdBQUcsSUFBSSx1QkFBdUIsRUFBRTtZQUNsQyxJQUFJLENBQUMsbUJBQW1CLENBQUMsdUJBQXVCLEVBQUUsWUFBWSxDQUFDLENBQUM7U0FDakU7YUFBTTtZQUNMLElBQUksSUFBSSxDQUFDLFFBQVEsQ0FBQyxNQUFNLElBQUksSUFBSSxDQUFDLE1BQU0sQ0FBQyxVQUFVLEVBQUU7Z0JBQ2xELElBQUksQ0FBQyxRQUFRLENBQUMsTUFBTSxDQUFDLENBQUMsRUFBRSxDQUFDLENBQUMsQ0FBQzthQUM1QjtZQUNELElBQUksQ0FBQyxRQUFRLENBQUMsSUFBSSxDQUFDLG1CQUFBLFlBQVksRUFBc0MsQ0FBQyxDQUFDO1NBQ3hFO1FBQ0QsSUFBSSxDQUFDLEdBQUcsQ0FBQyxhQUFhLEVBQUUsQ0FBQztJQUMzQixDQUFDOzs7Ozs7O0lBRU8sbUJBQW1CLENBQUMsR0FBNkIsRUFBRSxJQUE4QjtRQUN2RixHQUFHLENBQUMsS0FBSyxHQUFHLElBQUksQ0FBQyxLQUFLLENBQUM7UUFDdkIsR0FBRyxDQUFDLE9BQU8sR0FBRyxJQUFJLENBQUMsT0FBTyxDQUFDO1FBQzNCLEdBQUcsQ0FBQyxRQUFRLEdBQUcsSUFBSSxDQUFDLFFBQVEsQ0FBQztRQUM3QixHQUFHLENBQUMsSUFBSSxHQUFHLElBQUksQ0FBQyxJQUFJLENBQUM7SUFDdkIsQ0FBQzs7O1lBcEVGLFNBQVMsU0FBQztnQkFDVCxlQUFlLEVBQUUsdUJBQXVCLENBQUMsTUFBTTtnQkFDL0MsYUFBYSxFQUFFLGlCQUFpQixDQUFDLElBQUk7Z0JBQ3JDLFFBQVEsRUFBRSwyQkFBMkI7Z0JBQ3JDLG1CQUFtQixFQUFFLEtBQUs7Z0JBQzFCLDRxQkFBeUQ7YUFDMUQ7Ozs7WUFwQkMsaUJBQWlCOzRDQWdDZCxRQUFRLFlBQUksTUFBTSxTQUFDLDhCQUE4Qjs0Q0FDakQsUUFBUSxZQUFJLE1BQU0sU0FBQyxzQkFBc0I7Ozs7SUFYNUMsa0RBQXVDOztJQUN2QyxrREFBc0I7Ozs7O0lBS3RCLG9EQUF5RCIsInNvdXJjZXNDb250ZW50IjpbImltcG9ydCB7XG4gIENoYW5nZURldGVjdGlvblN0cmF0ZWd5LFxuICBDaGFuZ2VEZXRlY3RvclJlZixcbiAgQ29tcG9uZW50LFxuICBJbmplY3QsXG4gIE9wdGlvbmFsLFxuICBWaWV3RW5jYXBzdWxhdGlvblxufSBmcm9tICdAYW5ndWxhci9jb3JlJztcbmltcG9ydCB7IFN1YmplY3QgfSBmcm9tICdyeGpzJztcblxuaW1wb3J0IHsgdG9Dc3NQaXhlbCB9IGZyb20gJy4uL2NvcmUvdXRpbCc7XG5pbXBvcnQgeyBOek1lc3NhZ2VDb250YWluZXJDb21wb25lbnQgfSBmcm9tICcuLi9tZXNzYWdlL256LW1lc3NhZ2UtY29udGFpbmVyLmNvbXBvbmVudCc7XG5cbmltcG9ydCB7IE56Tm90aWZpY2F0aW9uQ29uZmlnLCBOWl9OT1RJRklDQVRJT05fQ09ORklHLCBOWl9OT1RJRklDQVRJT05fREVGQVVMVF9DT05GSUcgfSBmcm9tICcuL256LW5vdGlmaWNhdGlvbi1jb25maWcnO1xuaW1wb3J0IHsgTnpOb3RpZmljYXRpb25EYXRhRmlsbGVkLCBOek5vdGlmaWNhdGlvbkRhdGFPcHRpb25zIH0gZnJvbSAnLi9uei1ub3RpZmljYXRpb24uZGVmaW5pdGlvbnMnO1xuXG5AQ29tcG9uZW50KHtcbiAgY2hhbmdlRGV0ZWN0aW9uOiBDaGFuZ2VEZXRlY3Rpb25TdHJhdGVneS5PblB1c2gsXG4gIGVuY2Fwc3VsYXRpb246IFZpZXdFbmNhcHN1bGF0aW9uLk5vbmUsXG4gIHNlbGVjdG9yOiAnbnotbm90aWZpY2F0aW9uLWNvbnRhaW5lcicsXG4gIHByZXNlcnZlV2hpdGVzcGFjZXM6IGZhbHNlLFxuICB0ZW1wbGF0ZVVybDogJy4vbnotbm90aWZpY2F0aW9uLWNvbnRhaW5lci5jb21wb25lbnQuaHRtbCdcbn0pXG5leHBvcnQgY2xhc3MgTnpOb3RpZmljYXRpb25Db250YWluZXJDb21wb25lbnQgZXh0ZW5kcyBOek1lc3NhZ2VDb250YWluZXJDb21wb25lbnQge1xuICBjb25maWc6IFJlcXVpcmVkPE56Tm90aWZpY2F0aW9uQ29uZmlnPjtcbiAgYm90dG9tOiBzdHJpbmcgfCBudWxsO1xuXG4gIC8qKlxuICAgKiBAb3ZlcnJpZGVcbiAgICovXG4gIG1lc3NhZ2VzOiBBcnJheTxSZXF1aXJlZDxOek5vdGlmaWNhdGlvbkRhdGFGaWxsZWQ+PiA9IFtdO1xuXG4gIGNvbnN0cnVjdG9yKFxuICAgIGNkcjogQ2hhbmdlRGV0ZWN0b3JSZWYsXG4gICAgQE9wdGlvbmFsKCkgQEluamVjdChOWl9OT1RJRklDQVRJT05fREVGQVVMVF9DT05GSUcpIGRlZmF1bHRDb25maWc6IE56Tm90aWZpY2F0aW9uQ29uZmlnLFxuICAgIEBPcHRpb25hbCgpIEBJbmplY3QoTlpfTk9USUZJQ0FUSU9OX0NPTkZJRykgY29uZmlnOiBOek5vdGlmaWNhdGlvbkNvbmZpZ1xuICApIHtcbiAgICBzdXBlcihjZHIsIGRlZmF1bHRDb25maWcsIGNvbmZpZyk7XG4gIH1cblxuICAvKipcbiAgICogQG92ZXJyaWRlXG4gICAqL1xuICBzZXRDb25maWcoY29uZmlnOiBOek5vdGlmaWNhdGlvbkNvbmZpZyk6IHZvaWQge1xuICAgIGNvbnN0IG5ld0NvbmZpZyA9ICh0aGlzLmNvbmZpZyA9IHsgLi4udGhpcy5jb25maWcsIC4uLmNvbmZpZyB9KTtcbiAgICBjb25zdCBwbGFjZW1lbnQgPSB0aGlzLmNvbmZpZy5uelBsYWNlbWVudDtcblxuICAgIHRoaXMudG9wID0gcGxhY2VtZW50ID09PSAndG9wTGVmdCcgfHwgcGxhY2VtZW50ID09PSAndG9wUmlnaHQnID8gdG9Dc3NQaXhlbChuZXdDb25maWcubnpUb3ApIDogbnVsbDtcbiAgICB0aGlzLmJvdHRvbSA9IHBsYWNlbWVudCA9PT0gJ2JvdHRvbUxlZnQnIHx8IHBsYWNlbWVudCA9PT0gJ2JvdHRvbVJpZ2h0JyA/IHRvQ3NzUGl4ZWwobmV3Q29uZmlnLm56Qm90dG9tKSA6IG51bGw7XG5cbiAgICB0aGlzLmNkci5tYXJrRm9yQ2hlY2soKTtcbiAgfVxuXG4gIC8qKlxuICAgKiBDcmVhdGUgYSBuZXcgbm90aWZpY2F0aW9uLlxuICAgKiBJZiB0aGVyZSdzIGEgbm90aWZpY2F0aW9uIHdob3NlIGBuektleWAgaXMgc2FtZSB3aXRoIGBuektleWAgaW4gYE56Tm90aWZpY2F0aW9uRGF0YUZpbGxlZGAsXG4gICAqIHJlcGxhY2UgaXRzIGNvbnRlbnQgaW5zdGVhZCBvZiBjcmVhdGUgYSBuZXcgb25lLlxuICAgKiBAb3ZlcnJpZGVcbiAgICogQHBhcmFtIG5vdGlmaWNhdGlvblxuICAgKi9cbiAgY3JlYXRlTWVzc2FnZShub3RpZmljYXRpb246IE56Tm90aWZpY2F0aW9uRGF0YUZpbGxlZCk6IHZvaWQge1xuICAgIG5vdGlmaWNhdGlvbi5vcHRpb25zID0gdGhpcy5fbWVyZ2VNZXNzYWdlT3B0aW9ucyhub3RpZmljYXRpb24ub3B0aW9ucyk7XG4gICAgbm90aWZpY2F0aW9uLm9uQ2xvc2UgPSBuZXcgU3ViamVjdDxib29sZWFuPigpO1xuICAgIGNvbnN0IGtleSA9IG5vdGlmaWNhdGlvbi5vcHRpb25zLm56S2V5O1xuICAgIGNvbnN0IG5vdGlmaWNhdGlvbldpdGhTYW1lS2V5ID0gdGhpcy5tZXNzYWdlcy5maW5kKFxuICAgICAgbXNnID0+IG1zZy5vcHRpb25zLm56S2V5ID09PSAobm90aWZpY2F0aW9uLm9wdGlvbnMgYXMgUmVxdWlyZWQ8TnpOb3RpZmljYXRpb25EYXRhT3B0aW9ucz4pLm56S2V5XG4gICAgKTtcblxuICAgIGlmIChrZXkgJiYgbm90aWZpY2F0aW9uV2l0aFNhbWVLZXkpIHtcbiAgICAgIHRoaXMucmVwbGFjZU5vdGlmaWNhdGlvbihub3RpZmljYXRpb25XaXRoU2FtZUtleSwgbm90aWZpY2F0aW9uKTtcbiAgICB9IGVsc2Uge1xuICAgICAgaWYgKHRoaXMubWVzc2FnZXMubGVuZ3RoID49IHRoaXMuY29uZmlnLm56TWF4U3RhY2spIHtcbiAgICAgICAgdGhpcy5tZXNzYWdlcy5zcGxpY2UoMCwgMSk7XG4gICAgICB9XG4gICAgICB0aGlzLm1lc3NhZ2VzLnB1c2gobm90aWZpY2F0aW9uIGFzIFJlcXVpcmVkPE56Tm90aWZpY2F0aW9uRGF0YUZpbGxlZD4pO1xuICAgIH1cbiAgICB0aGlzLmNkci5kZXRlY3RDaGFuZ2VzKCk7XG4gIH1cblxuICBwcml2YXRlIHJlcGxhY2VOb3RpZmljYXRpb24ob2xkOiBOek5vdGlmaWNhdGlvbkRhdGFGaWxsZWQsIF9uZXc6IE56Tm90aWZpY2F0aW9uRGF0YUZpbGxlZCk6IHZvaWQge1xuICAgIG9sZC50aXRsZSA9IF9uZXcudGl0bGU7XG4gICAgb2xkLmNvbnRlbnQgPSBfbmV3LmNvbnRlbnQ7XG4gICAgb2xkLnRlbXBsYXRlID0gX25ldy50ZW1wbGF0ZTtcbiAgICBvbGQudHlwZSA9IF9uZXcudHlwZTtcbiAgfVxufVxuIl19