/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { Overlay } from '@angular/cdk/overlay';
import { ApplicationRef, ComponentFactoryResolver, Injectable, Injector } from '@angular/core';
import { NzMessageBaseService } from '../message/nz-message.service';
import { NzNotificationContainerComponent } from './nz-notification-container.component';
import * as i0 from "@angular/core";
import * as i1 from "./public-api";
import * as i2 from "@angular/cdk/overlay";
export class NzNotificationService extends NzMessageBaseService {
    /**
     * @param {?} overlay
     * @param {?} injector
     * @param {?} cfr
     * @param {?} appRef
     */
    constructor(overlay, injector, cfr, appRef) {
        super(overlay, NzNotificationContainerComponent, injector, cfr, appRef, 'notification-');
    }
    // Shortcut methods
    /**
     * @param {?} title
     * @param {?} content
     * @param {?=} options
     * @return {?}
     */
    success(title, content, options) {
        return (/** @type {?} */ (this.createMessage({ type: 'success', title, content }, options)));
    }
    /**
     * @param {?} title
     * @param {?} content
     * @param {?=} options
     * @return {?}
     */
    error(title, content, options) {
        return (/** @type {?} */ (this.createMessage({ type: 'error', title, content }, options)));
    }
    /**
     * @param {?} title
     * @param {?} content
     * @param {?=} options
     * @return {?}
     */
    info(title, content, options) {
        return (/** @type {?} */ (this.createMessage({ type: 'info', title, content }, options)));
    }
    /**
     * @param {?} title
     * @param {?} content
     * @param {?=} options
     * @return {?}
     */
    warning(title, content, options) {
        return (/** @type {?} */ (this.createMessage({ type: 'warning', title, content }, options)));
    }
    /**
     * @param {?} title
     * @param {?} content
     * @param {?=} options
     * @return {?}
     */
    blank(title, content, options) {
        return (/** @type {?} */ (this.createMessage({ type: 'blank', title, content }, options)));
    }
    /**
     * @param {?} type
     * @param {?} title
     * @param {?} content
     * @param {?=} options
     * @return {?}
     */
    create(type, title, content, options) {
        return (/** @type {?} */ (this.createMessage({ type, title, content }, options)));
    }
    // For content with template
    /**
     * @param {?} template
     * @param {?=} options
     * @return {?}
     */
    template(template, options) {
        return (/** @type {?} */ (this.createMessage({ template }, options)));
    }
}
NzNotificationService.decorators = [
    { type: Injectable, args: [{
                providedIn: 'root'
            },] }
];
/** @nocollapse */
NzNotificationService.ctorParameters = () => [
    { type: Overlay },
    { type: Injector },
    { type: ComponentFactoryResolver },
    { type: ApplicationRef }
];
/** @nocollapse */ NzNotificationService.ngInjectableDef = i0.defineInjectable({ factory: function NzNotificationService_Factory() { return new i1.NzNotificationService(i0.inject(i2.Overlay), i0.inject(i0.INJECTOR), i0.inject(i0.ComponentFactoryResolver), i0.inject(i0.ApplicationRef)); }, token: i1.NzNotificationService, providedIn: "root" });
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotbm90aWZpY2F0aW9uLnNlcnZpY2UuanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsibm90aWZpY2F0aW9uL256LW5vdGlmaWNhdGlvbi5zZXJ2aWNlLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7QUFBQSxPQUFPLEVBQUUsT0FBTyxFQUFFLE1BQU0sc0JBQXNCLENBQUM7QUFDL0MsT0FBTyxFQUFFLGNBQWMsRUFBRSx3QkFBd0IsRUFBRSxVQUFVLEVBQUUsUUFBUSxFQUFlLE1BQU0sZUFBZSxDQUFDO0FBRTVHLE9BQU8sRUFBRSxvQkFBb0IsRUFBRSxNQUFNLCtCQUErQixDQUFDO0FBR3JFLE9BQU8sRUFBRSxnQ0FBZ0MsRUFBRSxNQUFNLHVDQUF1QyxDQUFDOzs7O0FBTXpGLE1BQU0sT0FBTyxxQkFBc0IsU0FBUSxvQkFJMUM7Ozs7Ozs7SUFDQyxZQUFZLE9BQWdCLEVBQUUsUUFBa0IsRUFBRSxHQUE2QixFQUFFLE1BQXNCO1FBQ3JHLEtBQUssQ0FBQyxPQUFPLEVBQUUsZ0NBQWdDLEVBQUUsUUFBUSxFQUFFLEdBQUcsRUFBRSxNQUFNLEVBQUUsZUFBZSxDQUFDLENBQUM7SUFDM0YsQ0FBQzs7Ozs7Ozs7SUFHRCxPQUFPLENBQUMsS0FBYSxFQUFFLE9BQWUsRUFBRSxPQUFtQztRQUN6RSxPQUFPLG1CQUFBLElBQUksQ0FBQyxhQUFhLENBQUMsRUFBRSxJQUFJLEVBQUUsU0FBUyxFQUFFLEtBQUssRUFBRSxPQUFPLEVBQUUsRUFBRSxPQUFPLENBQUMsRUFBNEIsQ0FBQztJQUN0RyxDQUFDOzs7Ozs7O0lBRUQsS0FBSyxDQUFDLEtBQWEsRUFBRSxPQUFlLEVBQUUsT0FBbUM7UUFDdkUsT0FBTyxtQkFBQSxJQUFJLENBQUMsYUFBYSxDQUFDLEVBQUUsSUFBSSxFQUFFLE9BQU8sRUFBRSxLQUFLLEVBQUUsT0FBTyxFQUFFLEVBQUUsT0FBTyxDQUFDLEVBQTRCLENBQUM7SUFDcEcsQ0FBQzs7Ozs7OztJQUVELElBQUksQ0FBQyxLQUFhLEVBQUUsT0FBZSxFQUFFLE9BQW1DO1FBQ3RFLE9BQU8sbUJBQUEsSUFBSSxDQUFDLGFBQWEsQ0FBQyxFQUFFLElBQUksRUFBRSxNQUFNLEVBQUUsS0FBSyxFQUFFLE9BQU8sRUFBRSxFQUFFLE9BQU8sQ0FBQyxFQUE0QixDQUFDO0lBQ25HLENBQUM7Ozs7Ozs7SUFFRCxPQUFPLENBQUMsS0FBYSxFQUFFLE9BQWUsRUFBRSxPQUFtQztRQUN6RSxPQUFPLG1CQUFBLElBQUksQ0FBQyxhQUFhLENBQUMsRUFBRSxJQUFJLEVBQUUsU0FBUyxFQUFFLEtBQUssRUFBRSxPQUFPLEVBQUUsRUFBRSxPQUFPLENBQUMsRUFBNEIsQ0FBQztJQUN0RyxDQUFDOzs7Ozs7O0lBRUQsS0FBSyxDQUFDLEtBQWEsRUFBRSxPQUFlLEVBQUUsT0FBbUM7UUFDdkUsT0FBTyxtQkFBQSxJQUFJLENBQUMsYUFBYSxDQUFDLEVBQUUsSUFBSSxFQUFFLE9BQU8sRUFBRSxLQUFLLEVBQUUsT0FBTyxFQUFFLEVBQUUsT0FBTyxDQUFDLEVBQTRCLENBQUM7SUFDcEcsQ0FBQzs7Ozs7Ozs7SUFFRCxNQUFNLENBQ0osSUFBaUUsRUFDakUsS0FBYSxFQUNiLE9BQWUsRUFDZixPQUFtQztRQUVuQyxPQUFPLG1CQUFBLElBQUksQ0FBQyxhQUFhLENBQUMsRUFBRSxJQUFJLEVBQUUsS0FBSyxFQUFFLE9BQU8sRUFBRSxFQUFFLE9BQU8sQ0FBQyxFQUE0QixDQUFDO0lBQzNGLENBQUM7Ozs7Ozs7SUFHRCxRQUFRLENBQUMsUUFBeUIsRUFBRSxPQUFtQztRQUNyRSxPQUFPLG1CQUFBLElBQUksQ0FBQyxhQUFhLENBQUMsRUFBRSxRQUFRLEVBQUUsRUFBRSxPQUFPLENBQUMsRUFBNEIsQ0FBQztJQUMvRSxDQUFDOzs7WUE3Q0YsVUFBVSxTQUFDO2dCQUNWLFVBQVUsRUFBRSxNQUFNO2FBQ25COzs7O1lBWFEsT0FBTztZQUMrQyxRQUFRO1lBQTlDLHdCQUF3QjtZQUF4QyxjQUFjIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHsgT3ZlcmxheSB9IGZyb20gJ0Bhbmd1bGFyL2Nkay9vdmVybGF5JztcbmltcG9ydCB7IEFwcGxpY2F0aW9uUmVmLCBDb21wb25lbnRGYWN0b3J5UmVzb2x2ZXIsIEluamVjdGFibGUsIEluamVjdG9yLCBUZW1wbGF0ZVJlZiB9IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuXG5pbXBvcnQgeyBOek1lc3NhZ2VCYXNlU2VydmljZSB9IGZyb20gJy4uL21lc3NhZ2UvbnotbWVzc2FnZS5zZXJ2aWNlJztcblxuaW1wb3J0IHsgTnpOb3RpZmljYXRpb25Db25maWcgfSBmcm9tICcuL256LW5vdGlmaWNhdGlvbi1jb25maWcnO1xuaW1wb3J0IHsgTnpOb3RpZmljYXRpb25Db250YWluZXJDb21wb25lbnQgfSBmcm9tICcuL256LW5vdGlmaWNhdGlvbi1jb250YWluZXIuY29tcG9uZW50JztcbmltcG9ydCB7IE56Tm90aWZpY2F0aW9uRGF0YSwgTnpOb3RpZmljYXRpb25EYXRhRmlsbGVkLCBOek5vdGlmaWNhdGlvbkRhdGFPcHRpb25zIH0gZnJvbSAnLi9uei1ub3RpZmljYXRpb24uZGVmaW5pdGlvbnMnO1xuXG5ASW5qZWN0YWJsZSh7XG4gIHByb3ZpZGVkSW46ICdyb290J1xufSlcbmV4cG9ydCBjbGFzcyBOek5vdGlmaWNhdGlvblNlcnZpY2UgZXh0ZW5kcyBOek1lc3NhZ2VCYXNlU2VydmljZTxcbiAgTnpOb3RpZmljYXRpb25Db250YWluZXJDb21wb25lbnQsXG4gIE56Tm90aWZpY2F0aW9uRGF0YSxcbiAgTnpOb3RpZmljYXRpb25Db25maWdcbj4ge1xuICBjb25zdHJ1Y3RvcihvdmVybGF5OiBPdmVybGF5LCBpbmplY3RvcjogSW5qZWN0b3IsIGNmcjogQ29tcG9uZW50RmFjdG9yeVJlc29sdmVyLCBhcHBSZWY6IEFwcGxpY2F0aW9uUmVmKSB7XG4gICAgc3VwZXIob3ZlcmxheSwgTnpOb3RpZmljYXRpb25Db250YWluZXJDb21wb25lbnQsIGluamVjdG9yLCBjZnIsIGFwcFJlZiwgJ25vdGlmaWNhdGlvbi0nKTtcbiAgfVxuXG4gIC8vIFNob3J0Y3V0IG1ldGhvZHNcbiAgc3VjY2Vzcyh0aXRsZTogc3RyaW5nLCBjb250ZW50OiBzdHJpbmcsIG9wdGlvbnM/OiBOek5vdGlmaWNhdGlvbkRhdGFPcHRpb25zKTogTnpOb3RpZmljYXRpb25EYXRhRmlsbGVkIHtcbiAgICByZXR1cm4gdGhpcy5jcmVhdGVNZXNzYWdlKHsgdHlwZTogJ3N1Y2Nlc3MnLCB0aXRsZSwgY29udGVudCB9LCBvcHRpb25zKSBhcyBOek5vdGlmaWNhdGlvbkRhdGFGaWxsZWQ7XG4gIH1cblxuICBlcnJvcih0aXRsZTogc3RyaW5nLCBjb250ZW50OiBzdHJpbmcsIG9wdGlvbnM/OiBOek5vdGlmaWNhdGlvbkRhdGFPcHRpb25zKTogTnpOb3RpZmljYXRpb25EYXRhRmlsbGVkIHtcbiAgICByZXR1cm4gdGhpcy5jcmVhdGVNZXNzYWdlKHsgdHlwZTogJ2Vycm9yJywgdGl0bGUsIGNvbnRlbnQgfSwgb3B0aW9ucykgYXMgTnpOb3RpZmljYXRpb25EYXRhRmlsbGVkO1xuICB9XG5cbiAgaW5mbyh0aXRsZTogc3RyaW5nLCBjb250ZW50OiBzdHJpbmcsIG9wdGlvbnM/OiBOek5vdGlmaWNhdGlvbkRhdGFPcHRpb25zKTogTnpOb3RpZmljYXRpb25EYXRhRmlsbGVkIHtcbiAgICByZXR1cm4gdGhpcy5jcmVhdGVNZXNzYWdlKHsgdHlwZTogJ2luZm8nLCB0aXRsZSwgY29udGVudCB9LCBvcHRpb25zKSBhcyBOek5vdGlmaWNhdGlvbkRhdGFGaWxsZWQ7XG4gIH1cblxuICB3YXJuaW5nKHRpdGxlOiBzdHJpbmcsIGNvbnRlbnQ6IHN0cmluZywgb3B0aW9ucz86IE56Tm90aWZpY2F0aW9uRGF0YU9wdGlvbnMpOiBOek5vdGlmaWNhdGlvbkRhdGFGaWxsZWQge1xuICAgIHJldHVybiB0aGlzLmNyZWF0ZU1lc3NhZ2UoeyB0eXBlOiAnd2FybmluZycsIHRpdGxlLCBjb250ZW50IH0sIG9wdGlvbnMpIGFzIE56Tm90aWZpY2F0aW9uRGF0YUZpbGxlZDtcbiAgfVxuXG4gIGJsYW5rKHRpdGxlOiBzdHJpbmcsIGNvbnRlbnQ6IHN0cmluZywgb3B0aW9ucz86IE56Tm90aWZpY2F0aW9uRGF0YU9wdGlvbnMpOiBOek5vdGlmaWNhdGlvbkRhdGFGaWxsZWQge1xuICAgIHJldHVybiB0aGlzLmNyZWF0ZU1lc3NhZ2UoeyB0eXBlOiAnYmxhbmsnLCB0aXRsZSwgY29udGVudCB9LCBvcHRpb25zKSBhcyBOek5vdGlmaWNhdGlvbkRhdGFGaWxsZWQ7XG4gIH1cblxuICBjcmVhdGUoXG4gICAgdHlwZTogJ3N1Y2Nlc3MnIHwgJ2luZm8nIHwgJ3dhcm5pbmcnIHwgJ2Vycm9yJyB8ICdibGFuaycgfCBzdHJpbmcsXG4gICAgdGl0bGU6IHN0cmluZyxcbiAgICBjb250ZW50OiBzdHJpbmcsXG4gICAgb3B0aW9ucz86IE56Tm90aWZpY2F0aW9uRGF0YU9wdGlvbnNcbiAgKTogTnpOb3RpZmljYXRpb25EYXRhRmlsbGVkIHtcbiAgICByZXR1cm4gdGhpcy5jcmVhdGVNZXNzYWdlKHsgdHlwZSwgdGl0bGUsIGNvbnRlbnQgfSwgb3B0aW9ucykgYXMgTnpOb3RpZmljYXRpb25EYXRhRmlsbGVkO1xuICB9XG5cbiAgLy8gRm9yIGNvbnRlbnQgd2l0aCB0ZW1wbGF0ZVxuICB0ZW1wbGF0ZSh0ZW1wbGF0ZTogVGVtcGxhdGVSZWY8e30+LCBvcHRpb25zPzogTnpOb3RpZmljYXRpb25EYXRhT3B0aW9ucyk6IE56Tm90aWZpY2F0aW9uRGF0YUZpbGxlZCB7XG4gICAgcmV0dXJuIHRoaXMuY3JlYXRlTWVzc2FnZSh7IHRlbXBsYXRlIH0sIG9wdGlvbnMpIGFzIE56Tm90aWZpY2F0aW9uRGF0YUZpbGxlZDtcbiAgfVxufVxuIl19