/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import * as tslib_1 from "tslib";
import { Overlay } from '@angular/cdk/overlay';
import { ApplicationRef, ComponentFactoryResolver, Injectable, Injector } from '@angular/core';
import { NzMessageContainerComponent } from './nz-message-container.component';
import * as i0 from "@angular/core";
import * as i1 from "./public-api";
import * as i2 from "@angular/cdk/overlay";
/** @type {?} */
var globalCounter = 0;
/**
 * @template ContainerClass, MessageData, MessageConfig
 */
var /**
 * @template ContainerClass, MessageData, MessageConfig
 */
NzMessageBaseService = /** @class */ (function () {
    function NzMessageBaseService(overlay, containerClass, injector, cfr, appRef, _idPrefix) {
        if (_idPrefix === void 0) { _idPrefix = ''; }
        this.overlay = overlay;
        this.containerClass = containerClass;
        this.injector = injector;
        this.cfr = cfr;
        this.appRef = appRef;
        this._idPrefix = _idPrefix;
        this._container = this.createContainer();
    }
    /**
     * @param {?=} messageId
     * @return {?}
     */
    NzMessageBaseService.prototype.remove = /**
     * @param {?=} messageId
     * @return {?}
     */
    function (messageId) {
        if (messageId) {
            this._container.removeMessage(messageId);
        }
        else {
            this._container.removeMessageAll();
        }
    };
    /**
     * @param {?} message
     * @param {?=} options
     * @return {?}
     */
    NzMessageBaseService.prototype.createMessage = /**
     * @param {?} message
     * @param {?=} options
     * @return {?}
     */
    function (message, options) {
        /** @type {?} */
        var resultMessage = tslib_1.__assign({}, ((/** @type {?} */ (message))), {
            createdAt: new Date(),
            messageId: this._generateMessageId(),
            options: options
        });
        this._container.createMessage(resultMessage);
        return resultMessage;
    };
    /**
     * @param {?} config
     * @return {?}
     */
    NzMessageBaseService.prototype.config = /**
     * @param {?} config
     * @return {?}
     */
    function (config) {
        this._container.setConfig(config);
    };
    /**
     * @protected
     * @return {?}
     */
    NzMessageBaseService.prototype._generateMessageId = /**
     * @protected
     * @return {?}
     */
    function () {
        return this._idPrefix + globalCounter++;
    };
    // Manually creating container for overlay to avoid multi-checking error, see: https://github.com/NG-ZORRO/ng-zorro-antd/issues/391
    // NOTE: we never clean up the container component and it's overlay resources, if we should, we need to do it by our own codes.
    // Manually creating container for overlay to avoid multi-checking error, see: https://github.com/NG-ZORRO/ng-zorro-antd/issues/391
    // NOTE: we never clean up the container component and it's overlay resources, if we should, we need to do it by our own codes.
    /**
     * @private
     * @return {?}
     */
    NzMessageBaseService.prototype.createContainer = 
    // Manually creating container for overlay to avoid multi-checking error, see: https://github.com/NG-ZORRO/ng-zorro-antd/issues/391
    // NOTE: we never clean up the container component and it's overlay resources, if we should, we need to do it by our own codes.
    /**
     * @private
     * @return {?}
     */
    function () {
        /** @type {?} */
        var factory = this.cfr.resolveComponentFactory(this.containerClass);
        /** @type {?} */
        var componentRef = factory.create(this.injector);
        componentRef.changeDetectorRef.detectChanges(); // Immediately change detection to avoid multi-checking error
        this.appRef.attachView(componentRef.hostView); // Load view into app root
        // Load view into app root
        /** @type {?} */
        var overlayPane = this.overlay.create().overlayElement;
        overlayPane.style.zIndex = '1010'; // Patching: assign the same zIndex of ant-message to it's parent overlay panel, to the ant-message's zindex work.
        overlayPane.appendChild((/** @type {?} */ (((/** @type {?} */ (componentRef.hostView))).rootNodes[0])));
        return componentRef.instance;
    };
    return NzMessageBaseService;
}());
/**
 * @template ContainerClass, MessageData, MessageConfig
 */
export { NzMessageBaseService };
if (false) {
    /**
     * @type {?}
     * @protected
     */
    NzMessageBaseService.prototype._container;
    /**
     * @type {?}
     * @private
     */
    NzMessageBaseService.prototype.overlay;
    /**
     * @type {?}
     * @private
     */
    NzMessageBaseService.prototype.containerClass;
    /**
     * @type {?}
     * @private
     */
    NzMessageBaseService.prototype.injector;
    /**
     * @type {?}
     * @private
     */
    NzMessageBaseService.prototype.cfr;
    /**
     * @type {?}
     * @private
     */
    NzMessageBaseService.prototype.appRef;
    /**
     * @type {?}
     * @private
     */
    NzMessageBaseService.prototype._idPrefix;
}
var NzMessageService = /** @class */ (function (_super) {
    tslib_1.__extends(NzMessageService, _super);
    function NzMessageService(overlay, injector, cfr, appRef) {
        return _super.call(this, overlay, NzMessageContainerComponent, injector, cfr, appRef, 'message-') || this;
    }
    // Shortcut methods
    // Shortcut methods
    /**
     * @param {?} content
     * @param {?=} options
     * @return {?}
     */
    NzMessageService.prototype.success = 
    // Shortcut methods
    /**
     * @param {?} content
     * @param {?=} options
     * @return {?}
     */
    function (content, options) {
        return this.createMessage({ type: 'success', content: content }, options);
    };
    /**
     * @param {?} content
     * @param {?=} options
     * @return {?}
     */
    NzMessageService.prototype.error = /**
     * @param {?} content
     * @param {?=} options
     * @return {?}
     */
    function (content, options) {
        return this.createMessage({ type: 'error', content: content }, options);
    };
    /**
     * @param {?} content
     * @param {?=} options
     * @return {?}
     */
    NzMessageService.prototype.info = /**
     * @param {?} content
     * @param {?=} options
     * @return {?}
     */
    function (content, options) {
        return this.createMessage({ type: 'info', content: content }, options);
    };
    /**
     * @param {?} content
     * @param {?=} options
     * @return {?}
     */
    NzMessageService.prototype.warning = /**
     * @param {?} content
     * @param {?=} options
     * @return {?}
     */
    function (content, options) {
        return this.createMessage({ type: 'warning', content: content }, options);
    };
    /**
     * @param {?} content
     * @param {?=} options
     * @return {?}
     */
    NzMessageService.prototype.loading = /**
     * @param {?} content
     * @param {?=} options
     * @return {?}
     */
    function (content, options) {
        return this.createMessage({ type: 'loading', content: content }, options);
    };
    /**
     * @param {?} type
     * @param {?} content
     * @param {?=} options
     * @return {?}
     */
    NzMessageService.prototype.create = /**
     * @param {?} type
     * @param {?} content
     * @param {?=} options
     * @return {?}
     */
    function (type, content, options) {
        return this.createMessage({ type: type, content: content }, options);
    };
    NzMessageService.decorators = [
        { type: Injectable, args: [{
                    providedIn: 'root'
                },] }
    ];
    /** @nocollapse */
    NzMessageService.ctorParameters = function () { return [
        { type: Overlay },
        { type: Injector },
        { type: ComponentFactoryResolver },
        { type: ApplicationRef }
    ]; };
    /** @nocollapse */ NzMessageService.ngInjectableDef = i0.defineInjectable({ factory: function NzMessageService_Factory() { return new i1.NzMessageService(i0.inject(i2.Overlay), i0.inject(i0.INJECTOR), i0.inject(i0.ComponentFactoryResolver), i0.inject(i0.ApplicationRef)); }, token: i1.NzMessageService, providedIn: "root" });
    return NzMessageService;
}(NzMessageBaseService));
export { NzMessageService };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotbWVzc2FnZS5zZXJ2aWNlLmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbIm1lc3NhZ2UvbnotbWVzc2FnZS5zZXJ2aWNlLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7O0FBQUEsT0FBTyxFQUFFLE9BQU8sRUFBRSxNQUFNLHNCQUFzQixDQUFDO0FBQy9DLE9BQU8sRUFDTCxjQUFjLEVBQ2Qsd0JBQXdCLEVBRXhCLFVBQVUsRUFDVixRQUFRLEVBR1QsTUFBTSxlQUFlLENBQUM7QUFHdkIsT0FBTyxFQUFFLDJCQUEyQixFQUFFLE1BQU0sa0NBQWtDLENBQUM7Ozs7O0lBRzNFLGFBQWEsR0FBRyxDQUFDOzs7O0FBRXJCOzs7O0lBT0UsOEJBQ1UsT0FBZ0IsRUFDaEIsY0FBb0MsRUFDcEMsUUFBa0IsRUFDbEIsR0FBNkIsRUFDN0IsTUFBc0IsRUFDdEIsU0FBc0I7UUFBdEIsMEJBQUEsRUFBQSxjQUFzQjtRQUx0QixZQUFPLEdBQVAsT0FBTyxDQUFTO1FBQ2hCLG1CQUFjLEdBQWQsY0FBYyxDQUFzQjtRQUNwQyxhQUFRLEdBQVIsUUFBUSxDQUFVO1FBQ2xCLFFBQUcsR0FBSCxHQUFHLENBQTBCO1FBQzdCLFdBQU0sR0FBTixNQUFNLENBQWdCO1FBQ3RCLGNBQVMsR0FBVCxTQUFTLENBQWE7UUFFOUIsSUFBSSxDQUFDLFVBQVUsR0FBRyxJQUFJLENBQUMsZUFBZSxFQUFFLENBQUM7SUFDM0MsQ0FBQzs7Ozs7SUFFRCxxQ0FBTTs7OztJQUFOLFVBQU8sU0FBa0I7UUFDdkIsSUFBSSxTQUFTLEVBQUU7WUFDYixJQUFJLENBQUMsVUFBVSxDQUFDLGFBQWEsQ0FBQyxTQUFTLENBQUMsQ0FBQztTQUMxQzthQUFNO1lBQ0wsSUFBSSxDQUFDLFVBQVUsQ0FBQyxnQkFBZ0IsRUFBRSxDQUFDO1NBQ3BDO0lBQ0gsQ0FBQzs7Ozs7O0lBRUQsNENBQWE7Ozs7O0lBQWIsVUFBYyxPQUFvQixFQUFFLE9BQThCOztZQUMxRCxhQUFhLHdCQUNkLENBQUMsbUJBQUEsT0FBTyxFQUFpQixDQUFDLEVBQzFCO1lBQ0QsU0FBUyxFQUFFLElBQUksSUFBSSxFQUFFO1lBQ3JCLFNBQVMsRUFBRSxJQUFJLENBQUMsa0JBQWtCLEVBQUU7WUFDcEMsT0FBTyxTQUFBO1NBQ1IsQ0FDRjtRQUNELElBQUksQ0FBQyxVQUFVLENBQUMsYUFBYSxDQUFDLGFBQWEsQ0FBQyxDQUFDO1FBRTdDLE9BQU8sYUFBYSxDQUFDO0lBQ3ZCLENBQUM7Ozs7O0lBRUQscUNBQU07Ozs7SUFBTixVQUFPLE1BQXFCO1FBQzFCLElBQUksQ0FBQyxVQUFVLENBQUMsU0FBUyxDQUFDLE1BQU0sQ0FBQyxDQUFDO0lBQ3BDLENBQUM7Ozs7O0lBRVMsaURBQWtCOzs7O0lBQTVCO1FBQ0UsT0FBTyxJQUFJLENBQUMsU0FBUyxHQUFHLGFBQWEsRUFBRSxDQUFDO0lBQzFDLENBQUM7SUFFRCxtSUFBbUk7SUFDbkksK0hBQStIOzs7Ozs7O0lBQ3ZILDhDQUFlOzs7Ozs7O0lBQXZCOztZQUNRLE9BQU8sR0FBRyxJQUFJLENBQUMsR0FBRyxDQUFDLHVCQUF1QixDQUFDLElBQUksQ0FBQyxjQUFjLENBQUM7O1lBQy9ELFlBQVksR0FBRyxPQUFPLENBQUMsTUFBTSxDQUFDLElBQUksQ0FBQyxRQUFRLENBQUM7UUFDbEQsWUFBWSxDQUFDLGlCQUFpQixDQUFDLGFBQWEsRUFBRSxDQUFDLENBQUMsNkRBQTZEO1FBQzdHLElBQUksQ0FBQyxNQUFNLENBQUMsVUFBVSxDQUFDLFlBQVksQ0FBQyxRQUFRLENBQUMsQ0FBQyxDQUFDLDBCQUEwQjs7O1lBQ25FLFdBQVcsR0FBRyxJQUFJLENBQUMsT0FBTyxDQUFDLE1BQU0sRUFBRSxDQUFDLGNBQWM7UUFDeEQsV0FBVyxDQUFDLEtBQUssQ0FBQyxNQUFNLEdBQUcsTUFBTSxDQUFDLENBQUMsa0hBQWtIO1FBQ3JKLFdBQVcsQ0FBQyxXQUFXLENBQUMsbUJBQUEsQ0FBQyxtQkFBQSxZQUFZLENBQUMsUUFBUSxFQUF1QixDQUFDLENBQUMsU0FBUyxDQUFDLENBQUMsQ0FBQyxFQUFlLENBQUMsQ0FBQztRQUVwRyxPQUFPLFlBQVksQ0FBQyxRQUFRLENBQUM7SUFDL0IsQ0FBQztJQUNILDJCQUFDO0FBQUQsQ0FBQyxBQTdERCxJQTZEQzs7Ozs7Ozs7OztJQXhEQywwQ0FBcUM7Ozs7O0lBR25DLHVDQUF3Qjs7Ozs7SUFDeEIsOENBQTRDOzs7OztJQUM1Qyx3Q0FBMEI7Ozs7O0lBQzFCLG1DQUFxQzs7Ozs7SUFDckMsc0NBQThCOzs7OztJQUM5Qix5Q0FBOEI7O0FBa0RsQztJQUdzQyw0Q0FJckM7SUFDQywwQkFBWSxPQUFnQixFQUFFLFFBQWtCLEVBQUUsR0FBNkIsRUFBRSxNQUFzQjtlQUNyRyxrQkFBTSxPQUFPLEVBQUUsMkJBQTJCLEVBQUUsUUFBUSxFQUFFLEdBQUcsRUFBRSxNQUFNLEVBQUUsVUFBVSxDQUFDO0lBQ2hGLENBQUM7SUFFRCxtQkFBbUI7Ozs7Ozs7SUFDbkIsa0NBQU87Ozs7Ozs7SUFBUCxVQUFRLE9BQW1DLEVBQUUsT0FBOEI7UUFDekUsT0FBTyxJQUFJLENBQUMsYUFBYSxDQUFDLEVBQUUsSUFBSSxFQUFFLFNBQVMsRUFBRSxPQUFPLFNBQUEsRUFBRSxFQUFFLE9BQU8sQ0FBQyxDQUFDO0lBQ25FLENBQUM7Ozs7OztJQUVELGdDQUFLOzs7OztJQUFMLFVBQU0sT0FBbUMsRUFBRSxPQUE4QjtRQUN2RSxPQUFPLElBQUksQ0FBQyxhQUFhLENBQUMsRUFBRSxJQUFJLEVBQUUsT0FBTyxFQUFFLE9BQU8sU0FBQSxFQUFFLEVBQUUsT0FBTyxDQUFDLENBQUM7SUFDakUsQ0FBQzs7Ozs7O0lBRUQsK0JBQUk7Ozs7O0lBQUosVUFBSyxPQUFtQyxFQUFFLE9BQThCO1FBQ3RFLE9BQU8sSUFBSSxDQUFDLGFBQWEsQ0FBQyxFQUFFLElBQUksRUFBRSxNQUFNLEVBQUUsT0FBTyxTQUFBLEVBQUUsRUFBRSxPQUFPLENBQUMsQ0FBQztJQUNoRSxDQUFDOzs7Ozs7SUFFRCxrQ0FBTzs7Ozs7SUFBUCxVQUFRLE9BQW1DLEVBQUUsT0FBOEI7UUFDekUsT0FBTyxJQUFJLENBQUMsYUFBYSxDQUFDLEVBQUUsSUFBSSxFQUFFLFNBQVMsRUFBRSxPQUFPLFNBQUEsRUFBRSxFQUFFLE9BQU8sQ0FBQyxDQUFDO0lBQ25FLENBQUM7Ozs7OztJQUVELGtDQUFPOzs7OztJQUFQLFVBQVEsT0FBbUMsRUFBRSxPQUE4QjtRQUN6RSxPQUFPLElBQUksQ0FBQyxhQUFhLENBQUMsRUFBRSxJQUFJLEVBQUUsU0FBUyxFQUFFLE9BQU8sU0FBQSxFQUFFLEVBQUUsT0FBTyxDQUFDLENBQUM7SUFDbkUsQ0FBQzs7Ozs7OztJQUVELGlDQUFNOzs7Ozs7SUFBTixVQUNFLElBQW1FLEVBQ25FLE9BQW1DLEVBQ25DLE9BQThCO1FBRTlCLE9BQU8sSUFBSSxDQUFDLGFBQWEsQ0FBQyxFQUFFLElBQUksTUFBQSxFQUFFLE9BQU8sU0FBQSxFQUFFLEVBQUUsT0FBTyxDQUFDLENBQUM7SUFDeEQsQ0FBQzs7Z0JBdkNGLFVBQVUsU0FBQztvQkFDVixVQUFVLEVBQUUsTUFBTTtpQkFDbkI7Ozs7Z0JBbEZRLE9BQU87Z0JBTWQsUUFBUTtnQkFIUix3QkFBd0I7Z0JBRHhCLGNBQWM7OzsyQkFGaEI7Q0F3SEMsQUF4Q0QsQ0FHc0Msb0JBQW9CLEdBcUN6RDtTQXJDWSxnQkFBZ0IiLCJzb3VyY2VzQ29udGVudCI6WyJpbXBvcnQgeyBPdmVybGF5IH0gZnJvbSAnQGFuZ3VsYXIvY2RrL292ZXJsYXknO1xuaW1wb3J0IHtcbiAgQXBwbGljYXRpb25SZWYsXG4gIENvbXBvbmVudEZhY3RvcnlSZXNvbHZlcixcbiAgRW1iZWRkZWRWaWV3UmVmLFxuICBJbmplY3RhYmxlLFxuICBJbmplY3RvcixcbiAgVGVtcGxhdGVSZWYsXG4gIFR5cGVcbn0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5cbmltcG9ydCB7IE56TWVzc2FnZUNvbmZpZyB9IGZyb20gJy4vbnotbWVzc2FnZS1jb25maWcnO1xuaW1wb3J0IHsgTnpNZXNzYWdlQ29udGFpbmVyQ29tcG9uZW50IH0gZnJvbSAnLi9uei1tZXNzYWdlLWNvbnRhaW5lci5jb21wb25lbnQnO1xuaW1wb3J0IHsgTnpNZXNzYWdlRGF0YSwgTnpNZXNzYWdlRGF0YUZpbGxlZCwgTnpNZXNzYWdlRGF0YU9wdGlvbnMgfSBmcm9tICcuL256LW1lc3NhZ2UuZGVmaW5pdGlvbnMnO1xuXG5sZXQgZ2xvYmFsQ291bnRlciA9IDA7XG5cbmV4cG9ydCBjbGFzcyBOek1lc3NhZ2VCYXNlU2VydmljZTxcbiAgQ29udGFpbmVyQ2xhc3MgZXh0ZW5kcyBOek1lc3NhZ2VDb250YWluZXJDb21wb25lbnQsXG4gIE1lc3NhZ2VEYXRhLFxuICBNZXNzYWdlQ29uZmlnIGV4dGVuZHMgTnpNZXNzYWdlQ29uZmlnXG4+IHtcbiAgcHJvdGVjdGVkIF9jb250YWluZXI6IENvbnRhaW5lckNsYXNzO1xuXG4gIGNvbnN0cnVjdG9yKFxuICAgIHByaXZhdGUgb3ZlcmxheTogT3ZlcmxheSxcbiAgICBwcml2YXRlIGNvbnRhaW5lckNsYXNzOiBUeXBlPENvbnRhaW5lckNsYXNzPixcbiAgICBwcml2YXRlIGluamVjdG9yOiBJbmplY3RvcixcbiAgICBwcml2YXRlIGNmcjogQ29tcG9uZW50RmFjdG9yeVJlc29sdmVyLFxuICAgIHByaXZhdGUgYXBwUmVmOiBBcHBsaWNhdGlvblJlZixcbiAgICBwcml2YXRlIF9pZFByZWZpeDogc3RyaW5nID0gJydcbiAgKSB7XG4gICAgdGhpcy5fY29udGFpbmVyID0gdGhpcy5jcmVhdGVDb250YWluZXIoKTtcbiAgfVxuXG4gIHJlbW92ZShtZXNzYWdlSWQ/OiBzdHJpbmcpOiB2b2lkIHtcbiAgICBpZiAobWVzc2FnZUlkKSB7XG4gICAgICB0aGlzLl9jb250YWluZXIucmVtb3ZlTWVzc2FnZShtZXNzYWdlSWQpO1xuICAgIH0gZWxzZSB7XG4gICAgICB0aGlzLl9jb250YWluZXIucmVtb3ZlTWVzc2FnZUFsbCgpO1xuICAgIH1cbiAgfVxuXG4gIGNyZWF0ZU1lc3NhZ2UobWVzc2FnZTogTWVzc2FnZURhdGEsIG9wdGlvbnM/OiBOek1lc3NhZ2VEYXRhT3B0aW9ucyk6IE56TWVzc2FnZURhdGFGaWxsZWQge1xuICAgIGNvbnN0IHJlc3VsdE1lc3NhZ2U6IE56TWVzc2FnZURhdGFGaWxsZWQgPSB7XG4gICAgICAuLi4obWVzc2FnZSBhcyBOek1lc3NhZ2VEYXRhKSxcbiAgICAgIC4uLntcbiAgICAgICAgY3JlYXRlZEF0OiBuZXcgRGF0ZSgpLFxuICAgICAgICBtZXNzYWdlSWQ6IHRoaXMuX2dlbmVyYXRlTWVzc2FnZUlkKCksXG4gICAgICAgIG9wdGlvbnNcbiAgICAgIH1cbiAgICB9O1xuICAgIHRoaXMuX2NvbnRhaW5lci5jcmVhdGVNZXNzYWdlKHJlc3VsdE1lc3NhZ2UpO1xuXG4gICAgcmV0dXJuIHJlc3VsdE1lc3NhZ2U7XG4gIH1cblxuICBjb25maWcoY29uZmlnOiBNZXNzYWdlQ29uZmlnKTogdm9pZCB7XG4gICAgdGhpcy5fY29udGFpbmVyLnNldENvbmZpZyhjb25maWcpO1xuICB9XG5cbiAgcHJvdGVjdGVkIF9nZW5lcmF0ZU1lc3NhZ2VJZCgpOiBzdHJpbmcge1xuICAgIHJldHVybiB0aGlzLl9pZFByZWZpeCArIGdsb2JhbENvdW50ZXIrKztcbiAgfVxuXG4gIC8vIE1hbnVhbGx5IGNyZWF0aW5nIGNvbnRhaW5lciBmb3Igb3ZlcmxheSB0byBhdm9pZCBtdWx0aS1jaGVja2luZyBlcnJvciwgc2VlOiBodHRwczovL2dpdGh1Yi5jb20vTkctWk9SUk8vbmctem9ycm8tYW50ZC9pc3N1ZXMvMzkxXG4gIC8vIE5PVEU6IHdlIG5ldmVyIGNsZWFuIHVwIHRoZSBjb250YWluZXIgY29tcG9uZW50IGFuZCBpdCdzIG92ZXJsYXkgcmVzb3VyY2VzLCBpZiB3ZSBzaG91bGQsIHdlIG5lZWQgdG8gZG8gaXQgYnkgb3VyIG93biBjb2Rlcy5cbiAgcHJpdmF0ZSBjcmVhdGVDb250YWluZXIoKTogQ29udGFpbmVyQ2xhc3Mge1xuICAgIGNvbnN0IGZhY3RvcnkgPSB0aGlzLmNmci5yZXNvbHZlQ29tcG9uZW50RmFjdG9yeSh0aGlzLmNvbnRhaW5lckNsYXNzKTtcbiAgICBjb25zdCBjb21wb25lbnRSZWYgPSBmYWN0b3J5LmNyZWF0ZSh0aGlzLmluamVjdG9yKTsgLy8gVXNlIHJvb3QgaW5qZWN0b3JcbiAgICBjb21wb25lbnRSZWYuY2hhbmdlRGV0ZWN0b3JSZWYuZGV0ZWN0Q2hhbmdlcygpOyAvLyBJbW1lZGlhdGVseSBjaGFuZ2UgZGV0ZWN0aW9uIHRvIGF2b2lkIG11bHRpLWNoZWNraW5nIGVycm9yXG4gICAgdGhpcy5hcHBSZWYuYXR0YWNoVmlldyhjb21wb25lbnRSZWYuaG9zdFZpZXcpOyAvLyBMb2FkIHZpZXcgaW50byBhcHAgcm9vdFxuICAgIGNvbnN0IG92ZXJsYXlQYW5lID0gdGhpcy5vdmVybGF5LmNyZWF0ZSgpLm92ZXJsYXlFbGVtZW50O1xuICAgIG92ZXJsYXlQYW5lLnN0eWxlLnpJbmRleCA9ICcxMDEwJzsgLy8gUGF0Y2hpbmc6IGFzc2lnbiB0aGUgc2FtZSB6SW5kZXggb2YgYW50LW1lc3NhZ2UgdG8gaXQncyBwYXJlbnQgb3ZlcmxheSBwYW5lbCwgdG8gdGhlIGFudC1tZXNzYWdlJ3MgemluZGV4IHdvcmsuXG4gICAgb3ZlcmxheVBhbmUuYXBwZW5kQ2hpbGQoKGNvbXBvbmVudFJlZi5ob3N0VmlldyBhcyBFbWJlZGRlZFZpZXdSZWY8e30+KS5yb290Tm9kZXNbMF0gYXMgSFRNTEVsZW1lbnQpO1xuXG4gICAgcmV0dXJuIGNvbXBvbmVudFJlZi5pbnN0YW5jZTtcbiAgfVxufVxuXG5ASW5qZWN0YWJsZSh7XG4gIHByb3ZpZGVkSW46ICdyb290J1xufSlcbmV4cG9ydCBjbGFzcyBOek1lc3NhZ2VTZXJ2aWNlIGV4dGVuZHMgTnpNZXNzYWdlQmFzZVNlcnZpY2U8XG4gIE56TWVzc2FnZUNvbnRhaW5lckNvbXBvbmVudCxcbiAgTnpNZXNzYWdlRGF0YSxcbiAgTnpNZXNzYWdlQ29uZmlnXG4+IHtcbiAgY29uc3RydWN0b3Iob3ZlcmxheTogT3ZlcmxheSwgaW5qZWN0b3I6IEluamVjdG9yLCBjZnI6IENvbXBvbmVudEZhY3RvcnlSZXNvbHZlciwgYXBwUmVmOiBBcHBsaWNhdGlvblJlZikge1xuICAgIHN1cGVyKG92ZXJsYXksIE56TWVzc2FnZUNvbnRhaW5lckNvbXBvbmVudCwgaW5qZWN0b3IsIGNmciwgYXBwUmVmLCAnbWVzc2FnZS0nKTtcbiAgfVxuXG4gIC8vIFNob3J0Y3V0IG1ldGhvZHNcbiAgc3VjY2Vzcyhjb250ZW50OiBzdHJpbmcgfCBUZW1wbGF0ZVJlZjx2b2lkPiwgb3B0aW9ucz86IE56TWVzc2FnZURhdGFPcHRpb25zKTogTnpNZXNzYWdlRGF0YUZpbGxlZCB7XG4gICAgcmV0dXJuIHRoaXMuY3JlYXRlTWVzc2FnZSh7IHR5cGU6ICdzdWNjZXNzJywgY29udGVudCB9LCBvcHRpb25zKTtcbiAgfVxuXG4gIGVycm9yKGNvbnRlbnQ6IHN0cmluZyB8IFRlbXBsYXRlUmVmPHZvaWQ+LCBvcHRpb25zPzogTnpNZXNzYWdlRGF0YU9wdGlvbnMpOiBOek1lc3NhZ2VEYXRhRmlsbGVkIHtcbiAgICByZXR1cm4gdGhpcy5jcmVhdGVNZXNzYWdlKHsgdHlwZTogJ2Vycm9yJywgY29udGVudCB9LCBvcHRpb25zKTtcbiAgfVxuXG4gIGluZm8oY29udGVudDogc3RyaW5nIHwgVGVtcGxhdGVSZWY8dm9pZD4sIG9wdGlvbnM/OiBOek1lc3NhZ2VEYXRhT3B0aW9ucyk6IE56TWVzc2FnZURhdGFGaWxsZWQge1xuICAgIHJldHVybiB0aGlzLmNyZWF0ZU1lc3NhZ2UoeyB0eXBlOiAnaW5mbycsIGNvbnRlbnQgfSwgb3B0aW9ucyk7XG4gIH1cblxuICB3YXJuaW5nKGNvbnRlbnQ6IHN0cmluZyB8IFRlbXBsYXRlUmVmPHZvaWQ+LCBvcHRpb25zPzogTnpNZXNzYWdlRGF0YU9wdGlvbnMpOiBOek1lc3NhZ2VEYXRhRmlsbGVkIHtcbiAgICByZXR1cm4gdGhpcy5jcmVhdGVNZXNzYWdlKHsgdHlwZTogJ3dhcm5pbmcnLCBjb250ZW50IH0sIG9wdGlvbnMpO1xuICB9XG5cbiAgbG9hZGluZyhjb250ZW50OiBzdHJpbmcgfCBUZW1wbGF0ZVJlZjx2b2lkPiwgb3B0aW9ucz86IE56TWVzc2FnZURhdGFPcHRpb25zKTogTnpNZXNzYWdlRGF0YUZpbGxlZCB7XG4gICAgcmV0dXJuIHRoaXMuY3JlYXRlTWVzc2FnZSh7IHR5cGU6ICdsb2FkaW5nJywgY29udGVudCB9LCBvcHRpb25zKTtcbiAgfVxuXG4gIGNyZWF0ZShcbiAgICB0eXBlOiAnc3VjY2VzcycgfCAnaW5mbycgfCAnd2FybmluZycgfCAnZXJyb3InIHwgJ2xvYWRpbmcnIHwgc3RyaW5nLFxuICAgIGNvbnRlbnQ6IHN0cmluZyB8IFRlbXBsYXRlUmVmPHZvaWQ+LFxuICAgIG9wdGlvbnM/OiBOek1lc3NhZ2VEYXRhT3B0aW9uc1xuICApOiBOek1lc3NhZ2VEYXRhRmlsbGVkIHtcbiAgICByZXR1cm4gdGhpcy5jcmVhdGVNZXNzYWdlKHsgdHlwZSwgY29udGVudCB9LCBvcHRpb25zKTtcbiAgfVxufVxuIl19