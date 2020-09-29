/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { Overlay } from '@angular/cdk/overlay';
import { ComponentPortal } from '@angular/cdk/portal';
import { Injectable } from '@angular/core';
import { LoggerService } from '../core/util/logger/logger.service';
import { NzModalControlService } from './nz-modal-control.service';
import { NzModalComponent } from './nz-modal.component';
// A builder used for managing service creating modals
var 
// A builder used for managing service creating modals
ModalBuilderForService = /** @class */ (function () {
    function ModalBuilderForService(overlay, options) {
        if (options === void 0) { options = {}; }
        var _this = this;
        this.overlay = overlay;
        this.createModal();
        if (!('nzGetContainer' in options)) {
            // As we use CDK to create modal in service by force, there is no need to use nzGetContainer
            options.nzGetContainer = undefined; // Override nzGetContainer's default value to prevent creating another overlay
        }
        this.changeProps(options);
        (/** @type {?} */ (this.modalRef)).instance.open();
        (/** @type {?} */ (this.modalRef)).instance.nzAfterClose.subscribe((/**
         * @return {?}
         */
        function () { return _this.destroyModal(); })); // [NOTE] By default, close equals destroy when using as Service
    }
    /**
     * @return {?}
     */
    ModalBuilderForService.prototype.getInstance = /**
     * @return {?}
     */
    function () {
        return this.modalRef && this.modalRef.instance;
    };
    /**
     * @return {?}
     */
    ModalBuilderForService.prototype.destroyModal = /**
     * @return {?}
     */
    function () {
        if (this.modalRef) {
            this.overlayRef.dispose();
            this.modalRef = null;
        }
    };
    /**
     * @private
     * @param {?} options
     * @return {?}
     */
    ModalBuilderForService.prototype.changeProps = /**
     * @private
     * @param {?} options
     * @return {?}
     */
    function (options) {
        if (this.modalRef) {
            Object.assign(this.modalRef.instance, options); // DANGER: here not limit user's inputs at runtime
        }
    };
    // Create component to ApplicationRef
    // Create component to ApplicationRef
    /**
     * @private
     * @return {?}
     */
    ModalBuilderForService.prototype.createModal = 
    // Create component to ApplicationRef
    /**
     * @private
     * @return {?}
     */
    function () {
        this.overlayRef = this.overlay.create();
        this.modalRef = this.overlayRef.attach(new ComponentPortal(NzModalComponent));
    };
    return ModalBuilderForService;
}());
// A builder used for managing service creating modals
export { ModalBuilderForService };
if (false) {
    /**
     * @type {?}
     * @private
     */
    ModalBuilderForService.prototype.modalRef;
    /**
     * @type {?}
     * @private
     */
    ModalBuilderForService.prototype.overlayRef;
    /**
     * @type {?}
     * @private
     */
    ModalBuilderForService.prototype.overlay;
}
var NzModalService = /** @class */ (function () {
    function NzModalService(overlay, logger, modalControl) {
        this.overlay = overlay;
        this.logger = logger;
        this.modalControl = modalControl;
    }
    Object.defineProperty(NzModalService.prototype, "openModals", {
        // Track of the current close modals (we assume invisible is close this time)
        get: 
        // Track of the current close modals (we assume invisible is close this time)
        /**
         * @return {?}
         */
        function () {
            return this.modalControl.openModals;
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzModalService.prototype, "afterAllClose", {
        get: /**
         * @return {?}
         */
        function () {
            return this.modalControl.afterAllClose.asObservable();
        },
        enumerable: true,
        configurable: true
    });
    // Closes all of the currently-open dialogs
    // Closes all of the currently-open dialogs
    /**
     * @return {?}
     */
    NzModalService.prototype.closeAll = 
    // Closes all of the currently-open dialogs
    /**
     * @return {?}
     */
    function () {
        this.modalControl.closeAll();
    };
    /**
     * @template T
     * @param {?=} options
     * @return {?}
     */
    NzModalService.prototype.create = /**
     * @template T
     * @param {?=} options
     * @return {?}
     */
    function (options) {
        if (options === void 0) { options = {}; }
        if (typeof options.nzOnCancel !== 'function') {
            options.nzOnCancel = (/**
             * @return {?}
             */
            function () { }); // Leave a empty function to close this modal by default
        }
        // NOTE: use NzModalComponent as the NzModalRef by now, we may need archive the real NzModalRef object in the future
        /** @type {?} */
        var modalRef = (/** @type {?} */ (new ModalBuilderForService(this.overlay, options).getInstance()));
        return modalRef;
    };
    /**
     * @template T
     * @param {?=} options
     * @param {?=} confirmType
     * @return {?}
     */
    NzModalService.prototype.confirm = /**
     * @template T
     * @param {?=} options
     * @param {?=} confirmType
     * @return {?}
     */
    function (options, confirmType) {
        if (options === void 0) { options = {}; }
        if (confirmType === void 0) { confirmType = 'confirm'; }
        if ('nzFooter' in options) {
            this.logger.warn("The Confirm-Modal doesn't support \"nzFooter\", this property will be ignored.");
        }
        if (!('nzWidth' in options)) {
            options.nzWidth = 416;
        }
        if (typeof options.nzOnOk !== 'function') {
            // NOTE: only support function currently by calling confirm()
            options.nzOnOk = (/**
             * @return {?}
             */
            function () { }); // Leave a empty function to close this modal by default
        }
        options.nzModalType = 'confirm';
        options.nzClassName = "ant-modal-confirm ant-modal-confirm-" + confirmType + " " + (options.nzClassName || '');
        options.nzMaskClosable = false;
        return this.create(options);
    };
    /**
     * @template T
     * @param {?=} options
     * @return {?}
     */
    NzModalService.prototype.info = /**
     * @template T
     * @param {?=} options
     * @return {?}
     */
    function (options) {
        if (options === void 0) { options = {}; }
        return this.simpleConfirm(options, 'info');
    };
    /**
     * @template T
     * @param {?=} options
     * @return {?}
     */
    NzModalService.prototype.success = /**
     * @template T
     * @param {?=} options
     * @return {?}
     */
    function (options) {
        if (options === void 0) { options = {}; }
        return this.simpleConfirm(options, 'success');
    };
    /**
     * @template T
     * @param {?=} options
     * @return {?}
     */
    NzModalService.prototype.error = /**
     * @template T
     * @param {?=} options
     * @return {?}
     */
    function (options) {
        if (options === void 0) { options = {}; }
        return this.simpleConfirm(options, 'error');
    };
    /**
     * @template T
     * @param {?=} options
     * @return {?}
     */
    NzModalService.prototype.warning = /**
     * @template T
     * @param {?=} options
     * @return {?}
     */
    function (options) {
        if (options === void 0) { options = {}; }
        return this.simpleConfirm(options, 'warning');
    };
    /**
     * @private
     * @template T
     * @param {?=} options
     * @param {?=} confirmType
     * @return {?}
     */
    NzModalService.prototype.simpleConfirm = /**
     * @private
     * @template T
     * @param {?=} options
     * @param {?=} confirmType
     * @return {?}
     */
    function (options, confirmType) {
        if (options === void 0) { options = {}; }
        /** @type {?} */
        var iconMap = {
            info: 'info-circle',
            success: 'check-circle',
            error: 'close-circle',
            warning: 'exclamation-circle'
        };
        if (!('nzIconType' in options)) {
            options.nzIconType = iconMap[confirmType];
        }
        if (!('nzCancelText' in options)) {
            // Remove the Cancel button if the user not specify a Cancel button
            options.nzCancelText = null;
        }
        return this.confirm(options, confirmType);
    };
    NzModalService.decorators = [
        { type: Injectable }
    ];
    /** @nocollapse */
    NzModalService.ctorParameters = function () { return [
        { type: Overlay },
        { type: LoggerService },
        { type: NzModalControlService }
    ]; };
    return NzModalService;
}());
export { NzModalService };
if (false) {
    /**
     * @type {?}
     * @private
     */
    NzModalService.prototype.overlay;
    /**
     * @type {?}
     * @private
     */
    NzModalService.prototype.logger;
    /**
     * @type {?}
     * @private
     */
    NzModalService.prototype.modalControl;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotbW9kYWwuc2VydmljZS5qcyIsInNvdXJjZVJvb3QiOiJuZzovL25nLXpvcnJvLWFudGQvIiwic291cmNlcyI6WyJtb2RhbC9uei1tb2RhbC5zZXJ2aWNlLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7QUFBQSxPQUFPLEVBQUUsT0FBTyxFQUFjLE1BQU0sc0JBQXNCLENBQUM7QUFDM0QsT0FBTyxFQUFFLGVBQWUsRUFBRSxNQUFNLHFCQUFxQixDQUFDO0FBQ3RELE9BQU8sRUFBZ0IsVUFBVSxFQUFFLE1BQU0sZUFBZSxDQUFDO0FBSXpELE9BQU8sRUFBRSxhQUFhLEVBQUUsTUFBTSxvQ0FBb0MsQ0FBQztBQUVuRSxPQUFPLEVBQUUscUJBQXFCLEVBQUUsTUFBTSw0QkFBNEIsQ0FBQztBQUVuRSxPQUFPLEVBQUUsZ0JBQWdCLEVBQUUsTUFBTSxzQkFBc0IsQ0FBQzs7QUFJeEQ7OztJQUlFLGdDQUFvQixPQUFnQixFQUFFLE9BQW9DO1FBQXBDLHdCQUFBLEVBQUEsWUFBb0M7UUFBMUUsaUJBV0M7UUFYbUIsWUFBTyxHQUFQLE9BQU8sQ0FBUztRQUNsQyxJQUFJLENBQUMsV0FBVyxFQUFFLENBQUM7UUFFbkIsSUFBSSxDQUFDLENBQUMsZ0JBQWdCLElBQUksT0FBTyxDQUFDLEVBQUU7WUFDbEMsNEZBQTRGO1lBQzVGLE9BQU8sQ0FBQyxjQUFjLEdBQUcsU0FBUyxDQUFDLENBQUMsOEVBQThFO1NBQ25IO1FBRUQsSUFBSSxDQUFDLFdBQVcsQ0FBQyxPQUFPLENBQUMsQ0FBQztRQUMxQixtQkFBQSxJQUFJLENBQUMsUUFBUSxFQUFDLENBQUMsUUFBUSxDQUFDLElBQUksRUFBRSxDQUFDO1FBQy9CLG1CQUFBLElBQUksQ0FBQyxRQUFRLEVBQUMsQ0FBQyxRQUFRLENBQUMsWUFBWSxDQUFDLFNBQVM7OztRQUFDLGNBQU0sT0FBQSxLQUFJLENBQUMsWUFBWSxFQUFFLEVBQW5CLENBQW1CLEVBQUMsQ0FBQyxDQUFDLGdFQUFnRTtJQUM3SSxDQUFDOzs7O0lB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RCxJQUFJLENBQUMsQ0FBQyxZQUFZLElBQUksT0FBTyxDQUFDLEVBQUU7WUFDOUIsT0FBTyxDQUFDLFVBQVUsR0FBRyxPQUFPLENBQUMsV0FBVyxDQUFDLENBQUM7U0FDM0M7UUFDRCxJQUFJLENBQUMsQ0FBQyxjQUFjLElBQUksT0FBTyxDQUFDLEVBQUU7WUFDaEMsbUVBQW1FO1lBQ25FLE9BQU8sQ0FBQyxZQUFZLEdBQUcsSUFBSSxDQUFDO1NBQzdCO1FBQ0QsT0FBTyxJQUFJLENBQUMsT0FBTyxDQUFDLE9BQU8sRUFBRSxXQUFXLENBQUMsQ0FBQztJQUM1QyxDQUFDOztnQkE5RUYsVUFBVTs7OztnQkF2REYsT0FBTztnQkFNUCxhQUFhO2dCQUViLHFCQUFxQjs7SUE4SDlCLHFCQUFDO0NBQUEsQUEvRUQsSUErRUM7U0E5RVksY0FBYzs7Ozs7O0lBVWIsaUNBQXdCOzs7OztJQUFFLGdDQUE2Qjs7Ozs7SUFBRSxzQ0FBMkMiLCJzb3VyY2VzQ29udGVudCI6WyJpbXBvcnQgeyBPdmVybGF5LCBPdmVybGF5UmVmIH0gZnJvbSAnQGFuZ3VsYXIvY2RrL292ZXJsYXknO1xuaW1wb3J0IHsgQ29tcG9uZW50UG9ydGFsIH0gZnJvbSAnQGFuZ3VsYXIvY2RrL3BvcnRhbCc7XG5pbXBvcnQgeyBDb21wb25lbnRSZWYsIEluamVjdGFibGUgfSBmcm9tICdAYW5ndWxhci9jb3JlJztcbmltcG9ydCB7IE9ic2VydmFibGUgfSBmcm9tICdyeGpzJztcbmltcG9ydCB7IEluZGV4YWJsZU9iamVjdCB9IGZyb20gJy4uL2NvcmUvdHlwZXMvaW5kZXhhYmxlJztcblxuaW1wb3J0IHsgTG9nZ2VyU2VydmljZSB9IGZyb20gJy4uL2NvcmUvdXRpbC9sb2dnZXIvbG9nZ2VyLnNlcnZpY2UnO1xuXG5pbXBvcnQgeyBOek1vZGFsQ29udHJvbFNlcnZpY2UgfSBmcm9tICcuL256LW1vZGFsLWNvbnRyb2wuc2VydmljZSc7XG5pbXBvcnQgeyBOek1vZGFsUmVmIH0gZnJvbSAnLi9uei1tb2RhbC1yZWYuY2xhc3MnO1xuaW1wb3J0IHsgTnpNb2RhbENvbXBvbmVudCB9IGZyb20gJy4vbnotbW9kYWwuY29tcG9uZW50JztcbmltcG9ydCB7IENvbmZpcm1UeXBlLCBNb2RhbE9wdGlvbnMsIE1vZGFsT3B0aW9uc0ZvclNlcnZpY2UgfSBmcm9tICcuL256LW1vZGFsLnR5cGUnO1xuXG4vLyBBIGJ1aWxkZXIgdXNlZCBmb3IgbWFuYWdpbmcgc2VydmljZSBjcmVhdGluZyBtb2RhbHNcbmV4cG9ydCBjbGFzcyBNb2RhbEJ1aWxkZXJGb3JTZXJ2aWNlIHtcbiAgcHJpdmF0ZSBtb2RhbFJlZjogQ29tcG9uZW50UmVmPE56TW9kYWxDb21wb25lbnQ+IHwgbnVsbDsgLy8gTW9kYWwgQ29tcG9uZW50UmVmLCBcIm51bGxcIiBtZWFucyBpdCBoYXMgYmVlbiBkZXN0cm95ZWRcbiAgcHJpdmF0ZSBvdmVybGF5UmVmOiBPdmVybGF5UmVmO1xuXG4gIGNvbnN0cnVjdG9yKHByaXZhdGUgb3ZlcmxheTogT3ZlcmxheSwgb3B0aW9uczogTW9kYWxPcHRpb25zRm9yU2VydmljZSA9IHt9KSB7XG4gICAgdGhpcy5jcmVhdGVNb2RhbCgpO1xuXG4gICAgaWYgKCEoJ256R2V0Q29udGFpbmVyJyBpbiBvcHRpb25zKSkge1xuICAgICAgLy8gQXMgd2UgdXNlIENESyB0byBjcmVhdGUgbW9kYWwgaW4gc2VydmljZSBieSBmb3JjZSwgdGhlcmUgaXMgbm8gbmVlZCB0byB1c2UgbnpHZXRDb250YWluZXJcbiAgICAgIG9wdGlvbnMubnpHZXRDb250YWluZXIgPSB1bmRlZmluZWQ7IC8vIE92ZXJyaWRlIG56R2V0Q29udGFpbmVyJ3MgZGVmYXVsdCB2YWx1ZSB0byBwcmV2ZW50IGNyZWF0aW5nIGFub3RoZXIgb3ZlcmxheVxuICAgIH1cblxuICAgIHRoaXMuY2hhbmdlUHJvcHMob3B0aW9ucyk7XG4gICAgdGhpcy5tb2RhbFJlZiEuaW5zdGFuY2Uub3BlbigpO1xuICAgIHRoaXMubW9kYWxSZWYhLmluc3RhbmNlLm56QWZ0ZXJDbG9zZS5zdWJzY3JpYmUoKCkgPT4gdGhpcy5kZXN0cm95TW9kYWwoKSk7IC8vIFtOT1RFXSBCeSBkZWZhdWx0LCBjbG9zZSBlcXVhbHMgZGVzdHJveSB3aGVuIHVzaW5nIGFzIFNlcnZpY2VcbiAgfVxuXG4gIGdldEluc3RhbmNlKCk6IE56TW9kYWxDb21wb25lbnQgfCBudWxsIHtcbiAgICByZXR1cm4gdGhpcy5tb2RhbFJlZiAmJiB0aGlzLm1vZGFsUmVmLmluc3RhbmNlO1xuICB9XG5cbiAgZGVzdHJveU1vZGFsKCk6IHZvaWQge1xuICAgIGlmICh0aGlzLm1vZGFsUmVmKSB7XG4gICAgICB0aGlzLm92ZXJsYXlSZWYuZGlzcG9zZSgpO1xuICAgICAgdGhpcy5tb2RhbFJlZiA9IG51bGw7XG4gICAgfVxuICB9XG5cbiAgcHJpdmF0ZSBjaGFuZ2VQcm9wcyhvcHRpb25zOiBNb2RhbE9wdGlvbnMpOiB2b2lkIHtcbiAgICBpZiAodGhpcy5tb2RhbFJlZikge1xuICAgICAgT2JqZWN0LmFzc2lnbih0aGlzLm1vZGFsUmVmLmluc3RhbmNlLCBvcHRpb25zKTsgLy8gREFOR0VSOiBoZXJlIG5vdCBsaW1pdCB1c2VyJ3MgaW5wdXRzIGF0IHJ1bnRpbWVcbiAgICB9XG4gIH1cblxuICAvLyBDcmVhdGUgY29tcG9uZW50IHRvIEFwcGxpY2F0aW9uUmVmXG4gIHByaXZhdGUgY3JlYXRlTW9kYWwoKTogdm9pZCB7XG4gICAgdGhpcy5vdmVybGF5UmVmID0gdGhpcy5vdmVybGF5LmNyZWF0ZSgpO1xuICAgIHRoaXMubW9kYWxSZWYgPSB0aGlzLm92ZXJsYXlSZWYuYXR0YWNoKG5ldyBDb21wb25lbnRQb3J0YWwoTnpNb2RhbENvbXBvbmVudCkpO1xuICB9XG59XG5cbkBJbmplY3RhYmxlKClcbmV4cG9ydCBjbGFzcyBOek1vZGFsU2VydmljZSB7XG4gIC8vIFRyYWNrIG9mIHRoZSBjdXJyZW50IGNsb3NlIG1vZGFscyAod2UgYXNzdW1lIGludmlzaWJsZSBpcyBjbG9zZSB0aGlzIHRpbWUpXG4gIGdldCBvcGVuTW9kYWxzKCk6IE56TW9kYWxSZWZbXSB7XG4gICAgcmV0dXJuIHRoaXMubW9kYWxDb250cm9sLm9wZW5Nb2RhbHM7XG4gIH1cblxuICBnZXQgYWZ0ZXJBbGxDbG9zZSgpOiBPYnNlcnZhYmxlPHZvaWQ+IHtcbiAgICByZXR1cm4gdGhpcy5tb2RhbENvbnRyb2wuYWZ0ZXJBbGxDbG9zZS5hc09ic2VydmFibGUoKTtcbiAgfVxuXG4gIGNvbnN0cnVjdG9yKHByaXZhdGUgb3ZlcmxheTogT3ZlcmxheSwgcHJpdmF0ZSBsb2dnZXI6IExvZ2dlclNlcnZpY2UsIHByaXZhdGUgbW9kYWxDb250cm9sOiBOek1vZGFsQ29udHJvbFNlcnZpY2UpIHt9XG5cbiAgLy8gQ2xvc2VzIGFsbCBvZiB0aGUgY3VycmVudGx5LW9wZW4gZGlhbG9nc1xuICBjbG9zZUFsbCgpOiB2b2lkIHtcbiAgICB0aGlzLm1vZGFsQ29udHJvbC5jbG9zZUFsbCgpO1xuICB9XG5cbiAgY3JlYXRlPFQ+KG9wdGlvbnM6IE1vZGFsT3B0aW9uc0ZvclNlcnZpY2U8VD4gPSB7fSk6IE56TW9kYWxSZWY8VD4ge1xuICAgIGlmICh0eXBlb2Ygb3B0aW9ucy5uek9uQ2FuY2VsICE9PSAnZnVuY3Rpb24nKSB7XG4gICAgICBvcHRpb25zLm56T25DYW5jZWwgPSAoKSA9PiB7fTsgLy8gTGVhdmUgYSBlbXB0eSBmdW5jdGlvbiB0byBjbG9zZSB0aGlzIG1vZGFsIGJ5IGRlZmF1bHRcbiAgICB9XG5cbiAgICAvLyBOT1RFOiB1c2UgTnpNb2RhbENvbXBvbmVudCBhcyB0aGUgTnpNb2RhbFJlZiBieSBub3csIHdlIG1heSBuZWVkIGFyY2hpdmUgdGhlIHJlYWwgTnpNb2RhbFJlZiBvYmplY3QgaW4gdGhlIGZ1dHVyZVxuICAgIGNvbnN0IG1vZGFsUmVmID0gbmV3IE1vZGFsQnVpbGRlckZvclNlcnZpY2UodGhpcy5vdmVybGF5LCBvcHRpb25zKS5nZXRJbnN0YW5jZSgpITtcblxuICAgIHJldHVybiBtb2RhbFJlZjtcbiAgfVxuXG4gIGNvbmZpcm08VD4ob3B0aW9uczogTW9kYWxPcHRpb25zRm9yU2VydmljZTxUPiA9IHt9LCBjb25maXJtVHlwZTogQ29uZmlybVR5cGUgPSAnY29uZmlybScpOiBOek1vZGFsUmVmPFQ+IHtcbiAgICBpZiAoJ256Rm9vdGVyJyBpbiBvcHRpb25zKSB7XG4gICAgICB0aGlzLmxvZ2dlci53YXJuKGBUaGUgQ29uZmlybS1Nb2RhbCBkb2Vzbid0IHN1cHBvcnQgXCJuekZvb3RlclwiLCB0aGlzIHByb3BlcnR5IHdpbGwgYmUgaWdub3JlZC5gKTtcbiAgICB9XG4gICAgaWYgKCEoJ256V2lkdGgnIGluIG9wdGlvbnMpKSB7XG4gICAgICBvcHRpb25zLm56V2lkdGggPSA0MTY7XG4gICAgfVxuICAgIGlmICh0eXBlb2Ygb3B0aW9ucy5uek9uT2sgIT09ICdmdW5jdGlvbicpIHtcbiAgICAgIC8vIE5PVEU6IG9ubHkgc3VwcG9ydCBmdW5jdGlvbiBjdXJyZW50bHkgYnkgY2FsbGluZyBjb25maXJtKClcbiAgICAgIG9wdGlvbnMubnpPbk9rID0gKCkgPT4ge307IC8vIExlYXZlIGEgZW1wdHkgZnVuY3Rpb24gdG8gY2xvc2UgdGhpcyBtb2RhbCBieSBkZWZhdWx0XG4gICAgfVxuXG4gICAgb3B0aW9ucy5uek1vZGFsVHlwZSA9ICdjb25maXJtJztcbiAgICBvcHRpb25zLm56Q2xhc3NOYW1lID0gYGFudC1tb2RhbC1jb25maXJtIGFudC1tb2RhbC1jb25maXJtLSR7Y29uZmlybVR5cGV9ICR7b3B0aW9ucy5uekNsYXNzTmFtZSB8fCAnJ31gO1xuICAgIG9wdGlvbnMubnpNYXNrQ2xvc2FibGUgPSBmYWxzZTtcbiAgICByZXR1cm4gdGhpcy5jcmVhdGUob3B0aW9ucyk7XG4gIH1cblxuICBpbmZvPFQ+KG9wdGlvbnM6IE1vZGFsT3B0aW9uc0ZvclNlcnZpY2U8VD4gPSB7fSk6IE56TW9kYWxSZWY8VD4ge1xuICAgIHJldHVybiB0aGlzLnNpbXBsZUNvbmZpcm0ob3B0aW9ucywgJ2luZm8nKTtcbiAgfVxuXG4gIHN1Y2Nlc3M8VD4ob3B0aW9uczogTW9kYWxPcHRpb25zRm9yU2VydmljZTxUPiA9IHt9KTogTnpNb2RhbFJlZjxUPiB7XG4gICAgcmV0dXJuIHRoaXMuc2ltcGxlQ29uZmlybShvcHRpb25zLCAnc3VjY2VzcycpO1xuICB9XG5cbiAgZXJyb3I8VD4ob3B0aW9uczogTW9kYWxPcHRpb25zRm9yU2VydmljZTxUPiA9IHt9KTogTnpNb2RhbFJlZjxUPiB7XG4gICAgcmV0dXJuIHRoaXMuc2ltcGxlQ29uZmlybShvcHRpb25zLCAnZXJyb3InKTtcbiAgfVxuXG4gIHdhcm5pbmc8VD4ob3B0aW9uczogTW9kYWxPcHRpb25zRm9yU2VydmljZTxUPiA9IHt9KTogTnpNb2RhbFJlZjxUPiB7XG4gICAgcmV0dXJuIHRoaXMuc2ltcGxlQ29uZmlybShvcHRpb25zLCAnd2FybmluZycpO1xuICB9XG5cbiAgcHJpdmF0ZSBzaW1wbGVDb25maXJtPFQ+KG9wdGlvbnM6IE1vZGFsT3B0aW9uc0ZvclNlcnZpY2U8VD4gPSB7fSwgY29uZmlybVR5cGU6IENvbmZpcm1UeXBlKTogTnpNb2RhbFJlZjxUPiB7XG4gICAgY29uc3QgaWNvbk1hcDogSW5kZXhhYmxlT2JqZWN0ID0ge1xuICAgICAgaW5mbzogJ2luZm8tY2lyY2xlJyxcbiAgICAgIHN1Y2Nlc3M6ICdjaGVjay1jaXJjbGUnLFxuICAgICAgZXJyb3I6ICdjbG9zZS1jaXJjbGUnLFxuICAgICAgd2FybmluZzogJ2V4Y2xhbWF0aW9uLWNpcmNsZSdcbiAgICB9O1xuICAgIGlmICghKCduekljb25UeXBlJyBpbiBvcHRpb25zKSkge1xuICAgICAgb3B0aW9ucy5uekljb25UeXBlID0gaWNvbk1hcFtjb25maXJtVHlwZV07XG4gICAgfVxuICAgIGlmICghKCduekNhbmNlbFRleHQnIGluIG9wdGlvbnMpKSB7XG4gICAgICAvLyBSZW1vdmUgdGhlIENhbmNlbCBidXR0b24gaWYgdGhlIHVzZXIgbm90IHNwZWNpZnkgYSBDYW5jZWwgYnV0dG9uXG4gICAgICBvcHRpb25zLm56Q2FuY2VsVGV4dCA9IG51bGw7XG4gICAgfVxuICAgIHJldHVybiB0aGlzLmNvbmZpcm0ob3B0aW9ucywgY29uZmlybVR5cGUpO1xuICB9XG59XG4iXX0=