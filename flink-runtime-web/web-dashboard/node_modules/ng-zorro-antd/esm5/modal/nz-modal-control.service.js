/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { Injectable, Optional, SkipSelf } from '@angular/core';
import { Subject } from 'rxjs';
/**
 * @record
 */
function RegisteredMeta() { }
if (false) {
    /** @type {?} */
    RegisteredMeta.prototype.modalRef;
    /** @type {?} */
    RegisteredMeta.prototype.afterOpenSubscription;
    /** @type {?} */
    RegisteredMeta.prototype.afterCloseSubscription;
}
var NzModalControlService = /** @class */ (function () {
    function NzModalControlService(parentService) {
        this.parentService = parentService;
        this.rootOpenModals = this.parentService ? null : [];
        this.rootAfterAllClose = this.parentService ? null : new Subject();
        this.rootRegisteredMetaMap = this.parentService ? null : new Map();
    }
    Object.defineProperty(NzModalControlService.prototype, "afterAllClose", {
        // Track singleton afterAllClose through over the injection tree
        get: 
        // Track singleton afterAllClose through over the injection tree
        /**
         * @return {?}
         */
        function () {
            return this.parentService ? this.parentService.afterAllClose : (/** @type {?} */ (this.rootAfterAllClose));
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzModalControlService.prototype, "openModals", {
        // Track singleton openModals array through over the injection tree
        get: 
        // Track singleton openModals array through over the injection tree
        /**
         * @return {?}
         */
        function () {
            return this.parentService ? this.parentService.openModals : (/** @type {?} */ (this.rootOpenModals));
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzModalControlService.prototype, "registeredMetaMap", {
        get: /**
         * @private
         * @return {?}
         */
        function () {
            // Registered modal for later usage
            return this.parentService ? this.parentService.registeredMetaMap : (/** @type {?} */ (this.rootRegisteredMetaMap));
        },
        enumerable: true,
        configurable: true
    });
    // Register a modal to listen its open/close
    // Register a modal to listen its open/close
    /**
     * @param {?} modalRef
     * @return {?}
     */
    NzModalControlService.prototype.registerModal = 
    // Register a modal to listen its open/close
    /**
     * @param {?} modalRef
     * @return {?}
     */
    function (modalRef) {
        var _this = this;
        if (!this.hasRegistered(modalRef)) {
            /** @type {?} */
            var afterOpenSubscription = modalRef.afterOpen.subscribe((/**
             * @return {?}
             */
            function () { return _this.openModals.push(modalRef); }));
            /** @type {?} */
            var afterCloseSubscription = modalRef.afterClose.subscribe((/**
             * @return {?}
             */
            function () { return _this.removeOpenModal(modalRef); }));
            this.registeredMetaMap.set(modalRef, { modalRef: modalRef, afterOpenSubscription: afterOpenSubscription, afterCloseSubscription: afterCloseSubscription });
        }
    };
    // deregister modals
    // deregister modals
    /**
     * @param {?} modalRef
     * @return {?}
     */
    NzModalControlService.prototype.deregisterModal = 
    // deregister modals
    /**
     * @param {?} modalRef
     * @return {?}
     */
    function (modalRef) {
        /** @type {?} */
        var registeredMeta = this.registeredMetaMap.get(modalRef);
        if (registeredMeta) {
            // Remove this modal if it is still in the opened modal list (NOTE: it may trigger "afterAllClose")
            this.removeOpenModal(registeredMeta.modalRef);
            registeredMeta.afterOpenSubscription.unsubscribe();
            registeredMeta.afterCloseSubscription.unsubscribe();
            this.registeredMetaMap.delete(modalRef);
        }
    };
    /**
     * @param {?} modalRef
     * @return {?}
     */
    NzModalControlService.prototype.hasRegistered = /**
     * @param {?} modalRef
     * @return {?}
     */
    function (modalRef) {
        return this.registeredMetaMap.has(modalRef);
    };
    // Close all registered opened modals
    // Close all registered opened modals
    /**
     * @return {?}
     */
    NzModalControlService.prototype.closeAll = 
    // Close all registered opened modals
    /**
     * @return {?}
     */
    function () {
        /** @type {?} */
        var i = this.openModals.length;
        while (i--) {
            this.openModals[i].close();
        }
    };
    /**
     * @private
     * @param {?} modalRef
     * @return {?}
     */
    NzModalControlService.prototype.removeOpenModal = /**
     * @private
     * @param {?} modalRef
     * @return {?}
     */
    function (modalRef) {
        /** @type {?} */
        var index = this.openModals.indexOf(modalRef);
        if (index > -1) {
            this.openModals.splice(index, 1);
            if (!this.openModals.length) {
                this.afterAllClose.next();
            }
        }
    };
    NzModalControlService.decorators = [
        { type: Injectable }
    ];
    /** @nocollapse */
    NzModalControlService.ctorParameters = function () { return [
        { type: NzModalControlService, decorators: [{ type: Optional }, { type: SkipSelf }] }
    ]; };
    return NzModalControlService;
}());
export { NzModalControlService };
if (false) {
    /**
     * @type {?}
     * @private
     */
    NzModalControlService.prototype.rootOpenModals;
    /**
     * @type {?}
     * @private
     */
    NzModalControlService.prototype.rootAfterAllClose;
    /**
     * @type {?}
     * @private
     */
    NzModalControlService.prototype.rootRegisteredMetaMap;
    /**
     * @type {?}
     * @private
     */
    NzModalControlService.prototype.parentService;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotbW9kYWwtY29udHJvbC5zZXJ2aWNlLmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbIm1vZGFsL256LW1vZGFsLWNvbnRyb2wuc2VydmljZS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7O0FBQUEsT0FBTyxFQUFFLFVBQVUsRUFBRSxRQUFRLEVBQUUsUUFBUSxFQUFFLE1BQU0sZUFBZSxDQUFDO0FBQy9ELE9BQU8sRUFBRSxPQUFPLEVBQWdCLE1BQU0sTUFBTSxDQUFDOzs7O0FBSTdDLDZCQUlDOzs7SUFIQyxrQ0FBcUI7O0lBQ3JCLCtDQUFvQzs7SUFDcEMsZ0RBQXFDOztBQUd2QztJQXFCRSwrQkFBNEMsYUFBb0M7UUFBcEMsa0JBQWEsR0FBYixhQUFhLENBQXVCO1FBVHhFLG1CQUFjLEdBQXdCLElBQUksQ0FBQyxhQUFhLENBQUMsQ0FBQyxDQUFDLElBQUksQ0FBQyxDQUFDLENBQUMsRUFBRSxDQUFDO1FBQ3JFLHNCQUFpQixHQUF5QixJQUFJLENBQUMsYUFBYSxDQUFDLENBQUMsQ0FBQyxJQUFJLENBQUMsQ0FBQyxDQUFDLElBQUksT0FBTyxFQUFRLENBQUM7UUFDMUYsMEJBQXFCLEdBQTJDLElBQUksQ0FBQyxhQUFhLENBQUMsQ0FBQyxDQUFDLElBQUksQ0FBQyxDQUFDLENBQUMsSUFBSSxHQUFHLEVBQUUsQ0FBQztJQU8zQixDQUFDO0lBbEJwRixzQkFBSSxnREFBYTtRQURqQixnRUFBZ0U7Ozs7OztRQUNoRTtZQUNFLE9BQU8sSUFBSSxDQUFDLGFBQWEsQ0FBQyxDQUFDLENBQUMsSUFBSSxDQUFDLGFBQWEsQ0FBQyxhQUFhLENBQUMsQ0FBQyxDQUFDLG1CQUFBLElBQUksQ0FBQyxpQkFBaUIsRUFBQyxDQUFDO1FBQ3pGLENBQUM7OztPQUFBO0lBR0Qsc0JBQUksNkNBQVU7UUFEZCxtRUFBbUU7Ozs7OztRQUNuRTtZQUNFLE9BQU8sSUFBSSxDQUFDLGFBQWEsQ0FBQyxDQUFDLENBQUMsSUFBSSxD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QThHOzs7OztJQU9sRyw4Q0FBb0UiLCJzb3VyY2VzQ29udGVudCI6WyJpbXBvcnQgeyBJbmplY3RhYmxlLCBPcHRpb25hbCwgU2tpcFNlbGYgfSBmcm9tICdAYW5ndWxhci9jb3JlJztcbmltcG9ydCB7IFN1YmplY3QsIFN1YnNjcmlwdGlvbiB9IGZyb20gJ3J4anMnO1xuXG5pbXBvcnQgeyBOek1vZGFsUmVmIH0gZnJvbSAnLi9uei1tb2RhbC1yZWYuY2xhc3MnO1xuXG5pbnRlcmZhY2UgUmVnaXN0ZXJlZE1ldGEge1xuICBtb2RhbFJlZjogTnpNb2RhbFJlZjtcbiAgYWZ0ZXJPcGVuU3Vic2NyaXB0aW9uOiBTdWJzY3JpcHRpb247XG4gIGFmdGVyQ2xvc2VTdWJzY3JpcHRpb246IFN1YnNjcmlwdGlvbjtcbn1cblxuQEluamVjdGFibGUoKVxuZXhwb3J0IGNsYXNzIE56TW9kYWxDb250cm9sU2VydmljZSB7XG4gIC8vIFRyYWNrIHNpbmdsZXRvbiBhZnRlckFsbENsb3NlIHRocm91Z2ggb3ZlciB0aGUgaW5qZWN0aW9uIHRyZWVcbiAgZ2V0IGFmdGVyQWxsQ2xvc2UoKTogU3ViamVjdDx2b2lkPiB7XG4gICAgcmV0dXJuIHRoaXMucGFyZW50U2VydmljZSA/IHRoaXMucGFyZW50U2VydmljZS5hZnRlckFsbENsb3NlIDogdGhpcy5yb290QWZ0ZXJBbGxDbG9zZSE7XG4gIH1cblxuICAvLyBUcmFjayBzaW5nbGV0b24gb3Blbk1vZGFscyBhcnJheSB0aHJvdWdoIG92ZXIgdGhlIGluamVjdGlvbiB0cmVlXG4gIGdldCBvcGVuTW9kYWxzKCk6IE56TW9kYWxSZWZbXSB7XG4gICAgcmV0dXJuIHRoaXMucGFyZW50U2VydmljZSA/IHRoaXMucGFyZW50U2VydmljZS5vcGVuTW9kYWxzIDogdGhpcy5yb290T3Blbk1vZGFscyE7XG4gIH1cblxuICBwcml2YXRlIHJvb3RPcGVuTW9kYWxzOiBOek1vZGFsUmVmW10gfCBudWxsID0gdGhpcy5wYXJlbnRTZXJ2aWNlID8gbnVsbCA6IFtdO1xuICBwcml2YXRlIHJvb3RBZnRlckFsbENsb3NlOiBTdWJqZWN0PHZvaWQ+IHwgbnVsbCA9IHRoaXMucGFyZW50U2VydmljZSA/IG51bGwgOiBuZXcgU3ViamVjdDx2b2lkPigpO1xuICBwcml2YXRlIHJvb3RSZWdpc3RlcmVkTWV0YU1hcDogTWFwPE56TW9kYWxSZWYsIFJlZ2lzdGVyZWRNZXRhPiB8IG51bGwgPSB0aGlzLnBhcmVudFNlcnZpY2UgPyBudWxsIDogbmV3IE1hcCgpO1xuXG4gIHByaXZhdGUgZ2V0IHJlZ2lzdGVyZWRNZXRhTWFwKCk6IE1hcDxOek1vZGFsUmVmLCBSZWdpc3RlcmVkTWV0YT4ge1xuICAgIC8vIFJlZ2lzdGVyZWQgbW9kYWwgZm9yIGxhdGVyIHVzYWdlXG4gICAgcmV0dXJuIHRoaXMucGFyZW50U2VydmljZSA/IHRoaXMucGFyZW50U2VydmljZS5yZWdpc3RlcmVkTWV0YU1hcCA6IHRoaXMucm9vdFJlZ2lzdGVyZWRNZXRhTWFwITtcbiAgfVxuXG4gIGNvbnN0cnVjdG9yKEBPcHRpb25hbCgpIEBTa2lwU2VsZigpIHByaXZhdGUgcGFyZW50U2VydmljZTogTnpNb2RhbENvbnRyb2xTZXJ2aWNlKSB7fVxuXG4gIC8vIFJlZ2lzdGVyIGEgbW9kYWwgdG8gbGlzdGVuIGl0cyBvcGVuL2Nsb3NlXG4gIHJlZ2lzdGVyTW9kYWwobW9kYWxSZWY6IE56TW9kYWxSZWYpOiB2b2lkIHtcbiAgICBpZiAoIXRoaXMuaGFzUmVnaXN0ZXJlZChtb2RhbFJlZikpIHtcbiAgICAgIGNvbnN0IGFmdGVyT3BlblN1YnNjcmlwdGlvbiA9IG1vZGFsUmVmLmFmdGVyT3Blbi5zdWJzY3JpYmUoKCkgPT4gdGhpcy5vcGVuTW9kYWxzLnB1c2gobW9kYWxSZWYpKTtcbiAgICAgIGNvbnN0IGFmdGVyQ2xvc2VTdWJzY3JpcHRpb24gPSBtb2RhbFJlZi5hZnRlckNsb3NlLnN1YnNjcmliZSgoKSA9PiB0aGlzLnJlbW92ZU9wZW5Nb2RhbChtb2RhbFJlZikpO1xuXG4gICAgICB0aGlzLnJlZ2lzdGVyZWRNZXRhTWFwLnNldChtb2RhbFJlZiwgeyBtb2RhbFJlZiwgYWZ0ZXJPcGVuU3Vic2NyaXB0aW9uLCBhZnRlckNsb3NlU3Vic2NyaXB0aW9uIH0pO1xuICAgIH1cbiAgfVxuXG4gIC8vIGRlcmVnaXN0ZXIgbW9kYWxzXG4gIGRlcmVnaXN0ZXJNb2RhbChtb2RhbFJlZjogTnpNb2RhbFJlZik6IHZvaWQge1xuICAgIGNvbnN0IHJlZ2lzdGVyZWRNZXRhID0gdGhpcy5yZWdpc3RlcmVkTWV0YU1hcC5nZXQobW9kYWxSZWYpO1xuICAgIGlmIChyZWdpc3RlcmVkTWV0YSkge1xuICAgICAgLy8gUmVtb3ZlIHRoaXMgbW9kYWwgaWYgaXQgaXMgc3RpbGwgaW4gdGhlIG9wZW5lZCBtb2RhbCBsaXN0IChOT1RFOiBpdCBtYXkgdHJpZ2dlciBcImFmdGVyQWxsQ2xvc2VcIilcbiAgICAgIHRoaXMucmVtb3ZlT3Blbk1vZGFsKHJlZ2lzdGVyZWRNZXRhLm1vZGFsUmVmKTtcbiAgICAgIHJlZ2lzdGVyZWRNZXRhLmFmdGVyT3BlblN1YnNjcmlwdGlvbi51bnN1YnNjcmliZSgpO1xuICAgICAgcmVnaXN0ZXJlZE1ldGEuYWZ0ZXJDbG9zZVN1YnNjcmlwdGlvbi51bnN1YnNjcmliZSgpO1xuICAgICAgdGhpcy5yZWdpc3RlcmVkTWV0YU1hcC5kZWxldGUobW9kYWxSZWYpO1xuICAgIH1cbiAgfVxuXG4gIGhhc1JlZ2lzdGVyZWQobW9kYWxSZWY6IE56TW9kYWxSZWYpOiBib29sZWFuIHtcbiAgICByZXR1cm4gdGhpcy5yZWdpc3RlcmVkTWV0YU1hcC5oYXMobW9kYWxSZWYpO1xuICB9XG5cbiAgLy8gQ2xvc2UgYWxsIHJlZ2lzdGVyZWQgb3BlbmVkIG1vZGFsc1xuICBjbG9zZUFsbCgpOiB2b2lkIHtcbiAgICBsZXQgaSA9IHRoaXMub3Blbk1vZGFscy5sZW5ndGg7XG5cbiAgICB3aGlsZSAoaS0tKSB7XG4gICAgICB0aGlzLm9wZW5Nb2RhbHNbaV0uY2xvc2UoKTtcbiAgICB9XG4gIH1cblxuICBwcml2YXRlIHJlbW92ZU9wZW5Nb2RhbChtb2RhbFJlZjogTnpNb2RhbFJlZik6IHZvaWQge1xuICAgIGNvbnN0IGluZGV4ID0gdGhpcy5vcGVuTW9kYWxzLmluZGV4T2YobW9kYWxSZWYpO1xuXG4gICAgaWYgKGluZGV4ID4gLTEpIHtcbiAgICAgIHRoaXMub3Blbk1vZGFscy5zcGxpY2UoaW5kZXgsIDEpO1xuXG4gICAgICBpZiAoIXRoaXMub3Blbk1vZGFscy5sZW5ndGgpIHtcbiAgICAgICAgdGhpcy5hZnRlckFsbENsb3NlLm5leHQoKTtcbiAgICAgIH1cbiAgICB9XG4gIH1cbn1cbiJdfQ==