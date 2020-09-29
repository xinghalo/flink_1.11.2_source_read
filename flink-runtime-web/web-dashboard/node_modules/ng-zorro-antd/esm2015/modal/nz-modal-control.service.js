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
export class NzModalControlService {
    /**
     * @param {?} parentService
     */
    constructor(parentService) {
        this.parentService = parentService;
        this.rootOpenModals = this.parentService ? null : [];
        this.rootAfterAllClose = this.parentService ? null : new Subject();
        this.rootRegisteredMetaMap = this.parentService ? null : new Map();
    }
    // Track singleton afterAllClose through over the injection tree
    /**
     * @return {?}
     */
    get afterAllClose() {
        return this.parentService ? this.parentService.afterAllClose : (/** @type {?} */ (this.rootAfterAllClose));
    }
    // Track singleton openModals array through over the injection tree
    /**
     * @return {?}
     */
    get openModals() {
        return this.parentService ? this.parentService.openModals : (/** @type {?} */ (this.rootOpenModals));
    }
    /**
     * @private
     * @return {?}
     */
    get registeredMetaMap() {
        // Registered modal for later usage
        return this.parentService ? this.parentService.registeredMetaMap : (/** @type {?} */ (this.rootRegisteredMetaMap));
    }
    // Register a modal to listen its open/close
    /**
     * @param {?} modalRef
     * @return {?}
     */
    registerModal(modalRef) {
        if (!this.hasRegistered(modalRef)) {
            /** @type {?} */
            const afterOpenSubscription = modalRef.afterOpen.subscribe((/**
             * @return {?}
             */
            () => this.openModals.push(modalRef)));
            /** @type {?} */
            const afterCloseSubscription = modalRef.afterClose.subscribe((/**
             * @return {?}
             */
            () => this.removeOpenModal(modalRef)));
            this.registeredMetaMap.set(modalRef, { modalRef, afterOpenSubscription, afterCloseSubscription });
        }
    }
    // deregister modals
    /**
     * @param {?} modalRef
     * @return {?}
     */
    deregisterModal(modalRef) {
        /** @type {?} */
        const registeredMeta = this.registeredMetaMap.get(modalRef);
        if (registeredMeta) {
            // Remove this modal if it is still in the opened modal list (NOTE: it may trigger "afterAllClose")
            this.removeOpenModal(registeredMeta.modalRef);
            registeredMeta.afterOpenSubscription.unsubscribe();
            registeredMeta.afterCloseSubscription.unsubscribe();
            this.registeredMetaMap.delete(modalRef);
        }
    }
    /**
     * @param {?} modalRef
     * @return {?}
     */
    hasRegistered(modalRef) {
        return this.registeredMetaMap.has(modalRef);
    }
    // Close all registered opened modals
    /**
     * @return {?}
     */
    closeAll() {
        /** @type {?} */
        let i = this.openModals.length;
        while (i--) {
            this.openModals[i].close();
        }
    }
    /**
     * @private
     * @param {?} modalRef
     * @return {?}
     */
    removeOpenModal(modalRef) {
        /** @type {?} */
        const index = this.openModals.indexOf(modalRef);
        if (index > -1) {
            this.openModals.splice(index, 1);
            if (!this.openModals.length) {
                this.afterAllClose.next();
            }
        }
    }
}
NzModalControlService.decorators = [
    { type: Injectable }
];
/** @nocollapse */
NzModalControlService.ctorParameters = () => [
    { type: NzModalControlService, decorators: [{ type: Optional }, { type: SkipSelf }] }
];
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
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotbW9kYWwtY29udHJvbC5zZXJ2aWNlLmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbIm1vZGFsL256LW1vZGFsLWNvbnRyb2wuc2VydmljZS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7O0FBQUEsT0FBTyxFQUFFLFVBQVUsRUFBRSxRQUFRLEVBQUUsUUFBUSxFQUFFLE1BQU0sZUFBZSxDQUFDO0FBQy9ELE9BQU8sRUFBRSxPQUFPLEVBQWdCLE1BQU0sTUFBTSxDQUFDOzs7O0FBSTdDLDZCQUlDOzs7SUFIQyxrQ0FBcUI7O0lBQ3JCLCtDQUFvQzs7SUFDcEMsZ0RBQXFDOztBQUl2QyxNQUFNLE9BQU8scUJBQXFCOzs7O0lBb0JoQyxZQUE0QyxhQUFvQztRQUFwQyxrQkFBYSxHQUFiLGFBQWEsQ0FBdUI7UUFUeEUsbUJBQWMsR0FBd0IsSUFBSSxDQUFDLGFBQWEsQ0FBQyxDQUFDLENBQUMsSUFBSSxDQUFDLENBQUMsQ0FBQyxFQUFFLENBQUM7UUFDckUsc0JBQWlCLEdBQXlCLElBQUksQ0FBQyxhQUFhLENBQUMsQ0FBQyxDQUFDLElBQUksQ0FBQyxDQUFDLENBQUMsSUFBSSxPQUFPLEVBQVEsQ0FBQztRQUMxRiwwQkFBcUIsR0FBMkMsSUFBSSxDQUFDLGFBQWEsQ0FBQyxDQUFDLENBQUMsSUFBSSxDQUFDLENBQUMsQ0FBQyxJQUFJLEdBQUcsRUFBRSxDQUFDO0lBTzNCLENBQUM7Ozs7O0lBbEJwRixJQUFJLGFBQWE7UUFDZixPQUFPLElBQUksQ0FBQyxhQUFhLENBQUMsQ0FBQyxDQUFDLElBQUksQ0FBQyxhQUFhLENBQUMsYUFBYSxDQUFDLENBQUMsQ0FBQyxtQkFBQSxJQUFJLENBQUMsaUJBQWlCLEVBQUMsQ0FBQztJQUN6RixDQUFDOzs7OztJQUdELElBQUksVUFBVTtRQUNaLE9BQU8sSUFBSSxDQUFDLGFBQWEsQ0FBQyxDQUFDLENBQUMsSUFBSSxDQUFDLGFBQWEsQ0FBQyxVQUFVLENBQUMsQ0FBQyxDQUFDLG1CQUFBLElBQUksQ0FBQyxjQUFjLEVBQUMsQ0FBQztJQUNuRixDQUFDOzs7OztJQU1ELElBQVksaUJBQWlCO1FBQzNCLG1DQUFtQztRQUNuQyxPQUFPLElBQUksQ0FBQyxhQUFhLENBQUMsQ0FBQyxDQUFDLElBQUksQ0FBQyxhQUFhLENBQUMsaUJBQWlCLENBQUMsQ0FBQyxDQUFDLG1CQUFBLElBQUksQ0FBQyxxQkFBcUIsRUFBQyxDQUFDO0lBQ2pHLENBQUM7Ozs7OztJQUtELGFBQWEsQ0FBQyxRQUFvQjtRQUNoQyxJQUFJLENBQUMsSUFBSSxDQUFDLGFBQWEsQ0FBQyxRQUFRLENBQUMsRUFBRTs7a0JBQzNCLHFCQUFxQixHQUFHLFFBQVEsQ0FBQyxTQUFTLENBQUMsU0FBUzs7O1lBQUMsR0FBRyxFQUFFLENBQUMsSUFBSSxDQUFDLFVBQVUsQ0FBQyxJQUFJLENBQUMsUUFBUSxDQUFDLEVBQUM7O2tCQUMxRixzQkFBc0IsR0FBRyxRQUFRLENBQUMsVUFBVSxDQUFDLFNBQVM7OztZQUFDLEdBQUcsRUFBRSxDQUFDLElBQUksQ0FBQyxlQUFlLENBQUMsUUFBUSxDQUFDLEVBQUM7WUFFbEcsSUFBSSxDQUFDLGlCQUFpQixDQUFDLEdBQUcsQ0FBQyxRQUFRLEVBQUUsRUFBRSxRQUFRLEVBQUUscUJBQXFCLEVBQUUsc0JBQXNCLEVBQUUsQ0FBQyxDQUFDO1NBQ25HO0lBQ0gsQ0FBQzs7Ozs7O0lBR0QsZUFBZSxDQUFDLFFBQW9COztjQUM1QixjQUFjLEdBQUcsSUFBSSxDQUFDLGlCQUFpQixDQUFDLEdBQUcsQ0FBQyxRQUFRLENBQUM7UUFDM0QsSUFBSSxjQUFjLEVBQUU7WUFDbEIsbUdBQW1HO1lBQ25HLElBQUksQ0FBQyxlQUFlLENBQUMsY0FBYyxDQUFDLFFBQVEsQ0FBQyxDQUFDO1lBQzlDLGNBQWMsQ0FBQyxxQkFBcUIsQ0FBQyxXQUFXLEVBQUUsQ0FBQztZQUNuRCxjQUFjLENBQUMsc0JBQXNCLENBQUMsV0FBVyxFQUFFLENBQUM7WUFDcEQsSUFBSSxDQUFDLGlCQUFpQixDQUFDLE1BQU0sQ0FBQyxRQUFRLENBQUMsQ0FBQztTQUN6QztJQUNILENBQUM7Ozs7O0lBRUQsYUFBYSxDQUFDLFFBQW9CO1FBQ2hDLE9BQU8sSUFBSSxDQUFDLGlCQUFpQixDQUFDLEdBQUcsQ0FBQyxRQUFRLENBQUMsQ0FBQztJQUM5QyxDQUFDOzs7OztJQUdELFFBQVE7O1lBQ0YsQ0FBQyxHQUFHLElBQUksQ0FBQyxVQUFVLENBQUMsTUFBTTtRQUU5QixPQUFPLENBQUMsRUFBRSxFQUFFO1lBQ1YsSUFBSSxDQUFDLFVBQVUsQ0FBQyxDQUFDLENBQUMsQ0FBQyxLQUFLLEVBQUUsQ0FBQztTQUM1QjtJQUNILENBQUM7Ozs7OztJQUVPLGVBQWUsQ0FBQyxRQUFvQjs7Y0FDcEMsS0FBSyxHQUFHLElBQUksQ0FBQyxVQUFVLENBQUMsT0FBTyxDQUFDLFFBQVEsQ0FBQztRQUUvQyxJQUFJLEtBQUssR0FBRyxDQUFDLENBQUMsRUFBRTtZQUNkLElBQUksQ0FBQyxVQUFVLENBQUMsTUFBTSxDQUFDLEtBQUssRUFBRSxDQUFDLENBQUMsQ0FBQztZQUVqQyxJQUFJLENBQUMsSUFBSSxDQUFDLFVBQVUsQ0FBQyxNQUFNLEVBQUU7Z0JBQzNCLElBQUksQ0FBQyxhQUFhLENBQUMsSUFBSSxFQUFFLENBQUM7YUFDM0I7U0FDRjtJQUNILENBQUM7OztZQXBFRixVQUFVOzs7O1lBcUJrRCxxQkFBcUIsdUJBQW5FLFFBQVEsWUFBSSxRQUFROzs7Ozs7O0lBVGpDLCtDQUE2RTs7Ozs7SUFDN0Usa0RBQWtHOzs7OztJQUNsRyxzREFBOEc7Ozs7O0lBT2xHLDhDQUFvRSIsInNvdXJjZXNDb250ZW50IjpbImltcG9ydCB7IEluamVjdGFibGUsIE9wdGlvbmFsLCBTa2lwU2VsZiB9IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuaW1wb3J0IHsgU3ViamVjdCwgU3Vic2NyaXB0aW9uIH0gZnJvbSAncnhqcyc7XG5cbmltcG9ydCB7IE56TW9kYWxSZWYgfSBmcm9tICcuL256LW1vZGFsLXJlZi5jbGFzcyc7XG5cbmludGVyZmFjZSBSZWdpc3RlcmVkTWV0YSB7XG4gIG1vZGFsUmVmOiBOek1vZGFsUmVmO1xuICBhZnRlck9wZW5TdWJzY3JpcHRpb246IFN1YnNjcmlwdGlvbjtcbiAgYWZ0ZXJDbG9zZVN1YnNjcmlwdGlvbjogU3Vic2NyaXB0aW9uO1xufVxuXG5ASW5qZWN0YWJsZSgpXG5leHBvcnQgY2xhc3MgTnpNb2RhbENvbnRyb2xTZXJ2aWNlIHtcbiAgLy8gVHJhY2sgc2luZ2xldG9uIGFmdGVyQWxsQ2xvc2UgdGhyb3VnaCBvdmVyIHRoZSBpbmplY3Rpb24gdHJlZVxuICBnZXQgYWZ0ZXJBbGxDbG9zZSgpOiBTdWJqZWN0PHZvaWQ+IHtcbiAgICByZXR1cm4gdGhpcy5wYXJlbnRTZXJ2aWNlID8gdGhpcy5wYXJlbnRTZXJ2aWNlLmFmdGVyQWxsQ2xvc2UgOiB0aGlzLnJvb3RBZnRlckFsbENsb3NlITtcbiAgfVxuXG4gIC8vIFRyYWNrIHNpbmdsZXRvbiBvcGVuTW9kYWxzIGFycmF5IHRocm91Z2ggb3ZlciB0aGUgaW5qZWN0aW9uIHRyZWVcbiAgZ2V0IG9wZW5Nb2RhbHMoKTogTnpNb2RhbFJlZltdIHtcbiAgICByZXR1cm4gdGhpcy5wYXJlbnRTZXJ2aWNlID8gdGhpcy5wYXJlbnRTZXJ2aWNlLm9wZW5Nb2RhbHMgOiB0aGlzLnJvb3RPcGVuTW9kYWxzITtcbiAgfVxuXG4gIHByaXZhdGUgcm9vdE9wZW5Nb2RhbHM6IE56TW9kYWxSZWZbXSB8IG51bGwgPSB0aGlzLnBhcmVudFNlcnZpY2UgPyBudWxsIDogW107XG4gIHByaXZhdGUgcm9vdEFmdGVyQWxsQ2xvc2U6IFN1YmplY3Q8dm9pZD4gfCBudWxsID0gdGhpcy5wYXJlbnRTZXJ2aWNlID8gbnVsbCA6IG5ldyBTdWJqZWN0PHZvaWQ+KCk7XG4gIHByaXZhdGUgcm9vdFJlZ2lzdGVyZWRNZXRhTWFwOiBNYXA8TnpNb2RhbFJlZiwgUmVnaXN0ZXJlZE1ldGE+IHwgbnVsbCA9IHRoaXMucGFyZW50U2VydmljZSA/IG51bGwgOiBuZXcgTWFwKCk7XG5cbiAgcHJpdmF0ZSBnZXQgcmVnaXN0ZXJlZE1ldGFNYXAoKTogTWFwPE56TW9kYWxSZWYsIFJlZ2lzdGVyZWRNZXRhPiB7XG4gICAgLy8gUmVnaXN0ZXJlZCBtb2RhbCBmb3IgbGF0ZXIgdXNhZ2VcbiAgICByZXR1cm4gdGhpcy5wYXJlbnRTZXJ2aWNlID8gdGhpcy5wYXJlbnRTZXJ2aWNlLnJlZ2lzdGVyZWRNZXRhTWFwIDogdGhpcy5yb290UmVnaXN0ZXJlZE1ldGFNYXAhO1xuICB9XG5cbiAgY29uc3RydWN0b3IoQE9wdGlvbmFsKCkgQFNraXBTZWxmKCkgcHJpdmF0ZSBwYXJlbnRTZXJ2aWNlOiBOek1vZGFsQ29udHJvbFNlcnZpY2UpIHt9XG5cbiAgLy8gUmVnaXN0ZXIgYSBtb2RhbCB0byBsaXN0ZW4gaXRzIG9wZW4vY2xvc2VcbiAgcmVnaXN0ZXJNb2RhbChtb2RhbFJlZjogTnpNb2RhbFJlZik6IHZvaWQge1xuICAgIGlmICghdGhpcy5oYXNSZWdpc3RlcmVkKG1vZGFsUmVmKSkge1xuICAgICAgY29uc3QgYWZ0ZXJPcGVuU3Vic2NyaXB0aW9uID0gbW9kYWxSZWYuYWZ0ZXJPcGVuLnN1YnNjcmliZSgoKSA9PiB0aGlzLm9wZW5Nb2RhbHMucHVzaChtb2RhbFJlZikpO1xuICAgICAgY29uc3QgYWZ0ZXJDbG9zZVN1YnNjcmlwdGlvbiA9IG1vZGFsUmVmLmFmdGVyQ2xvc2Uuc3Vic2NyaWJlKCgpID0+IHRoaXMucmVtb3ZlT3Blbk1vZGFsKG1vZGFsUmVmKSk7XG5cbiAgICAgIHRoaXMucmVnaXN0ZXJlZE1ldGFNYXAuc2V0KG1vZGFsUmVmLCB7IG1vZGFsUmVmLCBhZnRlck9wZW5TdWJzY3JpcHRpb24sIGFmdGVyQ2xvc2VTdWJzY3JpcHRpb24gfSk7XG4gICAgfVxuICB9XG5cbiAgLy8gZGVyZWdpc3RlciBtb2RhbHNcbiAgZGVyZWdpc3Rlck1vZGFsKG1vZGFsUmVmOiBOek1vZGFsUmVmKTogdm9pZCB7XG4gICAgY29uc3QgcmVnaXN0ZXJlZE1ldGEgPSB0aGlzLnJlZ2lzdGVyZWRNZXRhTWFwLmdldChtb2RhbFJlZik7XG4gICAgaWYgKHJlZ2lzdGVyZWRNZXRhKSB7XG4gICAgICAvLyBSZW1vdmUgdGhpcyBtb2RhbCBpZiBpdCBpcyBzdGlsbCBpbiB0aGUgb3BlbmVkIG1vZGFsIGxpc3QgKE5PVEU6IGl0IG1heSB0cmlnZ2VyIFwiYWZ0ZXJBbGxDbG9zZVwiKVxuICAgICAgdGhpcy5yZW1vdmVPcGVuTW9kYWwocmVnaXN0ZXJlZE1ldGEubW9kYWxSZWYpO1xuICAgICAgcmVnaXN0ZXJlZE1ldGEuYWZ0ZXJPcGVuU3Vic2NyaXB0aW9uLnVuc3Vic2NyaWJlKCk7XG4gICAgICByZWdpc3RlcmVkTWV0YS5hZnRlckNsb3NlU3Vic2NyaXB0aW9uLnVuc3Vic2NyaWJlKCk7XG4gICAgICB0aGlzLnJlZ2lzdGVyZWRNZXRhTWFwLmRlbGV0ZShtb2RhbFJlZik7XG4gICAgfVxuICB9XG5cbiAgaGFzUmVnaXN0ZXJlZChtb2RhbFJlZjogTnpNb2RhbFJlZik6IGJvb2xlYW4ge1xuICAgIHJldHVybiB0aGlzLnJlZ2lzdGVyZWRNZXRhTWFwLmhhcyhtb2RhbFJlZik7XG4gIH1cblxuICAvLyBDbG9zZSBhbGwgcmVnaXN0ZXJlZCBvcGVuZWQgbW9kYWxzXG4gIGNsb3NlQWxsKCk6IHZvaWQge1xuICAgIGxldCBpID0gdGhpcy5vcGVuTW9kYWxzLmxlbmd0aDtcblxuICAgIHdoaWxlIChpLS0pIHtcbiAgICAgIHRoaXMub3Blbk1vZGFsc1tpXS5jbG9zZSgpO1xuICAgIH1cbiAgfVxuXG4gIHByaXZhdGUgcmVtb3ZlT3Blbk1vZGFsKG1vZGFsUmVmOiBOek1vZGFsUmVmKTogdm9pZCB7XG4gICAgY29uc3QgaW5kZXggPSB0aGlzLm9wZW5Nb2RhbHMuaW5kZXhPZihtb2RhbFJlZik7XG5cbiAgICBpZiAoaW5kZXggPiAtMSkge1xuICAgICAgdGhpcy5vcGVuTW9kYWxzLnNwbGljZShpbmRleCwgMSk7XG5cbiAgICAgIGlmICghdGhpcy5vcGVuTW9kYWxzLmxlbmd0aCkge1xuICAgICAgICB0aGlzLmFmdGVyQWxsQ2xvc2UubmV4dCgpO1xuICAgICAgfVxuICAgIH1cbiAgfVxufVxuIl19